import logger
import json
import uuid
import copy
import math
import re
import testconstants
from datetime import date, timedelta
import datetime
import time
from remote.remote_util import RemoteMachineShellConnection
from couchbase_helper.tuq_generators import JsonGenerator
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.api.exception import CBQError, ReadDocumentException
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper
from couchbase_helper.tuq_helper import N1QLHelper


class QueryTests(BaseTestCase):
    def setUp(self):
        if not self._testMethodName == 'suite_setUp':
            self.skip_buckets_handle = True
        super(QueryTests, self).setUp()
        if self.input.param("force_clean", False):
            self.skip_buckets_handle = False
            super(QueryTests, self).setUp()
        self.version = self.input.param("cbq_version", "sherlock")
        self.flat_json = self.input.param("flat_json", False)
        self.directory_flat_json = self.input.param("directory_flat_json", "/tmp/")
        if self.input.tuq_client and "client" in self.input.tuq_client:
            self.shell = RemoteMachineShellConnection(self.input.tuq_client["client"])
        else:
            self.shell = RemoteMachineShellConnection(self.master)
        if self.input.param("start_cmd", True):
            self._start_command_line_query(self.master, user=self.master.rest_username, password=self.master.rest_password)
        self.use_rest = self.input.param("use_rest", True)
        self.hint_index = self.input.param("hint", None)
        self.max_verify = self.input.param("max_verify", None)
        self.buckets = RestConnection(self.master).get_buckets()
        self.docs_per_day = self.input.param("doc-per-day", 49)
        self.item_flag = self.input.param("item_flag", 4042322160)
        self.gens_load = self.generate_docs(self.docs_per_day)
        self.skip_load = self.input.param("skip_load", False)
        self.skip_index = self.input.param("skip_index", False)
        self.n1ql_port = self.input.param("n1ql_port", 8093)
        self.primary_indx_type = self.input.param("primary_indx_type", 'VIEW')
        self.primary_indx_drop = self.input.param("primary_indx_drop", False)
        self.index_type = self.input.param("index_type", 'VIEW')
        self.scan_consistency = self.input.param("scan_consistency", 'REQUEST_PLUS')
        if self.input.param("reload_data", False):
            self.gens_load = self.generate_docs(self.docs_per_day)
            self.load(self.gens_load, flag=self.item_flag)
        if not (hasattr(self, 'skip_generation') and self.skip_generation):
            self.full_list = self.generate_full_docs_list(self.gens_load)
        if self.input.param("gomaxprocs", None):
            self.configure_gomaxprocs()
        if str(self.__class__).find('QueriesUpgradeTests') == -1:
            self.create_primary_index_for_3_0_and_greater()

    def suite_setUp(self):
        try:
            os = self.shell.extract_remote_info().type.lower()
            if os != 'windows':
                self.sleep(10, 'sleep before load')
            if not self.skip_load:
                if self.flat_json:
                    self.load_directory(self.gens_load)
                else:
                    self.load(self.gens_load, flag=self.item_flag)
            self.create_primary_index_for_3_0_and_greater()
            if not self.input.param("skip_build_tuq", True):
                self._build_tuq(self.master)
            self.skip_buckets_handle = True
        except Exception, ex:
            self.log.error('SUITE SETUP FAILED')
            self.tearDown()

    def tearDown(self):
        if self._testMethodName == 'suite_tearDown':
            self.skip_buckets_handle = False
        super(QueryTests, self).tearDown()

    def suite_tearDown(self):
        if not self.input.param("skip_build_tuq", True):
            if hasattr(self, 'shell'):
               o = self.shell.execute_command("ps -aef| grep cbq-engine")
               self.log.info(o)
               if len(o):
                   for cbq_engine in o[0]:
                       if cbq_engine.find('grep') == -1:
                           output = cbq_engine.split(' ')
                           if len(output) > 1:
                                pid = [item for item in output if item][1]
                                self.shell.execute_command("kill -9 %s" % pid)


##############################################################################################
#
#   SIMPLE CHECKS
##############################################################################################
    def test_escaped_identifiers(self):
        queries_errors = {'SELECT name FROM {0} as bucket' :
                          ('syntax error', 3000)} #execution phase
        self.negative_common_body(queries_errors)
        for bucket in self.buckets:
            self.query = 'SELECT name FROM %s as `bucket` ORDER BY name' % (bucket.name)
            actual_result = self.run_cbq_query()

            expected_list = [{"name" : doc["name"]} for doc in self.full_list]
            expected_list_sorted = sorted(expected_list, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_list_sorted)

##############################################################################################
#
#   ALL
##############################################################################################

    def test_all(self):
        for bucket in self.buckets:
            self.query = 'SELECT ALL job_title FROM %s ORDER BY job_title'  % (bucket.name)
            actual_result = self.run_cbq_query()


            expected_result = [{"job_title" : doc['job_title']}
                             for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result['results'], expected_result)

    def test_all_nested(self):
        for bucket in self.buckets:
            self.query = 'SELECT ALL tasks_points.task1 FROM %s '  % (bucket.name) +\
                         'ORDER BY tasks_points.task1'

            actual_result = self.run_cbq_query()
            expected_result = [{"task1" : doc['tasks_points']['task1']}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task1']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = 'SELECT ALL skills[0] as skill' +\
                         ' FROM %s ORDER BY skills[0]'  % (bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [{"skill" : doc['skills'][0]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['skill']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = 'SELECT ALL tasks_points.* ' +\
                         'FROM %s'  % (bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [doc['tasks_points'] for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc:
                                     (doc['task1'], doc['task2']))
            actual_result = sorted(actual_result['results'], key=lambda doc:
                                     (doc['task1'], doc['task2']))
            self._verify_results(actual_result, expected_result)

    def test_all_negative(self):
        queries_errors = {'SELECT ALL * FROM %s' : ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_keywords(self):
        queries_errors = {'SELECT description as DESC FROM %s order by DESC' : ('syntax error', 3000)}
        self.negative_common_body(queries_errors)
    
    def test_distinct_negative(self):
        queries_errors = {'SELECT name FROM {0} ORDER BY DISTINCT name' : ('syntax error', 3000),
                          'SELECT name FROM {0} GROUP BY DISTINCT name' : ('syntax error', 3000),
                          'SELECT ANY tasks_points FROM {0}' : ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_any(self):
        for bucket in self.buckets:
            self.query = "SELECT name, email FROM %s WHERE "  % (bucket.name) +\
                         "(ANY skill IN %s.skills SATISFIES skill = 'skill2010' END)" % (
                                                                      bucket.name) +\
                        " AND (ANY vm IN %s.VMs SATISFIES vm.RAM = 5 END)"  % (
                                                                bucket.name) +\
                        "AND  NOT (job_title = 'Sales') ORDER BY name"

            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name'], "email" : doc["email"]}
                               for doc in self.full_list
                               if len([skill for skill in doc["skills"]
                                       if skill == 'skill2010']) > 0 and\
                                  len([vm for vm in doc["VMs"]
                                       if vm["RAM"] == 5]) > 0 and\
                                  doc["job_title"] != 'Sales']
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_any_within(self):
        for bucket in self.buckets:
            self.query = "SELECT name, email FROM %s "  % (bucket.name) +\
                         "WHERE ANY vm within %s.VMs SATISFIES vm.RAM = 5 END" % (
                                                                      bucket.name)

            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name'], "email" : doc["email"]}
                               for doc in self.full_list
                               if len([vm for vm in doc["VMs"]
                                       if vm["RAM"] == 5]) > 0]
            expected_result = sorted(expected_result)
            self._verify_results(sorted(actual_result['results']), expected_result)

    def test_any_no_in_clause(self):
        for bucket in self.buckets:
            self.query = "SELECT name, email FROM %s WHERE "  % (bucket.name) +\
                         "(ANY skill IN %s.skills SATISFIES skill = 'skill2010' end)" % (
                                                                bucket.name) +\
                         "AND (ANY vm IN %s.VMs SATISFIES vm.RAM = 5 end) " % (
                                                            bucket.name) +\
                         "AND  NOT (job_title = 'Sales') ORDER BY name"

            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name'], "email" : doc["email"]}
                               for doc in self.full_list
                               if len([skill for skill in doc["skills"]
                                       if skill == 'skill2010']) > 0 and\
                                  len([vm for vm in doc["VMs"]
                                       if vm["RAM"] == 5]) > 0 and\
                                  doc["job_title"] != 'Sales']
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_prepared_any_no_in_clause(self):
        for bucket in self.buckets:
            self.query = "SELECT name, email FROM %s WHERE "  % (bucket.name) +\
                         "(ANY skill IN %s.skills SATISFIES skill = 'skill2010' end)" % (
                                                                bucket.name) +\
                         "AND (ANY vm IN %s.VMs SATISFIES vm.RAM = 5 end) " % (
                                                            bucket.name) +\
                         "AND  NOT (job_title = 'Sales') ORDER BY name"
            self.prepared_common_body()

    def test_any_external(self):
        for bucket in self.buckets:
            self.query = 'SELECT name FROM %s WHERE '  % (bucket.name) +\
                         'ANY x IN ["Support", "Management"] SATISFIES job_title = x END ' +\
                         'ORDER BY name'

            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']}
                               for doc in self.full_list
                               if doc["job_title"] in ["Support", "Management"]]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_every(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM %s WHERE " % (bucket.name) +\
                         "(EVERY vm IN %s.VMs SATISFIES CEIL(vm.memory) > 5 END)" % (
                                                            bucket.name) +\
                         " ORDER BY name"

            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']}
                               for doc in self.full_list
                               if len([vm for vm in doc["VMs"]
                                       if math.ceil(vm['memory']) > 5]) ==\
                                  len(doc["VMs"])]

            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_satisfy_negative(self):
        queries_errors = {'SELECT name FROM %s WHERE ANY x IN 123 SATISFIES job_title = x END' : ('syntax error', 3000),
                          'SELECT name FROM %s WHERE ANY x IN ["Sales"] SATISFIES job_title = x' : ('syntax error', 3000),
                          'SELECT job_title FROM %s WHERE ANY job_title IN ["Sales"] SATISFIES job_title = job_title END' : ('syntax error', 3000),
                          'SELECT job_title FROM %s WHERE EVERY ANY x IN ["Sales"] SATISFIES x = job_title END' : ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_array(self):
        for bucket in self.buckets:
            self.query = "SELECT ARRAY vm.memory FOR vm IN VMs END AS vm_memories" +\
            " FROM %s WHERE VMs IS NOT NULL "  % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['vm_memories']))
            expected_result = [{"vm_memories" : [vm["memory"] for vm in doc['VMs']]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['vm_memories']))
            self._verify_results(actual_result, expected_result)

    def test_arrays_negative(self):
        queries_errors = {'SELECT ARRAY vm.memory FOR vm IN 123 END AS vm_memories FROM %s' : ('syntax error', 3000),
                          'SELECT job_title, array_agg(name)[:5] as names FROM %s' : ('syntax error', 3000),
                          'SELECT job_title, array_agg(name)[-20:-5] as names FROM %s' : ('syntax error', 3000),
                          'SELECT job_title, array_agg(name)[a:-b] as names FROM %s' : ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_slicing(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_agg(name)[0:5] as names" +\
            " FROM %s GROUP BY job_title" % (bucket.name)

            actual_result = self.run_cbq_query()
            for item in actual_result['results']:
                self.log.info("Result: %s" % actual_result['results'])
                self.assertTrue(len(item['names']) <= 5, "Slicing doesn't work")

            self.query = "SELECT job_title, array_agg(name)[5:] as names" +\
            " FROM %s GROUP BY job_title" % (bucket.name)
            actual_result = self.run_cbq_query()

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_list = [{"job_title" : group,
                                "names" : [x["name"] for x in self.full_list
                                               if x["job_title"] == group]}
                               for group in tmp_groups]
            for item in actual_result['results']:
                for tmp_item in expected_list:
                    if item['job_title'] == tmp_item['job_title']:
                        expected_item = tmp_item
                self.log.info("Result: %s" % actual_result['results'])
                self.assertTrue(len(item['names']) == len(expected_item['names']),
                                "Slicing doesn't work")

            self.query = "SELECT job_title, array_agg(name)[5:10] as names" +\
            " FROM %s GROUP BY job_title" % (bucket.name)
            actual_result = self.run_cbq_query()
            for item in actual_result['results']:
                self.log.info("Result: %s" % actual_result['results'])
                self.assertTrue(len(item['names']) <= 5, "Slicing doesn't work")

##############################################################################################
#
#   LIKE
##############################################################################################

    def test_like(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM {0} WHERE job_title LIKE 'S%' ORDER BY name".format(bucket.name)
            actual_result = self.run_cbq_query()

            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if doc["job_title"].startswith('S')]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT name FROM {0} WHERE job_title LIKE '%u%' ORDER BY name".format(
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if doc["job_title"].find('u') != -1]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT name FROM {0} WHERE job_title NOT LIKE 'S%' ORDER BY name".format(
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if not doc["job_title"].startswith('S')]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT name FROM {0} WHERE job_title NOT LIKE '_ales' ORDER BY name".format(
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if not (doc["job_title"].endswith('ales') and\
                               len(doc["job_title"]) == 5)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_like_negative(self):
        queries_errors = {"SELECT tasks_points FROM {0} WHERE tasks_points.* LIKE '_1%'" :
                           ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_like_any(self):
        for bucket in self.buckets:
            self.query = "SELECT name, email FROM %s WHERE (ANY vm IN %s.VMs" % (
                                                        bucket.name, bucket.name) +\
            " SATISFIES vm.os LIKE '%bun%'" +\
            "END) AND (ANY skill IN %s.skills " % (bucket.name) +\
            "SATISFIES skill = 'skill2010' END) ORDER BY name"
            actual_result = self.run_cbq_query()

            expected_result = [{"name" : doc['name'], "email" : doc["email"]}
                               for doc in self.full_list
                               if len([vm for vm in doc["VMs"]
                                       if vm["os"].find('bun') != -1]) > 0 and\
                                  len([skill for skill in doc["skills"]
                                       if skill == 'skill2010']) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_like_every(self):
        for bucket in self.buckets:
            self.query = "SELECT name, email FROM %s WHERE (EVERY vm IN %s.VMs " % (
                                                        bucket.name, bucket.name) +\
                         "SATISFIES vm.os NOT LIKE '%cent%' END)" +\
                         " AND (ANY skill IN %s.skills SATISFIES skill =" % (bucket.name) +\
                         " 'skill2010' END) ORDER BY name"
            actual_result = self.run_cbq_query()

            expected_result = [{"name" : doc['name'], "email" : doc["email"]}
                               for doc in self.full_list
                               if len([vm for vm in doc["VMs"]
                                     if vm["os"].find('cent') == -1]) == len(doc["VMs"]) and\
                                  len([skill for skill in doc["skills"]
                                       if skill == 'skill2010']) > 0]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_like_aliases(self):
        for bucket in self.buckets:
            self.query = "select name AS NAME from %s " % (bucket.name) +\
            "AS EMPLOYEE where EMPLOYEE.name LIKE '_mpl%' ORDER BY name"
            actual_result = self.run_cbq_query()

            expected_result = [{"NAME" : doc['name']} for doc in self.full_list
                               if doc["name"].find('mpl') == 1]
            expected_result = sorted(expected_result, key=lambda doc: (doc['NAME']))
            self._verify_results(actual_result['results'], expected_result)

    def test_like_wildcards(self):
        for bucket in self.buckets:
            self.query = "SELECT email FROM %s WHERE email " % (bucket.name) +\
                         "LIKE '%@%.%' ORDER BY email"
            actual_result = self.run_cbq_query()

            expected_result = [{"email" : doc['email']} for doc in self.full_list
                               if re.match(r'.*@.*\..*', doc['email'])]
            expected_result = sorted(expected_result, key=lambda doc: (doc['email']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT email FROM %s WHERE email" % (bucket.name) +\
                         " LIKE '%@%.h' ORDER BY email"
            actual_result = self.run_cbq_query()
            expected_result = []
            self._verify_results(actual_result['results'], expected_result)

    def test_prepared_like_wildcards(self):
        for bucket in self.buckets:
            self.query = "SELECT email FROM %s WHERE email " % (bucket.name) +\
                         "LIKE '%@%.%' ORDER BY email"
            self.prepared_common_body()

    def test_between(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM {0} WHERE join_mo BETWEEN 1 AND 6 ORDER BY name".format(bucket.name)
            actual_result = self.run_cbq_query()

            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if doc["join_mo"] >= 1 and doc["join_mo"] <= 6]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT name FROM {0} WHERE join_mo NOT BETWEEN 1 AND 6 ORDER BY name".format(bucket.name)
            actual_result = self.run_cbq_query()

            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if not(doc["join_mo"] >= 1 and doc["join_mo"] <= 6)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_between_negative(self):
        queries_errors = {'SELECT name FROM %s WHERE join_mo BETWEEN 1 AND -10 ORDER BY name' : ('syntax error', 3000),
                          'SELECT name FROM %s WHERE join_mo BETWEEN 1 AND a ORDER BY name' : ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

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

            expected_result = [{"task" : doc['tasks_points']["task1"]}
                               for doc in self.full_list
                               if doc["join_mo"] > 7]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result['results'], expected_result)

    def test_group_by_having(self):
        for bucket in self.buckets:
            self.query = "from %s WHERE join_mo>7 GROUP BY tasks_points.task1 " % (bucket.name) +\
                         "HAVING COUNT(tasks_points.task1) > 0 SELECT tasks_points.task1 " +\
                         "AS task ORDER BY tasks_points.task1"
            actual_result = self.run_cbq_query()

            expected_result = [{"task" : doc['tasks_points']["task1"]}
                               for doc in self.full_list
                               if doc["join_mo"] > 7]
            expected_result = [doc for doc in expected_result
                               if expected_result.count(doc) > 0]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result['results'], expected_result)

    def test_group_by_aggr_fn(self):
        for bucket in self.buckets:
            self.query = "SELECT tasks_points.task1 AS task from %s " % (bucket.name) +\
                         "WHERE join_mo>7 GROUP BY tasks_points.task1 " +\
                         "HAVING COUNT(tasks_points.task1) > 0 AND "  +\
                         "(MIN(join_day)=1 OR MAX(join_yr=2011)) " +\
                         "ORDER BY tasks_points.task1"
            actual_result = self.run_cbq_query()

            tmp_groups = set([doc['tasks_points']["task1"] for doc in self.full_list])
            expected_result = [{"task" : group} for group in tmp_groups
                               if [doc['tasks_points']["task1"]
                                   for doc in self.full_list].count(group) >0 and\
                               (min([doc["join_day"] for doc in self.full_list
                                     if doc['tasks_points']["task1"] == group]) == 1 or\
                                max([doc["join_yr"] for doc in self.full_list
                                     if doc['tasks_points']["task1"] == group]) == 2011)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result['results'], expected_result)

    def test_prepared_group_by_aggr_fn(self):
        for bucket in self.buckets:
            self.query = "SELECT tasks_points.task1 AS task from %s " % (bucket.name) +\
                         "WHERE join_mo>7 GROUP BY tasks_points.task1 " +\
                         "HAVING COUNT(tasks_points.task1) > 0 AND "  +\
                         "(MIN(join_day)=1 OR MAX(join_yr=2011)) " +\
                         "ORDER BY tasks_points.task1"
            self.prepared_common_body()

    def test_group_by_satisfy(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, AVG(test_rate) as avg_rate FROM %s " % (bucket.name) +\
                         "WHERE (ANY skill IN %s.skills SATISFIES skill = 'skill2010' end) " % (
                                                                      bucket.name) +\
                         "AND (ANY vm IN %s.VMs SATISFIES vm.RAM = 5 end) "  % (
                                                                      bucket.name) +\
                         "GROUP BY job_title ORDER BY job_title"
            actual_result = self.run_cbq_query()

            tmp_groups = set([doc["job_title"] for doc in self.full_list])
            expected_result = [{"job_title" : doc['job_title'],
                                "test_rate" : doc["test_rate"]}
                               for doc in self.full_list
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
            self._verify_results(actual_result['results'], expected_result)

    def test_group_by_negative(self):
        queries_errors = {"SELECT tasks_points from {0} WHERE tasks_points.task2>3" +\
                          " GROUP BY tasks_points.*" :
                           ('syntax error', 3000),
                           "from {0} WHERE join_mo>7 GROUP BY tasks_points.task1 " +\
                           "SELECT tasks_points.task1 AS task HAVING COUNT(tasks_points.task1) > 0" :
                           ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

##############################################################################################
#
#   SCALAR FN
##############################################################################################

    def test_ceil(self):
        for bucket in self.buckets:
            self.query = "select name, ceil(test_rate) as rate from %s"  % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['rate']))

            expected_result = [{"name" : doc['name'], "rate" : math.ceil(doc['test_rate'])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['rate']))
            self._verify_results(actual_result, expected_result)

            self.query = "select name from %s where ceil(test_rate) > 5"  % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['name']))
            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if math.ceil(doc['test_rate']) > 5]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_floor(self):
        for bucket in self.buckets:
            self.query = "select name, floor(test_rate) as rate from %s"  % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['rate']))

            expected_result = [{"name" : doc['name'], "rate" : math.floor(doc['test_rate'])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['rate']))
            self._verify_results(actual_result, expected_result)

            self.query = "select name from %s where floor(test_rate) > 5"  % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['name']))
            expected_result = [{"name" : doc['name']} for doc in self.full_list
                               if math.floor(doc['test_rate']) > 5]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

            self.query = "select name from %s where floor(job_title) > 5"  %(bucket.name)
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['results'], [])

    def test_greatest(self):
        for bucket in self.buckets:
            self.query = "select name, GREATEST(skills[0], skills[1]) as SKILL from %s"  % (
                                                                                bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['SKILL']))

            expected_result = [{"name" : doc['name'], "SKILL" :
                                (doc['skills'][0], doc['skills'][1])[doc['skills'][0]<doc['skills'][1]]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['SKILL']))
            self._verify_results(actual_result, expected_result)

    def test_least(self):
        for bucket in self.buckets:
            self.query = "select name, LEAST(skills[0], skills[1]) as SKILL from %s"  % (
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['SKILL']))

            expected_result = [{"name" : doc['name'], "SKILL" :
                                (doc['skills'][0], doc['skills'][1])[doc['skills'][0]>doc['skills'][1]]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['SKILL']))
            self._verify_results(actual_result, expected_result)

    def test_meta(self):
        for bucket in self.buckets:
            self.query = 'SELECT distinct name FROM %s WHERE META(%s).`type` = "json"'  % (
                                                                            bucket.name, bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name']))

            expected_result = [{"name" : doc['name']} for doc in self.full_list]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT distinct name FROM %s WHERE META(%s).id IS NOT NULL"  % (
                                                                                   bucket.name, bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_meta_like(self):
        for bucket in self.buckets:
            self.query = 'SELECT name FROM %s WHERE META(%s).id LIKE "query-test%"'  % (
                                                                            bucket.name, bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name']))

            expected_result = [{"name" : doc['name']} for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_prepared_meta_like(self):
        for bucket in self.buckets:
            self.query = 'SELECT name FROM %s WHERE META(%s).id '  % (bucket.name, bucket.name) +\
                         'LIKE "query-test%"'
            self.prepared_common_body()

    def test_meta_flags(self):
        for bucket in self.buckets:
            self.query = 'SELECT DISTINCT META(%s).flags as flags FROM %s'  % (
                                                                bucket.name, bucket.name)
            actual_result = self.run_cbq_query()
            expected_result = [{"flags" : self.item_flag}]
            self._verify_results(actual_result['results'], expected_result)

    def test_meta_cas(self):
        for bucket in self.buckets:
            self.query = 'SELECT META(%s).cas as cas, META(%s).id as id FROM %s'  % (bucket.name, bucket.name, bucket.name)
            actual_result = self.run_cbq_query()
            keys = [doc['id'] for doc in actual_result['results']]
            actual_result = [{"cas": int(doc['cas'])} for doc in actual_result['results']]
            actual_result = sorted(actual_result,
                                   key=lambda doc: (doc['cas']))
            client = MemcachedClientHelper.direct_client(self.master, bucket.name)
            expected_result = []
            for key in keys:
                a, cas, b = client.get(key.encode('utf-8'))
                expected_result.append({"cas" : cas})
            expected_result = sorted(expected_result, key=lambda doc: (doc['cas']))
            self._verify_results(actual_result, expected_result)

    def test_meta_negative(self):
        queries_errors = {'SELECT distinct name FROM %s WHERE META().type = "json"' : ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_length(self):
        for bucket in self.buckets:
            self.query = 'Select name, email from %s where LENGTH(job_title) = 5'  % (
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name']))

            expected_result = [{"name" : doc['name'], "email" : doc['email']}
                               for doc in self.full_list
                               if len(doc['job_title']) == 5]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_upper(self):
        for bucket in self.buckets:
            self.query = 'SELECT DISTINCT UPPER(job_title) as JOB from %s'  % (
                                                                        bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['JOB']))

            expected_result = [{"JOB" : doc['job_title'].upper()}
                               for doc in self.full_list]
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['JOB']))
            self._verify_results(actual_result, expected_result)

    def test_lower(self):
        for bucket in self.buckets:
            self.query = "SELECT distinct email from %s" % (bucket.name) +\
                         " WHERE LOWER(job_title) < 't'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['email']))

            expected_result = [{"email" : doc['email'].lower()}
                               for doc in self.full_list
                               if doc['job_title'].lower() < 't']
            expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
            expected_result = sorted(expected_result, key=lambda doc: (doc['email']))
            self._verify_results(actual_result, expected_result)

    def test_round(self):
        for bucket in self.buckets:
            self.query = "select name, round(test_rate) as rate from %s" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['rate']))

            expected_result = [{"name": doc["name"], "rate" : round(doc['test_rate'])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['rate']))
            self._verify_results(actual_result, expected_result)

    def test_substr(self):
        for bucket in self.buckets:
            self.query = "select name, SUBSTR(email, 7) as DOMAIN from %s" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['DOMAIN']))

            expected_result = [{"name": doc["name"], "DOMAIN" : doc['email'][7:]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['DOMAIN']))
            self._verify_results(actual_result, expected_result)

    def test_trunc(self):
        for bucket in self.buckets:
            self.query = "select name, TRUNC(test_rate, 0) as rate from %s" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['rate']))

            expected_result = [{"name": doc["name"], "rate" : math.trunc(doc['test_rate'])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['rate']))
            self._verify_results(actual_result, expected_result)

    def test_first(self):
        for bucket in self.buckets:
            self.query = "select name, FIRST vm.os for vm in VMs end as OS from %s" % (
                                                                           bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['name'], doc['OS']))

            expected_result = [{"name": doc["name"], "OS" : doc['VMs'][0]["os"]}
                               for doc in self.full_list]
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

            tmp_groups = set([doc['join_mo'] for doc in self.full_list
                              if doc['join_mo'] < 5])
            expected_result = [{"join_mo" : group,
                                "points_sum" : math.fsum([doc['tasks_points']['task1']
                                                          for doc in self.full_list
                                                          if doc['join_mo'] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT join_mo, SUM(test_rate) as rate FROM %s " % (bucket.name) +\
                         "as employees WHERE job_title='Sales' GROUP BY join_mo " +\
                         "HAVING SUM(employees.test_rate) > 0 and " +\
                         "SUM(test_rate) < 100000"
            actual_result = self.run_cbq_query()
            actual_result = [{"join_mo" : doc["join_mo"], "rate" : round(doc["rate"])} for doc in actual_result['results']]
            actual_result = sorted(actual_result, key=lambda doc: (doc['join_mo']))
            tmp_groups = set([doc['join_mo'] for doc in self.full_list])
            expected_result = [{"join_mo" : group,
                                "rate" : round(math.fsum([doc['test_rate']
                                                          for doc in self.full_list
                                                          if doc['join_mo'] == group and\
                                                             doc['job_title'] == 'Sales']))}
                               for group in tmp_groups
                               if math.fsum([doc['test_rate']
                                            for doc in self.full_list
                                            if doc['join_mo'] == group and\
                                            doc['job_title'] == 'Sales'] ) > 0 and\
                                  math.fsum([doc['test_rate']
                                            for doc in self.full_list
                                            if doc['join_mo'] == group and\
                                            doc['job_title'] == 'Sales'] ) < 100000]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result, expected_result)

    def test_prepared_sum(self):
        for bucket in self.buckets:
            self.query = "SELECT join_mo, SUM(tasks_points.task1) as points_sum" +\
                        " FROM %s WHERE join_mo < 5 GROUP BY join_mo " % (bucket.name) +\
                        "ORDER BY join_mo"
            self.prepared_common_body()

    def test_sum_negative(self):
        queries_errors = {'SELECT join_mo, SUM(job_title) as rate FROM %s as employees' +\
                          ' WHERE job_title="Sales" GROUP BY join_mo' : ('syntax error', 3000),
                          'SELECT join_mo, SUM(VMs) as rate FROM %s as employees' +\
                          ' WHERE job_title="Sales" GROUP BY join_mo' : ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_avg(self):
        for bucket in self.buckets:
            self.query = "SELECT join_mo, AVG(tasks_points.task1) as points_avg" +\
                        " FROM %s WHERE join_mo < 5 GROUP BY join_mo " % (bucket.name) +\
                        "ORDER BY join_mo"
            actual_result = self.run_cbq_query()

            tmp_groups = set([doc['join_mo'] for doc in self.full_list
                              if doc['join_mo'] < 5])
            expected_result = [{"join_mo" : group,
                                "points_avg" : math.fsum([doc['tasks_points']['task1']
                                                          for doc in self.full_list
                                                          if doc['join_mo'] == group]) /
                                                len([doc['tasks_points']['task1']
                                                          for doc in self.full_list
                                                          if doc['join_mo'] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result['results'], expected_result)

            self.query = "SELECT join_mo, AVG(test_rate) as rate FROM %s " % (bucket.name) +\
                         "as employees WHERE job_title='Sales' GROUP BY join_mo " +\
                         "HAVING AVG(employees.test_rate) > 0 and " +\
                         "SUM(test_rate) < 100000"
            actual_result = self.run_cbq_query()

            actual_result = [{'join_mo' : doc['join_mo'],
                              'rate' : round(doc['rate'], 2)}
                             for doc in actual_result['results']]
            actual_result = sorted(actual_result, key=lambda doc: (doc['join_mo']))
            tmp_groups = set([doc['join_mo'] for doc in self.full_list])
            expected_result = [{"join_mo" : group,
                                "rate" : math.fsum([doc['test_rate']
                                                          for doc in self.full_list
                                                          if doc['join_mo'] == group and\
                                                             doc['job_title'] == 'Sales'])/
                                               len([doc['test_rate']
                                                          for doc in self.full_list
                                                          if doc['join_mo'] == group and\
                                                             doc['job_title'] == 'Sales'])}
                               for group in tmp_groups
                               if (math.fsum([doc['test_rate']
                                            for doc in self.full_list
                                            if doc['join_mo'] == group and\
                                            doc['job_title'] == 'Sales'])/
                                    len([doc['test_rate']
                                         for doc in self.full_list
                                         if doc['join_mo'] == group and\
                                         doc['job_title'] == 'Sales'])> 0)  and\
                                  math.fsum([doc['test_rate']
                                            for doc in self.full_list
                                            if doc['join_mo'] == group and\
                                            doc['job_title'] == 'Sales'])  < 100000]
            expected_result = [{'join_mo' : doc['join_mo'],
                              'rate' : round(doc['rate'], 2)}
                             for doc in expected_result]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result, expected_result)

    def test_min(self):
        for bucket in self.buckets:
            self.query = "SELECT join_mo, MIN(test_rate) as rate FROM %s " % (bucket.name) +\
                         "as employees WHERE job_title='Sales' GROUP BY join_mo " +\
                         "HAVING MIN(employees.test_rate) > 0 and " +\
                         "SUM(test_rate) < 100000"

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['join_mo']))
            tmp_groups = set([doc['join_mo'] for doc in self.full_list])
            expected_result = [{"join_mo" : group,
                                "rate" : min([doc['test_rate']
                                              for doc in self.full_list
                                              if doc['join_mo'] == group and\
                                              doc['job_title'] == 'Sales'])}
                               for group in tmp_groups
                               if min([doc['test_rate']
                                       for doc in self.full_list
                                       if doc['join_mo'] == group and\
                                       doc['job_title'] == 'Sales']) > 0 and\
                                  math.fsum([doc['test_rate']
                                            for doc in self.full_list
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

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['join_mo']))
            tmp_groups = set([doc['join_mo'] for doc in self.full_list])
            expected_result = [{"join_mo" : group,
                                "rate" : max([doc['test_rate']
                                              for doc in self.full_list
                                              if doc['join_mo'] == group and\
                                              doc['job_title'] == 'Sales'])}
                               for group in tmp_groups
                               if max([doc['test_rate']
                                       for doc in self.full_list
                                       if doc['join_mo'] == group and\
                                       doc['job_title'] == 'Sales'] ) > 0 and\
                                  math.fsum([doc['test_rate']
                                            for doc in self.full_list
                                            if doc['join_mo'] == group and\
                                            doc['job_title'] == 'Sales'] ) < 100000]
            expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
            self._verify_results(actual_result, expected_result)

    def test_array_agg_distinct(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_agg(DISTINCT name) as names" +\
            " FROM %s GROUP BY job_title" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_list = [{"job_title" : group,
                                "names" : set([x["name"] for x in self.full_list
                                               if x["job_title"] == group])}
                               for group in tmp_groups]
            expected_result = self.sort_nested_list(expected_list)
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_length(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_length(array_agg(DISTINCT name)) as num_names" +\
            " FROM %s GROUP BY job_title" % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "num_names" : len(set([x["name"] for x in self.full_list
                                               if x["job_title"] == group]))}
                               for group in tmp_groups]

            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)
            self.query = "SELECT job_title, array_length(array_agg(DISTINCT test_rate)) as rates" +\
            " FROM %s GROUP BY job_title" % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['job_title']))
            expected_result = [{"job_title" : group,
                                "rates" : len(set([x["test_rate"] for x in self.full_list
                                               if x["job_title"] == group]))}
                               for group in tmp_groups]

            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_append(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title," +\
                         " array_append(array_agg(DISTINCT name), 'new_name') as names" +\
                         " FROM %s GROUP BY job_title" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "names" : sorted(set([x["name"] for x in self.full_list
                                               if x["job_title"] == group] + ['new_name']))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_prepared_array_append(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title," +\
                         " array_append(array_agg(DISTINCT name), 'new_name') as names" +\
                         " FROM %s GROUP BY job_title" % (bucket.name)

            self.prepared_common_body()

    def test_array_concat(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title," +\
                         " array_concat(array_agg(name), array_agg(email)) as names" +\
                         " FROM %s GROUP BY job_title" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "names" : sorted([x["name"] for x in self.full_list
                                                  if x["job_title"] == group] + \
                                                 [x["email"] for x in self.full_list
                                                  if x["job_title"] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_prepend(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title," +\
                         " array_prepend(1.2, array_agg(test_rate)) as rates" +\
                         " FROM %s GROUP BY job_title" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "rates" : sorted([x["test_rate"] for x in self.full_list
                                                  if x["job_title"] == group] + [1.2])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)
            self.query = "SELECT job_title," +\
                         " array_prepend(['skill5', 'skill8'], array_agg(skills)) as skills_new" +\
                         " FROM %s GROUP BY job_title" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "skills_new" : sorted([x["skills"] for x in self.full_list
                                                  if x["job_title"] == group] + \
                                                  [['skill5', 'skill8']])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_remove(self):
        value = 'employee-1'
        for bucket in self.buckets:
            self.query = "SELECT job_title," +\
                         " array_remove(array_agg(DISTINCT name), '%s') as names" % (value) +\
                         " FROM %s GROUP BY job_title" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "names" : sorted(set([x["name"] for x in self.full_list
                                               if x["job_title"] == group and x["name"]!= value]))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_avg(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_avg(array_agg(test_rate))" +\
            " as rates FROM %s GROUP BY job_title" % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['job_title']))
            for doc in actual_result:
                doc['rates'] = round(doc['rates'])
            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "rates" : round(sum([x["test_rate"] for x in self.full_list
                                           if x["job_title"] == group]) / float(len([x["test_rate"]
                                                                                     for x in self.full_list
                                           if x["job_title"] == group])))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_contains(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_contains(array_agg(name), 'employee-1')" +\
            " as emp_job FROM %s GROUP BY job_title" % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "emp_job" : 'employee-1' in [x["name"] for x in self.full_list
                                                           if x["job_title"] == group] }
                               for group in tmp_groups]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_array_count(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_count(array_agg(name)) as names" +\
            " FROM %s GROUP BY job_title" % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "names" : len([x["name"] for x in self.full_list
                                           if x["job_title"] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_distinct(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_distinct(array_agg(name)) as names" +\
            " FROM %s GROUP BY job_title" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "names" : sorted(set([x["name"] for x in self.full_list
                                               if x["job_title"] == group]))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_max(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_max(array_agg(test_rate)) as rates" +\
            " FROM %s GROUP BY job_title" % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "rates" : max([x["test_rate"] for x in self.full_list
                                           if x["job_title"] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_sum(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, round(array_sum(array_agg(test_rate))) as rates" +\
            " FROM %s GROUP BY job_title" % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "rates" : round(sum([x["test_rate"] for x in self.full_list
                                           if x["job_title"] == group]))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_min(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_min(array_agg(test_rate)) as rates" +\
            " FROM %s GROUP BY job_title" % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "rates" : min([x["test_rate"] for x in self.full_list
                                           if x["job_title"] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_position(self):
        self.query = "select array_position(['support', 'qa'], 'dev') as index_num"
        actual_result = self.run_cbq_query()
        expected_result = [{"index_num" : -1}]
        self._verify_results(actual_result['results'], expected_result)

        self.query = "select array_position(['support', 'qa'], 'qa') as index_num"
        actual_result = self.run_cbq_query()
        expected_result = [{"index_num" : 1}]
        self._verify_results(actual_result['results'], expected_result)

    def test_array_put(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_put(array_agg(distinct name), 'employee-1') as emp_job" +\
            " FROM %s GROUP BY job_title" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result,
                                   key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "emp_job" : sorted(set([x["name"] for x in self.full_list
                                           if x["job_title"] == group]))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT job_title, array_put(array_agg(distinct name), 'employee-47') as emp_job" +\
            " FROM %s GROUP BY job_title" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result,
                                   key=lambda doc: (doc['job_title']))

            expected_result = [{"job_title" : group,
                                "emp_job" : sorted(set([x["name"] for x in self.full_list
                                           if x["job_title"] == group] + ['employee-47']))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_range(self):
        self.query = "select array_range(0,10) as num"
        actual_result = self.run_cbq_query()
        expected_result = [{"num" : list(xrange(10))}]
        self._verify_results(actual_result['results'], expected_result)

    def test_array_replace(self):
        for bucket in self.buckets:

            self.query = "SELECT job_title, array_replace(array_agg(name), 'employee-1', 'employee-47') as emp_job" +\
            " FROM %s GROUP BY job_title" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result,
                                   key=lambda doc: (doc['job_title']))
            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "emp_job" : sorted(["employee-47" if x["name"] == 'employee-1' else x["name"]
                                             for x in self.full_list
                                             if x["job_title"] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_array_repeat(self):
        self.query = "select ARRAY_REPEAT(2, 2) as num"
        actual_result = self.run_cbq_query()
        expected_result = [{"num" : [2] * 2}]
        self._verify_results(actual_result['results'], expected_result)

    def test_array_reverse(self):
        self.query = "select array_reverse([1,2,3]) as num"
        actual_result = self.run_cbq_query()
        expected_result = [{"num" : [3,2,1]}]
        self._verify_results(actual_result['results'], expected_result)

    def test_array_sort(self):
        for bucket in self.buckets:

            self.query = "SELECT job_title, array_sort(array_agg(distinct test_rate)) as emp_job" +\
            " FROM %s GROUP BY job_title" % (bucket.name)

            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['job_title']))
            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "emp_job" : sorted(set([x["test_rate"] for x in self.full_list
                                             if x["job_title"] == group]))}
                               for group in tmp_groups]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title']))
            self._verify_results(actual_result, expected_result)

    def test_poly_length(self):
        for bucket in self.buckets:

            query_fields = ['tasks_points', 'VMs', 'skills']
            for query_field in query_fields:
                self.query = "Select length(%s) as custom_num from %s order by custom_num" % (query_field, bucket.name)
                actual_result = self.run_cbq_query()
                expected_result = [{"custom_num" : None} for doc in self.full_list]
                self._verify_results(actual_result['results'], expected_result)

                self.query = "Select poly_length(%s) as custom_num from %s order by custom_num" % (query_field, bucket.name)
                actual_result = self.run_cbq_query()
                expected_result = [{"custom_num" : len(doc[query_field])}
                                   for doc in self.full_list]
                expected_result = sorted(expected_result, key=lambda doc: (doc['custom_num']))
                self._verify_results(actual_result['results'], expected_result)

    def test_array_agg(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, array_agg(name) as names" +\
            " FROM %s GROUP BY job_title" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = self.sort_nested_list(actual_list['results'])
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_list = [{"job_title" : group,
                                "names" : [x["name"] for x in self.full_list
                                               if x["job_title"] == group]}
                               for group in tmp_groups]
            expected_result = self.sort_nested_list(expected_list)
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
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name'],
                                                                       doc['period']))

            expected_result = [{"name" : doc['name'],
                                "period" : ((('autumn','summer')[doc['join_mo'] in [6,7,8]],
                                             'spring')[doc['join_mo'] in [3,4,5]],'winter')
                                             [doc['join_mo'] in [12,1,2]]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['period']))
            self._verify_results(actual_result, expected_result)

    def test_case_expr(self):
        for bucket in self.buckets:
            self.query = "SELECT name, CASE job_title WHEN 'Sales' THEN 'Marketing'" +\
                         "ELSE job_title END AS dept FROM %s" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name'],
                                                                       doc['dept']))

            expected_result = [{"name" : doc['name'],
                                "dept" : (doc['job_title'], 'Marketing')[doc['job_title'] == 'Sales']}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                       doc['dept']))
            self._verify_results(actual_result, expected_result)

    def test_case_arithm(self):
        self.query = "SELECT CASE WHEN 1+1=3 THEN 7+7 WHEN 2+2=5 THEN 8+8 ELSE 2 END"
        actual_result = self.run_cbq_query()
        expected_result = [{"$1" : 2}]
        self._verify_results(actual_result['results'], expected_result)

    def test_in_int(self):
        for bucket in self.buckets:
            self.query = "select name from %s where join_mo in [1,6]" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name']))

            expected_result = [{"name" : doc['name']}
                               for doc in self.full_list
                               if doc['join_mo'] in [1,6]]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

            self.query = "select name from %s where join_mo not in [1,6]" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name']))
            expected_result = [{"name" : doc['name']}
                               for doc in self.full_list
                               if not(doc['join_mo'] in [1,6])]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_in_str(self):
        for bucket in self.buckets:
            self.query = "select name from %s where job_title in ['Sales', 'Support']" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name']))

            expected_result = [{"name" : doc['name']}
                               for doc in self.full_list
                               if doc['job_title'] in ['Sales', 'Support']]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

            self.query = "select name from %s where job_title not in ['Sales', 'Support']" % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name']))
            expected_result = [{"name" : doc['name']}
                               for doc in self.full_list
                               if not(doc['job_title'] in ['Sales', 'Support'])]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_prepared_in_str(self):
        for bucket in self.buckets:
            self.query = "select name from %s where job_title in ['Sales', 'Support']" % (bucket.name)
            self.prepared_common_body()

    def test_logic_expr(self):
        for bucket in self.buckets:
            self.query = "SELECT tasks_points.task1 as task FROM %s WHERE " % (bucket.name)+\
            "tasks_points.task1 > 1 AND tasks_points.task1 < 4"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['task']))

            expected_result = [{"task" : doc['tasks_points']['task1']}
                               for doc in self.full_list
                               if doc['tasks_points']['task1'] > 1 and\
                                  doc['tasks_points']['task1'] < 4]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result, expected_result)

    def test_comparition_equal_int(self):
        for bucket in self.buckets:
            self.query = "SELECT tasks_points.task1 as task FROM %s WHERE " % (bucket.name)+\
            "tasks_points.task1 = 4"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['task']))

            expected_result = [{"task" : doc['tasks_points']['task1']}
                               for doc in self.full_list
                               if doc['tasks_points']['task1'] == 4]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result, expected_result)

            self.query = "SELECT tasks_points.task1 as task FROM %s WHERE " % (bucket.name)+\
            "tasks_points.task1 == 4"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['task']))
            self._verify_results(actual_result, expected_result)

    def test_comparition_equal_str(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM %s WHERE " % (bucket.name)+\
            "name = 'employee-4'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name']))

            expected_result = [{"name" : doc['name']}
                               for doc in self.full_list
                               if doc['name'] == 'employee-4']
            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result, expected_result)
            
            self.query = "SELECT name FROM %s WHERE " % (bucket.name)+\
            "name == 'employee-4'"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['name']))
            self._verify_results(actual_result, expected_result)

    def test_comparition_not_equal(self):
        for bucket in self.buckets:
            self.query = "SELECT tasks_points.task1 as task FROM %s WHERE " % (bucket.name)+\
            "tasks_points.task1 != 1"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['task']))

            expected_result = [{"task" : doc['tasks_points']['task1']}
                               for doc in self.full_list
                               if doc['tasks_points']['task1'] != 1]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result, expected_result)

    def test_comparition_more_less_equal(self):
        for bucket in self.buckets:
            self.query = "SELECT tasks_points.task1 as task FROM %s WHERE " % (bucket.name)+\
            "tasks_points.task1 >= 1 AND tasks_points.task1 <= 4"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (
                                                                       doc['task']))

            expected_result = [{"task" : doc['tasks_points']['task1']}
                               for doc in self.full_list
                               if doc['tasks_points']['task1'] >= 1 and
                               doc['tasks_points']['task1'] <= 4]
            expected_result = sorted(expected_result, key=lambda doc: (doc['task']))
            self._verify_results(actual_result, expected_result)

    def test_comparition_expr(self):
        for bucket in self.buckets:
            self.query = "SELECT tasks_points.task1 as task FROM %s WHERE " % (bucket.name)+\
            "tasks_points.task1 > tasks_points.task1"
            actual_result = self.run_cbq_query()
            self._verify_results(actual_result['results'], [])

    def test_arithm(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, SUM(test_rate) % COUNT(distinct join_yr)" +\
            " as avg_per_year from {0} group by job_title".format(bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = [{"job_title": doc["job_title"],
                              "avg_per_year" : round(doc["avg_per_year"],2)}
                              for doc in actual_result['results']]
            actual_result = sorted(actual_result, key=lambda doc: (doc['job_title']))

            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "avg_per_year" : math.fsum([doc['test_rate']
                                                             for doc in self.full_list
                                                             if doc['job_title'] == group]) %\
                                                  len(set([doc['join_yr'] for doc in self.full_list
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
                              for doc in actual_result['results']]
            actual_result = sorted(actual_result, key=lambda doc: (
                                                                       doc['job_title']))
            tmp_groups = set([doc['job_title'] for doc in self.full_list])
            expected_result = [{"job_title" : group,
                                "avg_per_year" : (math.fsum([doc['tasks_points']['task1']
                                                             for doc in self.full_list
                                                             if doc['job_title'] == group])+\
                                                  math.fsum([doc['tasks_points']['task2']
                                                             for doc in self.full_list
                                                             if doc['job_title'] == group]))%\
                                                  len(set([doc['join_yr'] for doc in self.full_list
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
            self.assertTrue(res["results"][0]["~children"][0]["index"] == "#primary",
                            "Type should be #primary, but is: %s" % res["results"])

##############################################################################################
#
#   DATETIME
##############################################################################################

    def test_clock_millis(self):
        self.query = "select clock_millis() as now"
        now = time.time()
        res = self.run_cbq_query()
        self.assertTrue((res["results"][0]["now"] > now * 1000),
                        "Result expected to be in: [%s ... %s]. Actual %s" % (
                                        now * 1000, (now + 10) * 1000, res["results"]))

    def test_clock_str(self):
        self.query = "select clock_str() as now"
        now = datetime.datetime.now()
        res = self.run_cbq_query()
        expected = "%s-%02d-%02dT" % (now.year, now.month, now.day)
        self.assertTrue(res["results"][0]["now"].startswith(expected),
                        "Result expected: %s. Actual %s" % (expected, res["results"]))

    def test_date_add_millis(self):
        self.query = "select date_add_millis(clock_millis(), 100, 'day') as now"
        now = time.time()
        res = self.run_cbq_query()
        self.assertTrue((res["results"][0]["now"] > now * 1000 + 100*24*60*60),
                        "Result expected to be in: [%s ... %s]. Actual %s" % (
                                        now * 1000 + 100*24*60*60, (now + 10) * 1000+ 100*24*60*60, res["results"]))

    def test_date_add_str(self):
        self.query = "select date_add_str(clock_str(), 10, 'day') as now"
        now = datetime.datetime.now() + datetime.timedelta(days=10)
        res = self.run_cbq_query()
        expected = "%s-%02d-%02dT%02d:" % (now.year, now.month, now.day, now.hour)
        expected_delta = "%s-%02d-%02dT%02d:" % (now.year, now.month, now.day, now.hour + 1)
        self.assertTrue(res["results"][0]["now"].startswith(expected) or res["results"][0]["now"].startswith(expected_delta),
                        "Result expected: %s. Actual %s" % (expected, res["results"]))

    def test_date_diff_millis(self):
        self.query = "select date_diff_millis(clock_millis(), date_add_millis(clock_millis(), 100, 'day'), 'day') as now"
        res = self.run_cbq_query()
        self.assertTrue(res["results"][0]["now"] == -100,
                        "Result expected: %s. Actual %s" % (-100, res["results"]))

    def test_date_diff_str(self):
        self.query = 'select date_diff_str("2014-08-24T01:33:59", "2014-08-24T07:33:59", "minute") as now'
        res = self.run_cbq_query()
        self.assertTrue(res["results"][0]["now"] == -360,
                        "Result expected: %s. Actual %s" % (-360, res["results"]))
        self.query = 'select date_diff_str("2014-08-24T01:33:59", "2014-08-24T07:33:59", "hour") as now'
        res = self.run_cbq_query()
        self.assertTrue(res["results"][0]["now"] == -6,
                        "Result expected: %s. Actual %s" % (-6, res["results"]))

    def test_now(self):
        self.query = "select now_str() as now"
        now = datetime.datetime.now()
        today = date.today()
        res = self.run_cbq_query()
        expected = "%s-%02d-%02dT" % (today.year, today.month, today.day,)
        self.assertTrue(res["results"][0]["now"].startswith(expected),
                        "Result expected: %s. Actual %s" % (expected, res["results"]))

    def test_hours(self):
        self.query = 'select date_part_str(now_str(), "hour") as hour, ' +\
        'date_part_str(now_str(),"minute") as minute, date_part_str(' +\
        'now_str(),"second") as sec, date_part_str(now_str(),"milliseconds") as msec'
        now = datetime.datetime.now()
        res = self.run_cbq_query()
        self.assertTrue(res["results"][0]["hour"] == now.hour or res["results"][0]["hour"] == (now.hour + 1),
                        "Result for hours expected: %s. Actual %s" % (now.hour, res["results"]))
        self.assertTrue("minute" in res["results"][0], "No minute field")
        self.assertTrue("sec" in res["results"][0], "No second field")
        self.assertTrue("msec" in res["results"][0], "No msec field")

    def test_where(self):
        for bucket in self.buckets:
            self.query = 'select name, join_yr, join_mo, join_day from %s' % (bucket.name) +\
            ' where date_part_str(now_str(),"month") < join_mo AND date_part_str(now_str(),"year")' +\
            ' > join_yr AND date_part_str(now_str(),"day") < join_day'
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result["results"], key=lambda doc: (doc['name'],
                                                                                doc['join_yr'],
                                                                                doc['join_mo'],
                                                                                doc['join_day']))

            today = date.today()
            expected_result = [{"name" : doc['name'], "join_yr" : doc['join_yr'],
                                "join_mo" : doc['join_mo'], "join_day" : doc['join_day']}
                               for doc in self.full_list
                               if doc['join_yr'] < today.year and\
                               doc['join_mo'] > today.month and\
                               doc['join_day'] > today.day]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                        doc['join_yr'],
                                                                        doc['join_mo'],
                                                                        doc['join_day']))
            self._verify_results(actual_result, expected_result)

    def test_prepared_date_where(self):
        for bucket in self.buckets:
            self.query = 'select name, join_yr, join_mo, join_day from %s' % (bucket.name) +\
            ' where date_part_str(now_str(),"month") < join_mo AND date_part_str(now_str(),"year")' +\
            ' > join_yr AND date_part_str(now_str(),"day") < join_day'
            self.prepared_common_body()

    def test_now_millis(self):
        self.query = "select now_millis() as now"
        now = time.time()
        res = self.run_cbq_query()
        self.assertTrue((res["results"][0]["now"] > now * 1000),
                        "Result expected to be in: [%s ... %s]. Actual %s" % (
                                        now * 1000, (now + 10) * 1000, res["results"]))

    def test_str_to_millis(self):
        now_millis = time.time()
        now_time = datetime.datetime.fromtimestamp(now_millis)
        now_millis = now_millis * 1000
        try:
            now_time_zone = round((round((datetime.datetime.now()-datetime.datetime.utcnow()).total_seconds())/1800)/2)
        except AttributeError, ex:
            raise Exception("Test requires python 2.7 : SKIPPING TEST")
        now_time_str = "%s-%02d-%02d" % (now_time.year, now_time.month, now_time.day)
        self.query = "select str_to_millis('%s') as now" % now_time_str
        res = self.run_cbq_query()
        self.assertTrue((res["results"][0]["now"] < (now_millis)) and\
                        (res["results"][0]["now"] > (now_millis - 5184000000)),
                        "Result expected to be in: [%s ... %s]. Actual %s" % (
                                       now_millis - 5184000000, now_millis, res["results"]))

    def test_millis_to_str(self):
        now_millis = time.time()
        now_time = datetime.datetime.fromtimestamp(now_millis)
        expected = "%s-%02d-%02dT%02d:%02d" % (now_time.year, now_time.month, now_time.day,
                                         now_time.hour, now_time.minute)
        self.query = "select millis_to_str(%s) as now" % (now_millis * 1000)
        res = self.run_cbq_query()
        self.assertTrue(res["results"][0]["now"].startswith(expected),
                        "Result expected: %s. Actual %s" % (expected, res["results"]))

    def test_date_part_millis(self):
        now_millis = time.time()
        now_time = datetime.datetime.fromtimestamp(now_millis)
        now_millis = now_millis * 1000
        self.query = 'select date_part_millis(%s, "hour") as hour, ' % (now_millis) +\
        'date_part_millis(%s,"minute") as minute, date_part_millis(' % (now_millis) +\
        '%s,"second") as sec, date_part_millis(%s,"milliseconds") as msec' % (now_millis,now_millis)
        res = self.run_cbq_query()
        self.assertTrue(res["results"][0]["hour"] == now_time.hour,
                        "Result expected: %s. Actual %s" % (now_time.hour, res["results"]))
        self.assertTrue(res["results"][0]["minute"] == now_time.minute,
                        "Result expected: %s. Actual %s" % (now_time.minute, res["results"]))
        self.assertTrue(res["results"][0]["sec"] == now_time.second,
                        "Result expected: %s. Actual %s" % (now_time.second, res["results"]))
        self.assertTrue("msec" in res["results"][0], "There are no msec in results")


    def test_where_millis(self):
        for bucket in self.buckets:
            self.query = "select join_yr, join_mo, join_day, name from %s" % (bucket.name) +\
            " where join_mo < 10 and join_day < 10 and str_to_millis(tostr(join_yr) || '-0'" +\
            " || tostr(join_mo) || '-0' || tostr(join_day)) < now_millis()"
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result["results"], key=lambda doc: (doc['name'],
                                                                                doc['join_yr'],
                                                                                doc['join_mo'],
                                                                                doc['join_day']))

            expected_result = [{"name" : doc['name'], "join_yr" : doc['join_yr'],
                                "join_mo" : doc['join_mo'], "join_day" : doc['join_day']}
                               for doc in self.full_list
                               if doc['join_mo'] < 10 and doc['join_day'] < 10]
            expected_result = sorted(expected_result, key=lambda doc: (doc['name'],
                                                                        doc['join_yr'],
                                                                        doc['join_mo'],
                                                                        doc['join_day']))
            self._verify_results(actual_result, expected_result)

    def test_order_by_dates(self):
        orders = ["asc", "desc"]
        for order in orders:
            for bucket in self.buckets:
                self.query = "select millis_to_str(str_to_millis('2010-01-01')) as date"
                actual_result = self.run_cbq_query()
                self.assertTrue(actual_result["results"][0]["date"][:10] == '2010-01-01', 'Actual result %s' %  actual_result)
                self.query = "select join_yr, join_mo, join_day, millis_to_str(str_to_millis(tostr(join_yr) || '-0' ||" +\
                " tostr(join_mo) || '-0' || tostr(join_day))) as date from %s" % (bucket.name) +\
                " where join_mo < 10 and join_day < 10 ORDER BY date %s" % order
                actual_result = self.run_cbq_query()
                actual_result = ([{"date" : doc["date"][:10],
                                   "join_yr" : doc['join_yr'],
                                    "join_mo": doc['join_mo'],
                                    "join_day": doc['join_day']} for doc in actual_result["results"]])
    
                expected_result = [{"date" : '%s-0%s-0%s' % (doc['join_yr'],
                                    doc['join_mo'], doc['join_day']),
                                    "join_yr" : doc['join_yr'],
                                    "join_mo": doc['join_mo'],
                                    "join_day": doc['join_day']}
                                   for doc in self.full_list
                                   if doc['join_mo'] < 10 and doc['join_day'] < 10]
                expected_result = sorted(expected_result, key=lambda doc: (doc['date']), reverse=(order=='desc'))
                self._verify_results(actual_result, expected_result)

##############################################################################################
#
#   TYPE FNS
##############################################################################################

    def test_type(self):
        types_list = [("name", "string"), ("tasks_points", "object"),
                      ("some_wrong_key", "missing"),
                      ("skills", "array"), ("VMs[0].RAM", "number"),
                      ("true", "boolean"), ("test_rate", "number"),
                      ("test.test[0]", "missing")]
        for bucket in self.buckets:
            for name_item, type_item in types_list:
                self.query = 'SELECT TYPENAME(%s) as type_output FROM %s' % (
                                                        name_item, bucket.name)
                actual_result = self.run_cbq_query()
                for doc in actual_result['results']:
                    self.assertTrue(doc["type_output"] == type_item,
                                    "Expected type for %s: %s. Actual: %s" %(
                                                    name_item, type_item, doc["type_output"]))
                self.log.info("Type for %s(%s) is checked." % (name_item, type_item))

    def test_check_types(self):
        types_list = [("name", "ISSTR", True), ("skills[0]", "ISSTR", True),
                      ("test_rate", "ISSTR", False), ("VMs", "ISSTR", False),
                      ("false", "ISBOOL", True), ("join_day", "ISBOOL", False),
                      ("VMs", "ISARRAY", True), ("VMs[0]", "ISARRAY", False),
                      ("skills[0]", "ISARRAY", False), ("skills", "ISARRAY", True)]
        for bucket in self.buckets:
            for name_item, fn, expected_result in types_list:
                self.query = 'SELECT %s(%s) as type_output FROM %s' % (
                                                        fn, name_item, bucket.name)
                actual_result = self.run_cbq_query()
                for doc in actual_result['results']:
                    self.assertTrue(doc["type_output"] == expected_result,
                                    "Expected output for fn %s( %s) : %s. Actual: %s" %(
                                                fn, name_item, expected_result, doc["type_output"]))
                self.log.info("Fn %s(%s) is checked. (%s)" % (fn, name_item, expected_result))

    def test_types_in_satisfy(self):
        for bucket in self.buckets:
            self.query = "SELECT name FROM %s WHERE " % (bucket.name) +\
                         "(EVERY vm IN %s.VMs SATISFIES ISOBJ(vm) END) AND" % (
                                                            bucket.name) +\
                         " ISSTR(email) ORDER BY name"

            actual_result = self.run_cbq_query()
            expected_result = [{"name" : doc['name']}
                               for doc in self.full_list]

            expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
            self._verify_results(actual_result['results'], expected_result)

    def test_to_num(self):
        self.query = 'SELECT tonum("12.12") - tonum("0.12") as num'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'][0]['num'] == 12,
                        "Expected: 12. Actual: %s" % (actual_result['results']))
        self.log.info("TONUM is checked")

    def test_to_str(self):
        for bucket in self.buckets:
            self.query = "SELECT TOSTR(join_mo) month FROM %s" % bucket.name
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'])

            expected_result = [{"month" : str(doc['join_mo'])} for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_to_bool(self):
        self.query = 'SELECT tobool("true") as boo'
        actual_result = self.run_cbq_query()
        self.assertTrue(actual_result['results'][0]['boo'] == True,
                        "Expected: true. Actual: %s" % (actual_result['results']))
        self.log.info("TOBOOL is checked")

    def test_to_array(self):
        for bucket in self.buckets:
            self.query = "SELECT job_title, toarray(name) as names" +\
            " FROM %s" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'], key=lambda doc: (doc['job_title'],
                                                                   doc['names']))
            expected_result = [{"job_title" : doc["job_title"],
                              "names" : [doc["name"]]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['job_title'],
                                                                       doc['names']))
            self._verify_results(actual_result, expected_result)
##############################################################################################
#
#   CONCATENATION
##############################################################################################

    def test_concatenation(self):
        for bucket in self.buckets:
            self.query = "SELECT name || \" \" || job_title as employee" +\
            " FROM %s" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'], key=lambda doc: (doc['employee']))
            expected_result = [{"employee" : doc["name"] + " " + doc["job_title"]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result, key=lambda doc: (doc['employee']))
            self._verify_results(actual_result, expected_result)

    def test_concatenation_where(self):
        for bucket in self.buckets:
            self.query = 'SELECT name, skills' +\
            ' FROM %s WHERE skills[0]=("skill" || "2010")' % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"], "skills" : doc["skills"]}
                               for doc in self.full_list
                               if doc["skills"][0] == 'skill2010']
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)
##############################################################################################
#
#   SPLIT
##############################################################################################

    def test_select_split_fn(self):
        for bucket in self.buckets:
            self.query = "SELECT SPLIT(email, '@')[0] as login" +\
            " FROM %s" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"login" : doc["email"].split('@')[0]}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_split_where(self):
        for bucket in self.buckets:
            self.query = 'SELECT name FROM %s' % (bucket.name) +\
            ' WHERE SPLIT(email, \'-\')[0] = SPLIT(name, \'-\')[1]' 

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"]}
                               for doc in self.full_list
                               if doc["email"].split('-')[0] == doc["name"].split('-')[1]]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

##############################################################################################
#
#   UNION
##############################################################################################

    def test_union(self):
        for bucket in self.buckets:
            self.query = "select name from %s union select email from %s" % (bucket.name, bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"]}
                               for doc in self.full_list]
            expected_result.extend([{"email" : doc["email"]}
                                   for doc in self.full_list])
            expected_result = sorted([dict(y) for y in set(tuple(x.items()) for x in expected_result)])
            self._verify_results(actual_result, expected_result)

    def test_prepared_union(self):
        for bucket in self.buckets:
            self.query = "select name from %s union select email from %s" % (bucket.name, bucket.name)
            self.prepared_common_body()

    def test_union_multiply_buckets(self):
        self.assertTrue(len(self.buckets) > 1, 'This test needs more than one bucket')
        self.query = "select name from %s union select email from %s" % (self.buckets[0].name, self.buckets[1].name)
        actual_list = self.run_cbq_query()
        actual_result = sorted(actual_list['results'])
        expected_result = [{"name" : doc["name"]}
                            for doc in self.full_list]
        expected_result.extend([{"email" : doc["email"]}
                                for doc in self.full_list])
        expected_result = sorted([dict(y) for y in set(tuple(x.items()) for x in expected_result)])
        self._verify_results(actual_result, expected_result)

    def test_union_all(self):
        for bucket in self.buckets:
            self.query = "select name from %s union all select email from %s" % (bucket.name, bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"]}
                               for doc in self.full_list]
            expected_result.extend([{"email" : doc["email"]}
                                   for doc in self.full_list])
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_union_all_multiply_buckets(self):
        self.assertTrue(len(self.buckets) > 1, 'This test needs more than one bucket')
        self.query = "select name from %s union all select email from %s" % (self.buckets[0].name, self.buckets[1].name)
        actual_list = self.run_cbq_query()
        actual_result = sorted(actual_list['results'])
        expected_result = [{"name" : doc["name"]}
                            for doc in self.full_list]
        expected_result.extend([{"email" : doc["email"]}
                                for doc in self.full_list])
        expected_result = sorted(expected_result)
        self._verify_results(actual_result, expected_result)

    def test_union_where(self):
        for bucket in self.buckets:
            self.query = "select name from %s union select email from %s where join_mo > 2" % (bucket.name, bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"]}
                               for doc in self.full_list]
            expected_result.extend([{"email" : doc["email"]}
                                   for doc in self.full_list if doc["join_mo"] > 2])
            expected_result = sorted([dict(y) for y in set(tuple(x.items()) for x in expected_result)])
            self._verify_results(actual_result, expected_result)

    def test_union_aggr_fns(self):
        for bucket in self.buckets:
            self.query = "select count(name) as names from %s union select count(email) as emails from %s" % (bucket.name, bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"names" : len(self.full_list)}]
            expected_result.extend([{"emails" : len(self.full_list)}])
            expected_result = sorted([dict(y) for y in set(tuple(x.items()) for x in expected_result)])
            self._verify_results(actual_result, expected_result)

    def test_intersect(self):
        for bucket in self.buckets:
            self.query = "select name from %s intersect select name from %s s where s.join_day>5" % (bucket.name, bucket.name)
            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"]}
                               for doc in self.full_list if doc['join_day'] > 5]
            expected_result = sorted([dict(y) for y in set(tuple(x.items()) for x in expected_result)])
            self._verify_results(actual_result, expected_result)

    def test_intersect_all(self):
        for bucket in self.buckets:
            self.query = "select name from %s intersect all select name from %s s where s.join_day>5" % (bucket.name, bucket.name)
            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"]}
                               for doc in self.full_list if doc['join_day'] > 5]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_prepared_intersect(self):
        for bucket in self.buckets:
            self.query = "select name from %s intersect all select name from %s s where s.join_day>5" % (bucket.name, bucket.name)
            self.prepared_common_body()

    def test_except(self):
        for bucket in self.buckets:
            self.query = "select name from %s except select name from %s s where s.join_day>5" % (bucket.name, bucket.name)
            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"]}
                               for doc in self.full_list if not doc['join_day'] > 5]
            expected_result = sorted([dict(y) for y in set(tuple(x.items()) for x in expected_result)])
            self._verify_results(actual_result, expected_result)

    def test_except_all(self):
        for bucket in self.buckets:
            self.query = "select name from %s except all select name from %s s where s.join_day>5" % (bucket.name, bucket.name)
            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"]}
                               for doc in self.full_list if not doc['join_day'] > 5]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

##############################################################################################
#
#   WITHIN
##############################################################################################

    def test_within_list_object(self):
        for bucket in self.buckets:
            self.query = "select name, VMs from %s WHERE 5 WITHIN VMs" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"], "VMs" : doc["VMs"]}
                               for doc in self.full_list
                               if len([vm for vm in doc["VMs"] if vm["RAM"] == 5])]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_prepared_within_list_object(self):
        for bucket in self.buckets:
            self.query = "select name, VMs from %s WHERE 5 WITHIN VMs" % (bucket.name)
            self.prepared_common_body()

    def test_within_list_of_lists(self):
        for bucket in self.buckets:
            self.query = "select name, VMs from %s where name within [['employee-2', 'employee-4'], ['employee-5']] " % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"], "VMs" : doc["VMs"]}
                               for doc in self.full_list
                               if doc["name"] in ['employee-2', 'employee-4', 'employee-5']]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_within_object(self):
        for bucket in self.buckets:
            self.query = "select name, tasks_points from %s WHERE 1 WITHIN tasks_points" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"], "tasks_points" : doc["tasks_points"]}
                               for doc in self.full_list
                               if doc["tasks_points"]["task1"] == 1 or doc["tasks_points"]["task2"] == 1]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_within_array(self):
        for bucket in self.buckets:
            self.query = " select name, skills from %s where 'skill2010' within skills" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"], "skills" : doc["skills"]}
                               for doc in self.full_list
                               if 'skill2010' in doc["skills"]]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

##############################################################################################
#
#   RAW
##############################################################################################

    def test_raw(self):
        for bucket in self.buckets:
            self.query = "select raw name from %s " % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [doc["name"] for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_raw_limit(self):
        for bucket in self.buckets:
            self.query = "select raw skills[0] from %s limit 5" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [doc["skills"][0] for doc in self.full_list][:5]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

##############################################################################################
#
#  Number fns
##############################################################################################

    def test_abs(self):
        for bucket in self.buckets:
            self.query = "select join_day from %s where join_day > abs(-10)" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [doc["join_day"] for doc in self.full_list
                               if doc["join_day"] > abs(-10)]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_acos(self):
        self.query = "select degrees(acos(0.5))"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': 60}]
        self._verify_results(actual_list['results'], expected_result)
        
    def test_asin(self):
        self.query = "select degrees(asin(0.5))"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': 30}]
        self._verify_results(actual_list['results'], expected_result)

    def test_tan(self):
        self.query = "select tan(radians(45))"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': 1}]
        self._verify_results(actual_list['results'], expected_result)

    def test_ln(self):
        self.query = "select ln(10) = ln(2) + ln(5)"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': True}]
        self._verify_results(actual_list['results'], expected_result)

    def test_power(self):
        self.query = "select power(sin(radians(33)), 2) + power(cos(radians(33)), 2)"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': 1}]
        self._verify_results(actual_list['results'], expected_result)

    def test_sqrt(self):
        self.query = "select sqrt(9)"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': 3}]
        self._verify_results(actual_list['results'], expected_result)

    def test_sign(self):
        self.query = "select sign(-5)"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': -1}]
        self._verify_results(actual_list['results'], expected_result)
        self.query = "select sign(5)"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': 1}]
        self._verify_results(actual_list['results'], expected_result)
        self.query = "select sign(0)"
        actual_list = self.run_cbq_query()
        expected_result = [{'$1': 0}]
        self._verify_results(actual_list['results'], expected_result)

##############################################################################################
#
#   CONDITIONAL FNS
##############################################################################################

    def test_nanif(self):
        for bucket in self.buckets:
            self.query = "select join_day, join_mo, NANIF(join_day, join_mo) as equality" +\
                         " from %s" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"join_day" : doc["join_day"], "join_mo" : doc["join_mo"],
                                "equality" : doc["join_day"] if doc["join_day"]!=doc["join_mo"] else 'NaN'}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_posinf(self):
        for bucket in self.buckets:
            self.query = "select join_day, join_mo, POSINFIF(join_day, join_mo) as equality" +\
                         " from %s" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"join_day" : doc["join_day"], "join_mo" : doc["join_mo"],
                                "equality" : doc["join_day"] if doc["join_day"]!=doc["join_mo"] else '+Infinity'}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

##############################################################################################
#
#   String FUNCTIONS
##############################################################################################

    def test_uuid(self):
        self.query = "select uuid() as uuid"
        actual_list = self.run_cbq_query()
        self.assertTrue('uuid' in actual_list['results'][0] and actual_list['results'][0]['uuid'], 'UUid is not working')

    def test_string_fn_negative(self):
        queries_errors = {'select name from %s when contains(VMs, "Sale")' : ('syntax error', 3000),
                          'select TITLE(test_rate) as OS from %s' : ('syntax error', 3000),
                          'select REPEAT(name, -2) as name from %s' : ('syntax error', 3000),
                          'select REPEAT(name, a) as name from %s' : ('syntax error', 3000),}
        self.negative_common_body(queries_errors)

    def test_contains(self):
        for bucket in self.buckets:
            self.query = "select name from %s where contains(job_title, 'Sale')" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"]}
                               for doc in self.full_list
                               if doc['job_title'].find('Sale') != -1]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_initcap(self):
        for bucket in self.buckets:
            self.query = "select INITCAP(VMs[0].os) as OS from %s" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"OS" : (doc["VMs"][0]["os"][0].upper() + doc["VMs"][0]["os"][1:])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_title(self):
        for bucket in self.buckets:
            self.query = "select TITLE(VMs[0].os) as OS from %s" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"OS" : (doc["VMs"][0]["os"][0].upper() + doc["VMs"][0]["os"][1:])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_prepared_title(self):
        for bucket in self.buckets:
            self.query = "select TITLE(VMs[0].os) as OS from %s" % (bucket.name)
            self.prepared_common_body()

    def test_position(self):
        for bucket in self.buckets:
            self.query = "select POSITION(VMs[1].name, 'vm') pos from %s" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"pos" : (doc["VMs"][1]["name"].find('vm'))}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)
            self.query = "select POSITION(VMs[1].name, 'server') pos from %s" % (bucket.name)
            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"pos" : (doc["VMs"][1]["name"].find('server'))}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_regex_contains(self):
        for bucket in self.buckets:
            self.query = "select email from %s where REGEXP_CONTAINS(email, '-m..l')" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"email" : doc["email"]}
                               for doc in self.full_list
                               if len(re.compile('-m..l').findall(doc['email'])) > 0]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_regex_like(self):
        for bucket in self.buckets:
            self.query = "select email from %s where REGEXP_LIKE(email, '.*-mail.*')" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"email" : doc["email"]}
                               for doc in self.full_list
                               if re.compile('.*-mail.*').search(doc['email'])]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_regex_position(self):
        for bucket in self.buckets:
            self.query = "select email from %s where REGEXP_POSITION(email, '-m..l*') = 10" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"email" : doc["email"]}
                               for doc in self.full_list
                               if doc['email'].find('-m') == 10]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_regex_replace(self):
        for bucket in self.buckets:
            self.query = "select name, REGEXP_REPLACE(email, '-mail', 'domain') as mail from %s" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"],
                                "mail" : doc["email"].replace('-mail', 'domain')}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)
            self.query = "select name, REGEXP_REPLACE(email, 'e', 'a', 2) as mail from %s" % (bucket.name)
            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"],
                                "mail" : doc["email"].replace('e', 'a', 2)}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_replace(self):
        for bucket in self.buckets:
            self.query = "select name, REPLACE(email, 'a', 'e', 1) as mail from %s" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"],
                                "mail" : doc["email"].replace('a', 'e', 1)}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_repeat(self):
        for bucket in self.buckets:
            self.query = "select REPEAT(name, 2) as name from %s" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"] * 2}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

##############################################################################################
#
#   LET
##############################################################################################

    def test_let_nums(self):
        for bucket in self.buckets:
            self.query = "select test_r, test_r > 2 compare from %s let test_r = (test_rate / 2)" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"test_r" : doc["test_rate"] / 2,
                                "compare" : (doc["test_rate"] / 2) > 2}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_prepared_let_nums(self):
        for bucket in self.buckets:
            self.query = "select test_r, test_r > 2 compare from %s let test_r = (test_rate / 2)" % (bucket.name)
            self.prepared_common_body()

    def test_let_string(self):
        for bucket in self.buckets:
            self.query = "select name, join_date date from %s let join_date = tostr(join_yr) || '-' || tostr(join_mo)" % (bucket.name)

            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            expected_result = [{"name" : doc["name"],
                                "date" : '%s-%s' % (doc['join_yr'], doc['join_mo'])}
                               for doc in self.full_list]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_letting(self):
        for bucket in self.buckets:
            self.query = "SELECT join_mo, sum_test from %s WHERE join_mo>7 group by join_mo letting sum_test = sum(tasks_points.task1)" % (bucket.name)
            actual_list = self.run_cbq_query()
            actual_result = sorted(actual_list['results'])
            tmp_groups = set([doc['join_mo'] for doc in self.full_list if doc['join_mo']>7])
            expected_result = [{"join_mo" : group,
                              "sum_test" : sum([x["tasks_points"]["task1"] for x in self.full_list
                                               if x["join_mo"] == group])}
                               for group in tmp_groups]
            expected_result = sorted(expected_result)
            self._verify_results(actual_result, expected_result)

    def test_prepared_letting(self):
        for bucket in self.buckets:
            self.query = "SELECT join_mo, sum_test from %s WHERE join_mo>7 group by join_mo letting sum_test = sum(tasks_points.task1)" % (bucket.name)
            self.prepared_common_body()

##############################################################################################
#
#   COMMON FUNCTIONS
##############################################################################################

    def negative_common_body(self, queries_errors={}):
        if not queries_errors:
            self.fail("No queries to run!")
        for bucket in self.buckets:
            for query, value in queries_errors.iteritems():
                error, code = value
                try:
                    actual_result = self.run_cbq_query(query.format(bucket.name))
                except CBQError as ex:
                    self.log.error(ex)
                    self.assertTrue(str(ex).find(error) != -1,
                                    "Error is incorrect.Actual %s.\n Expected: %s.\n" %(
                                                                str(ex).split(':')[-1], error))
                    self.assertTrue(str(ex).find(str(code)) != -1,
                                    "Error code is incorrect.Actual %s.\n Expected: %s.\n" %(
                                                                str(ex), code))
                else:
                    self.fail("There was no errors. Error expected: %s" % error)

    def prepared_common_body(self):
        result_no_prepare = self.run_cbq_query()['results']
        query = "PREPARE %s" % self.query
        prepared = self.run_cbq_query(query=query)['results'][0]
        result_with_prepare = self.run_cbq_query(query=prepared, is_prepared=True)['results']
        msg = "Query resut with prepare and without doesn't match.\nNo prepare: %s ... %s\nWith prepare: %s ... %s"
        self.assertTrue(sorted(result_no_prepare) == sorted(result_with_prepare),
                          msg % (result_no_prepare[:100],result_no_prepare[-100:],
                                 result_with_prepare[:100],result_with_prepare[-100:]))

    def run_cbq_query(self, query=None, min_output_size=10, server=None, query_params={}, is_prepared=False):
        if query is None:
            query = self.query
        if server is None:
           server = self.master
           if self.input.tuq_client and "client" in self.input.tuq_client:
               server = self.tuq_client
        cred_params = {'creds': []}
        for bucket in self.buckets:
            if bucket.saslPassword:
                cred_params['creds'].append({'user': 'local:%s' % bucket.name, 'pass': bucket.saslPassword})
        query_params.update(cred_params)
        if self.use_rest:
            query_params.update({'scan_consistency': self.scan_consistency})
            if hasattr(self, 'query_params') and self.query_params:
                query_params = self.query_params
            if self.hint_index and (query.lower().find('select') != -1):
                from_clause = re.sub(r'let.*', '', re.sub(r'.*from', '', re.sub(r'where.*', '', query)))
                from_clause = re.sub(r'LET.*', '', re.sub(r'.*FROM', '', re.sub(r'WHERE.*', '', from_clause)))
                from_clause = re.sub(r'select.*', '', re.sub(r'order by.*', '', re.sub(r'group by.*', '', from_clause)))
                from_clause = re.sub(r'SELECT.*', '', re.sub(r'ORDER BY.*', '', re.sub(r'GROUP BY.*', '', from_clause)))
                hint = ' USE INDEX (%s using %s) ' % (self.hint_index, self.index_type)
                query = query.replace(from_clause, from_clause + hint)
            self.log.info('RUN QUERY %s' % query)
            result = RestConnection(server).query_tool(query, self.n1ql_port, query_params=query_params, is_prepared=is_prepared)
        else:
            if self.version == "git_repo":
                output = self.shell.execute_commands_inside("$GOPATH/src/github.com/couchbase/query/" +\
                                                            "shell/cbq/cbq ",
                                                       subcommands=[query,],
                                                       min_output_size=20,
                                                       end_msg='cbq>')
            else:
                output = self.shell.execute_commands_inside("/tmp/tuq/cbq -engine=http://%s:%s/" % (server.ip, str(self.n1ql_port)),
                                                           subcommands=[query,],
                                                           min_output_size=20,
                                                           end_msg='cbq>')
            result = self._parse_query_output(output)
        if isinstance(result, str) or 'errors' in result:
            raise CBQError(result, server.ip)
        if 'metrics' in result:
            self.log.info("TOTAL ELAPSED TIME: %s" % result["metrics"]["elapsedTime"])
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

    def _start_command_line_query(self, server, options='', user=None, password=None):
        auth_row = None
        out = ''
        if user and password:
            auth_row = '%s:%s@' % (user, password)
        os = self.shell.extract_remote_info().type.lower()
        if self.flat_json:
            if os == 'windows':
                gopath = testconstants.WINDOWS_GOPATH
                cmd = "cd %s/src/github.com/couchbase/query/server/cbq-engine; " % (gopath) +\
                "./cbq-engine.exe -datastore=dir:%sdata >/dev/null 2>&1 &" % (self.directory_flat_json)
            else:
                gopath = testconstants.LINUX_GOPATH
                cmd = "cd %s/src/github.com/couchbase/query/server/cbq-engine; " % (gopath) +\
                "./cbq-engine -datastore=dir:%s/data >n1ql.log 2>&1 &" %(self.directory_flat_json)
            out = self.shell.execute_command(cmd)
            self.log.info(out)
        elif self.version == "git_repo":
            if os != 'windows':
                gopath = testconstants.LINUX_GOPATH
            else:
                gopath = testconstants.WINDOWS_GOPATH
            if self.input.tuq_client and "gopath" in self.input.tuq_client:
                gopath = self.input.tuq_client["gopath"]
            if os == 'windows':
                cmd = "cd %s/src/github.com/couchbase/query/server/cbq-engine; " % (gopath) +\
                "./cbq-engine.exe -datastore http://%s%s:%s/ %s >/dev/null 2>&1 &" %(
                                                                ('', auth_row)[auth_row is not None], server.ip, server.port, options)
            else:
                cmd = "cd %s/src/github.com/couchbase/query/server/cbq-engine; " % (gopath) +\
                "./cbq-engine -datastore http://%s%s:%s/ %s >n1ql.log 2>&1 &" %(
                                                                ('', auth_row)[auth_row is not None], server.ip, server.port, options)
            out = self.shell.execute_command(cmd)
        elif self.version == "sherlock":
            if self.services_init and self.services_init.find('n1ql') != -1:
                return
            if self.master.services and self.master.services.find('n1ql') !=-1:
                return
            if os == 'windows':
                couchbase_path = testconstants.WIN_COUCHBASE_BIN_PATH
                cmd = "cd %s; " % (couchbase_path) +\
                "./cbq-engine.exe -datastore http://%s%s:%s/ %s >/dev/null 2>&1 &" %(
                                                                ('', auth_row)[auth_row is not None], server.ip, server.port, options)
            else:
                couchbase_path = testconstants.LINUX_COUCHBASE_BIN_PATH
                cmd = "cd %s; " % (couchbase_path) +\
                "./cbq-engine -datastore http://%s%s:%s/ %s >n1ql.log 2>&1 &" %(
                                                                ('', auth_row)[auth_row is not None], server.ip, server.port, options)
            out = self.shell.execute_command(cmd)
            self.log.info(out)
        else:
            if os != 'windows':
                cmd = "cd /tmp/tuq;./cbq-engine -couchbase http://%s:%s/ >/dev/null 2>&1 &" %(
                                                                server.ip, server.port)
            else:
                cmd = "cd /cygdrive/c/tuq;./cbq-engine.exe -couchbase http://%s:%s/ >/dev/null 2>&1 &" %(
                                                                server.ip, server.port)
            self.shell.execute_command(cmd)
        return out

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
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee( docs_per_day, start)

    def _verify_results(self, actual_result, expected_result):
        if len(actual_result) != len(expected_result):
            missing, extra = self.check_missing_and_extra(actual_result, expected_result)
            self.log.error("Missing items: %s.\n Extra items: %s" % (missing[:100], extra[:100]))
            self.fail("Results are incorrect.Actual num %s. Expected num: %s.\n" % (
                                            len(actual_result), len(expected_result)))
        if self.max_verify is not None:
            actual_result = actual_result[:self.max_verify]
            expected_result = expected_result[:self.max_verify]

        msg = "Results are incorrect.\n Actual first and last 100:  %s.\n ... \n %s" +\
        "Expected first and last 100: %s.\n  ... \n %s"
        self.assertTrue(actual_result == expected_result,
                          msg % (actual_result[:100],actual_result[-100:],
                                 expected_result[:100],expected_result[-100:]))

    def check_missing_and_extra(self, actual, expected):
        missing = []
        extra = []
        for item in actual:
            if not (item in expected):
                 extra.append(item)
        for item in expected:
            if not (item in actual):
                missing.append(item)
        return missing, extra

    def sort_nested_list(self, result, key=None):
        actual_result = []
        for item in result:
            curr_item = {}
            for key, value in item.iteritems():
                if isinstance(value, list) or isinstance(value, set):
                    if not isinstance(value, set) and key and isinstance(value[0], dict) and key in value[0]:
                        curr_item[key] = sorted(value, key=lambda doc: (doc['task_name']))
                    else:
                        curr_item[key] = sorted(value)
                else:
                    curr_item[key] = value
            actual_result.append(curr_item)
        return actual_result

    def configure_gomaxprocs(self):
        max_proc = self.input.param("gomaxprocs", None)
        cmd = "export GOMAXPROCS=%s" % max_proc
        for server in self.servers:
            shell_connection = RemoteMachineShellConnection(self.master)
            shell_connection.execute_command(cmd)

    def load_directory(self, generators_load):
        gens_load = []
        for generator_load in generators_load:
            gens_load.append(copy.deepcopy(generator_load))
        items = 0
        for gen_load in gens_load:
            items += (gen_load.end - gen_load.start)
        shell = RemoteMachineShellConnection(self.master)
        for bucket in self.buckets:
            try:
                self.log.info("Delete directory's content %s/data/default/%s ..." % (self.directory_flat_json, bucket.name))
                o = shell.execute_command('rm -rf %sdata/default/*' % self.directory_flat_json)
                self.log.info("Create directory %s/data/default/%s..." % (self.directory_flat_json, bucket.name))
                o = shell.execute_command('mkdir -p %sdata/default/%s' % (self.directory_flat_json, bucket.name))
                self.log.info("Load %s documents to %sdata/default/%s..." % (items, self.directory_flat_json, bucket.name))
                for gen_load in gens_load:
                    for i in xrange(gen_load.end):
                        key, value = gen_load.next()
                        out = shell.execute_command("echo '%s' > %sdata/default/%s/%s.json" % (value, self.directory_flat_json,
                                                                                                bucket.name, key))
                self.log.info("LOAD IS FINISHED")
            finally:
                shell.disconnect()

    def create_primary_index_for_3_0_and_greater(self):
        if self.skip_index:
            self.log.info("Not creating index")
            return
        if self.flat_json:
                    return
        self.sleep(30, 'Sleep for some time prior to index creation')
        self.log.info("CREATE PRIMARY INDEX")
        rest = RestConnection(self.master)
        versions = rest.get_nodes_versions()
        if versions[0].startswith("4") or versions[0].startswith("3"):
            for bucket in self.buckets:
                if self.primary_indx_drop:
                    self.log.info("Dropping primary index for %s ..." % bucket.name)
                    self.query = "DROP PRIMARY INDEX ON %s" % (bucket.name)
                    self.sleep(3, 'Sleep for some time after index drop')
                self.log.info("Creating primary index for %s ..." % bucket.name)
                self.query = "CREATE PRIMARY INDEX ON %s USING %s" % (bucket.name, self.primary_indx_type)
                if self.primary_indx_type.lower() == 'view':
                    self.query = "CREATE PRIMARY INDEX ON %s" % (bucket.name)
                try:
                    self.run_cbq_query()
                    if self.primary_indx_type.lower() == 'gsi':
                        self._wait_for_index_online(bucket, '#primary')
                except Exception, ex:
                    self.log.info(str(ex))

    def _wait_for_index_online(self, bucket, index_name, timeout=300):
        end_time = time.time() + timeout
        while time.time() < end_time:
            query = "SELECT * FROM system:indexes"
            res = self.run_cbq_query(query)
            for item in res['results']:
                if 'keyspace_id' not in item['indexes']:
                    self.log.error(item)
                    continue
                if item['indexes']['keyspace_id'] == bucket.name and item['indexes']['name'] == index_name:
                    return
            self.sleep(5, 'index is pending or not in the list. sleeping...')
        raise Exception('index %s is not online. last response is %s' % (index_name, res))

