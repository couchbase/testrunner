import math
import re
import uuid

from tuq import QueryTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from membase.api.exception import CBQError

class QueriesIndexTests(QueryTests):

    FIELDS_TO_INDEX = [('name', 'job_title'), ('name', 'join_yr'), ('VMs', 'name')]
    COMPLEX_FIELDS_TO_INDEX = ['VMs', 'tasks_points', 'skills']

    def setUp(self):
        super(QueriesIndexTests, self).setUp()
        self.num_indexes = self.input.param('num_indexes', 1)
        if self.num_indexes > len(self.FIELDS_TO_INDEX):
            self.input.test_params["stop-on-failure"] = True
            self.log.error("MAX NUMBER OF INDEXES IS 3. ALL TESTS WILL BE SKIPPED")
            self.fail('MAX NUMBER OF INDEXES IS 3. ALL TESTS WILL BE SKIPPED')

    def suite_setUp(self):
        super(QueriesIndexTests, self).suite_setUp()

    def tearDown(self):
        super(QueriesIndexTests, self).tearDown()

    def suite_tearDown(self):
        super(QueriesIndexTests, self).suite_tearDown()

    def test_simple_create_delete_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    view_name = "my_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s) USING GSI" % (
                                            view_name, bucket.name, ','.join(self.FIELDS_TO_INDEX[ind - 1]))
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, view_name)
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(view_name)
                    self.assertTrue(self._is_index_in_list(bucket, view_name), "Index is not in list")
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name, view_name)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                    self.assertFalse(self._is_index_in_list(bucket, view_name), "Index is in list")

    def test_create_delete_index_with_query(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    view_name = "tuq_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(%s)  USING GSI" % (view_name, bucket.name, ','.join(self.FIELDS_TO_INDEX[ind - 1]))
                    actual_result = self.run_cbq_query()
                    self._wait_for_index_online(bucket, view_name)
                    self._verify_results(actual_result['results'], [])
                    created_indexes.append(view_name)
                    self.query = "select %s from %s where %s is not null and %s is not null" % (','.join(self.FIELDS_TO_INDEX[ind - 1]), bucket.name,
                                                                                                self.FIELDS_TO_INDEX[ind - 1][0], self.FIELDS_TO_INDEX[ind - 1][1])
                    actual_result = self.run_cbq_query()
                    self.assertTrue(len(actual_result['results']), self.num_items)
            except Exception, ex:
                content = self.cluster.query_view(self.master, "ddl_%s" % view_name, view_name, {"stale" : "ok"},
                                                  bucket="default", retry_time=1)
                self.log.info("Generated view has %s items" % len(content['rows']))
                raise ex
            finally:
                for view_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name, view_name)
                    actual_result = self.run_cbq_query()
                    self._verify_results(actual_result['results'], [])
                self.query = "select %s from %s where %s is not null and %s is not null" % (','.join(self.FIELDS_TO_INDEX[ind - 1]), bucket.name,
                                                                                                self.FIELDS_TO_INDEX[ind - 1][0], self.FIELDS_TO_INDEX[ind - 1][1])
                actual_result = self.run_cbq_query(query_params={'scan_consistency' : 'statement_plus'})
                self.assertTrue(len(actual_result['results']), self.num_items)

    def test_explain_query_count(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs, name)  USING GSI" % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(VMs) FROM %s where VMs is not null union SELECT count(name) FROM %s where name is not null' % (bucket.name, bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]['~children'][0]["children"][0]['~children'][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_query_group_by(self):
        for bucket in self.buckets:
            index_name = "my_index_child"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs, join_yr)  USING GSI" % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT count(*) FROM %s GROUP BY VMs, join_yr' % (bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_query_meta(self):
        for bucket in self.buckets:
            index_name = "my_index_meta"
            try:
                self.query = "CREATE INDEX %s ON %s(meta(%s).type, name)  USING GSI" % (index_name, bucket.name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT name FROM %s WHERE meta(%s).type = "json" and name is not null' % (bucket.name, bucket.name)
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                self.run_cbq_query()

    def test_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindex%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name, join_day)  USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT name, join_day FROM %s where name = 'employee-9'"% (bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                    self.query = "SELECT name, join_day FROM %s where name = 'employee-9'"  % (bucket.name)
                    actual_result = self.run_cbq_query()
                    self.log.info(actual_result)
                    self.query = "SELECT name, join_day FROM %s where name = 'employee-9'"  % (bucket.name)
                    actual_result = self.run_cbq_query()
                    self.log.info(actual_result)
                    expected_result = [{"name" : doc["name"],"join_day" : doc["join_day"]}
                               for doc in self.full_list
                               if doc['name'] == 'employee-9']
                    expected_result = sorted(expected_result, key=lambda doc: (doc['name']))
                    self._verify_results(actual_result['results'], expected_result)
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING  %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_covering_partial_index(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithwhere%s" % ind
                    self.query = "CREATE INDEX %s ON %s(email, VMs, join_day) where join_day > 10 USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain select email,VMs[0].RAM from %s where email "  % (bucket.name) +\
                                 "LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                    self.query = "select email,join_day from %s where email "  % (bucket.name) +\
                                 "LIKE '%@%.%' and VMs[0].RAM > 5 and join_day > 10"
                    actual_result = self.run_cbq_query()
                    self.log.info(actual_result)
                    expected_result = [{"join_day":doc["join_day"],"email" : doc["email"]}
                               for doc in self.full_list
                               if re.match(r'.*@.*\..*', doc['email']) and \
                                  doc['join_day'] > 10 and \
                                  len([vm for vm in doc["VMs"]
                                        if vm["RAM"] > 5]) > 0]
                    expected_result = sorted(expected_result, key=lambda doc: (doc["join_day"]))
                    self._verify_results(actual_result['results'], expected_result)
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_covering_orderby_limit(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithlimit%s" % ind
                    self.query = "CREATE INDEX %s ON %s(skills[0], join_yr, VMs[0].os) where join_yr =2010 USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain select * from %s where skills[0]='skill2010' and join_yr=2010 and VMs[0].os IN ['ubuntu','windows','linux'] order by _id asc LIMIT 10 OFFSET 0;" % (bucket.name)
                    self.test_explain_union(index_name)
                    self.query = "select name,skills[0] as skills from %s where skills[0]='skill2010' and join_yr=2010 and VMs[0].os IN ['ubuntu','windows','linux'] order by name LIMIT 15 OFFSET 0;" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    expected_result = [{"skills" : doc["skills"][0],"name":doc["name"]}
                                       for doc in self.full_list
                                       if doc['join_yr']==2010 and \
                                          doc['skills'][0]=='skill2010' and \
                                          len([vm for vm in doc["VMs"]
                                                if vm["os"] == 'ubuntu' or vm["os"] == 'windows' or vm["os"] == "linux"])>0]
                    actual_result = actual_result['results']
                    self.log.info(actual_result)
                    expected_result= sorted(expected_result,key=lambda doc: (doc["name"]))[:15]
                    self.log.info(expected_result)
                    self.max_verify = 15
                    self._verify_results(actual_result, expected_result)
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                    self.run_cbq_query()

    def test_covering_groupby(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithgroupby%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_yr,name) where join_yr >2009 USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain SELECT count(*),join_yr FROM %s AS test where join_yr > 2009 GROUP BY join_yr ORDER BY name';" % (bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                    self.query = "SELECT count(*),join_yr FROM %s AS test where join_yr > 2009 GROUP BY join_yr ORDER BY name;" % (bucket.name)
                    actual_result = self.run_cbq_query()
                    actual_result = actual_result['results']
                    expected_result = [{"join_yr" : doc["join_yr"]}
                                    for doc in self.full_list
                                    if doc["join_yr"] > 2009]
                    expected_result = [dict(y) for y in set(tuple(x.items()) for x in expected_result)]
                    if len(actual_result) != len(expected_result):
                         missing, extra = self.check_missing_and_extra(actual_result, expected_result)
                         self.log.error("Missing items: %s.\n Extra items: %s" % (missing[:100], extra[:100]))
                         self.fail("Results are incorrect.Actual num %s. Expected num: %s.\n" % (len(actual_result), len(expected_result)))
            finally:
                    for index_name in created_indexes:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                        self.run_cbq_query()

    def test_covering_groupby_having(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithgroupbyhaving%s" % ind
                    self.query = "CREATE INDEX %s ON %s(job_title,join_mo,test_rate) USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain SELECT join_mo, SUM(test_rate) as rate FROM %s " % (bucket.name) +\
                         "as employees WHERE job_title='Engineer' GROUP BY join_mo " +\
                         "HAVING SUM(employees.test_rate) > 0 and " +\
                         "SUM(test_rate) < 100000"
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                    self.query = "SELECT join_mo, SUM(test_rate) as rate FROM %s " % (bucket.name) +\
                         "as employees WHERE job_title='Engineer' GROUP BY join_mo " +\
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
                                                             doc['job_title'] == 'Engineer']))}
                               for group in tmp_groups
                               if math.fsum([doc['test_rate']
                                            for doc in self.full_list
                                            if doc['join_mo'] == group and\
                                            doc['job_title'] == 'Engineer'] ) > 0 and\
                                  math.fsum([doc['test_rate']
                                            for doc in self.full_list
                                            if doc['join_mo'] == group and\
                                            doc['job_title'] == 'Engineer'] ) < 100000]
                    expected_result = sorted(expected_result, key=lambda doc: (doc['join_mo']))
                    self._verify_results(actual_result, expected_result)
            finally:
                    for index_name in created_indexes:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                        self.run_cbq_query()

    def test_covering_groupby_letting(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithgroupbyletting%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_mo,test_rate) USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "explain SELECT join_mo, sum_test from %s WHERE join_mo>7 group by join_mo letting sum_test = sum(test_rate) " % (bucket.name)
                    if self.covering_index:
                        self.test_explain_covering_index(index_name)
                    self.query = "SELECT join_mo, sum_test from %s WHERE join_mo>7 group by join_mo letting sum_test = sum(test_rate)" % (bucket.name)
                    actual_list = self.run_cbq_query()
                    actual_result = sorted(actual_list['results'])
                    tmp_groups = set([doc['join_mo'] for doc in self.full_list if doc['join_mo']>7])
                    expected_result = [{"join_mo" : group,
                              "sum_test" : sum([x["test_rate"] for x in self.full_list
                                               if x["join_mo"] == group])}
                               for group in tmp_groups]
                    expected_result = sorted(expected_result)
                    self._verify_results(actual_result, expected_result)
            finally:
                    for index_name in created_indexes:
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                        self.run_cbq_query()

    def test_covering_index_hints_select_explain(self):
        index_name_prefix_list = ['hint' + str(uuid.uuid4())[:4],'covering_hint' + str(uuid.uuid4())[:4]]
        created_indexes = []
        for bucket in self.buckets:
            for ind in index_name_prefix_list:
                    for attr in ['join_day', 'join_mo']:
                        ind_name = '%s_%s' % (ind, attr)
                        self.query = "CREATE INDEX %s ON %s(%s)  USING %s" % (ind_name,
                                                                    bucket.name, attr, self.index_type)
                        self.run_cbq_query()
                        self._wait_for_index_online(bucket, ind_name)
                        created_indexes.append('%s' % (ind_name))
        for bucket in self.buckets:
                try:
                    for ind in created_indexes:
                        if "covering" in ind:
                                if "join_day" in ind:
                                    self.query = 'EXPLAIN SELECT join_day FROM %s  USE INDEX(%s using %s) WHERE join_day>2' % (bucket.name, ind, self.index_type)
                                    self.test_explain_covering_index(ind)
                                elif "join_mo" in ind:
                                    self.query = 'EXPLAIN SELECT join_mo FROM %s  USE INDEX(%s using %s) WHERE join_mo>3' % (bucket.name, ind, self.index_type)
                                    self.test_explain_covering_index(ind)
                        else:
                            self.query = 'EXPLAIN SELECT name,join_day, join_mo FROM %s  USE INDEX(%s using %s) WHERE join_day>2 AND join_mo>3' % (bucket.name, ind, self.index_type)
                            res = self.run_cbq_query()
                            self.log.info(res)
                            self.assertTrue(res["results"][0]["~children"][0]["index"] == ind,
                                    "Index should be %s, but is: %s" % (ind, res["results"][0]["~children"][0]["index"]))

                finally:
                     for index_name in set(created_indexes):
                        self.query = "DROP INDEX %s.%s USING %s" % (bucket.name, index_name,self.index_type)
                        self.run_cbq_query()

    def async_monitor_index(self, bucket, index_name = None):
        monitor_index_task = self.cluster.async_monitor_index(
                 server = self.n1ql_server, bucket = bucket,
                 n1ql_helper = self.n1ql_helper,
                 index_name = index_name)
        return monitor_index_task

##############################################################################################
#
#   SCALAR FN
##############################################################################################

    def test_ceil_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithceil%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)


            self.query = "explain select name,ceil(test_rate) as rate from %s where name LIKE '%s' and ceil(test_rate) > 5"  % (bucket.name,"employee")
            if self.covering_index:
                self.test_explain_covering_index(index_name)
            self.query = "select test_rate from %s where name = 'employee-9' and ceil(test_rate) > 5"  % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['test_rate']))
            expected_result = [{"test_rate" : doc['test_rate']} for doc in self.full_list
                               if doc['name'] == 'employee-9' and
                               math.ceil(doc['test_rate']) > 5]
            expected_result = sorted(expected_result, key=lambda doc: (doc['test_rate']))
            self._verify_results(actual_result, expected_result)

    def test_floor_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithfloor%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name,test_rate)  USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)

            self.query = "explain select name,floor(test_rate) from %s where name LIKE '%s' and floor(test_rate) > 5"  % (bucket.name,"employee")
            if self.covering_index:
                self.test_explain_covering_index(index_name)

            self.query = "select test_rate from %s where name = 'employee-9' and floor(test_rate) > 5"  % (bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'], key=lambda doc: (doc['test_rate']))
            expected_result = [{"test_rate" : doc['test_rate']} for doc in self.full_list
                               if doc['name'] == 'employee-9' and
                               math.floor(doc['test_rate']) > 5]
            expected_result = sorted(expected_result, key=lambda doc: (doc['test_rate']))
            self._verify_results(actual_result, expected_result)

    def test_greatest_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithgreatest%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name,skills[0], skills[1])  USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    created_indexes.append(index_name)
        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)

            self.query = "explain select GREATEST(skills[0], skills[1]) as SKILL from %s where name LIKE '%s' and skills[0]='skill2010'"  % (bucket.name,"employee")
            if self.covering_index:
                self.test_explain_covering_index(index_name)

            self.query = "select GREATEST(skills[0], skills[1]) as SKILL from %s where name = 'employee-9' and skills[0]='skill2010'"  % (
                                                                                bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['SKILL']))

            expected_result = [{"SKILL" :
                                (doc['skills'][0], doc['skills'][1])[doc['skills'][0]<doc['skills'][1]]}
                               for doc in self.full_list
                               if doc['skills'][0]=='skill2010' and doc['name']=='employee-9']
            expected_result = sorted(expected_result, key=lambda doc: (doc['SKILL']))
            self._verify_results(actual_result, expected_result)

    def test_least_covering_index(self):
        for bucket in self.buckets:
            created_indexes = []
            for ind in xrange(self.num_indexes):
                    index_name = "coveringindexwithleast%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name,skills[0], skills[1])  USING %s" % (index_name, bucket.name,self.index_type)
                    self.run_cbq_query()
                    created_indexes.append(index_name)

        for bucket in self.buckets:
            tasks=[]
            for index_name in created_indexes:
                try:
                    tasks.append(self.async_monitor_index(bucket = bucket.name, index_name = index_name))
                    for task in tasks:
                        task.result()
                except Exception, ex:
                    self.log.info(ex)

            self.query = "explain select LEAST(skills[0], skills[1]) as SKILL from %s where name LIKE '%s' and skills[0]='skill2010'"  % (bucket.name,"employee")
            if self.covering_index:
                self.test_explain_covering_index(index_name)
            self.query = "select   LEAST(skills[0], skills[1]) as SKILL from %s where name = 'employee-9' and skills[0]='skill2010'"  % (
                                                                            bucket.name)
            actual_result = self.run_cbq_query()
            actual_result = sorted(actual_result['results'],
                                   key=lambda doc: (doc['SKILL']))

            expected_result = [{"SKILL" :
                                (doc['skills'][0], doc['skills'][1])[doc['skills'][0]>doc['skills'][1]]}
                               for doc in self.full_list
                               if doc['skills'][0]=='skill2010' and doc['name']=='employee-9']
            expected_result = sorted(expected_result, key=lambda doc: (doc['SKILL']))
            self._verify_results(actual_result, expected_result)




    def test_explain_index_join(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(name, project)  USING GSI" % (index_name, bucket.name)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN SELECT employee.name, new_task.project FROM %s as employee JOIN %s as new_task USE KEYS ['key1']" % (bucket.name, bucket.name)
                    res = self.run_cbq_query()
                    self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                    self.run_cbq_query()

    def test_explain_index_subquery(self):
        for bucket in self.buckets:
            created_indexes = []
            try:
                for ind in xrange(self.num_indexes):
                    index_name = "join_index%s" % ind
                    self.query = "CREATE INDEX %s ON %s(join_day, name)  USING GSI" % (index_name, bucket.name)
                    self.run_cbq_query()
                    self._wait_for_index_online(bucket, index_name)
                    created_indexes.append(index_name)
                    self.query = "EXPLAIN select task_name, (select sum(test_rate) cn from %s use keys ['query-1'] where join_day>2 and name =='abc') as names from %s" % (bucket.name, bucket.name)
                    res = self.run_cbq_query()
                    self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                    "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                for index_name in created_indexes:
                    self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                    self.run_cbq_query()

    def test_explain_childs_list_objects(self):
        for bucket in self.buckets:
            index_name = "my_index_child2"
            try:
                self.query = "CREATE INDEX %s ON %s(VMs[0].RAM, VMS[1].RAM)  USING GSI" % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT VMs FROM %s ' % (bucket.name) + \
                        'WHERE ANY vm IN VMs SATISFIES vm.RAM > 5 AND vm.os = "ubuntu" end'
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_childs_objects(self):
        for bucket in self.buckets:
            index_name = "my_index_obj"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points, join_mo)  USING GSI" % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % (bucket.name) + \
                             'WHERE join_mo>7 and task_points > 0'
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def test_explain_childs_objects_element(self):
        for bucket in self.buckets:
            index_name = "my_index_obj_el"
            try:
                self.query = "CREATE INDEX %s ON %s(tasks_points.task1, join_mo)  USING GSI" % (index_name, bucket.name)
                self.run_cbq_query()
                self._wait_for_index_online(bucket, index_name)
                self.query = 'EXPLAIN SELECT tasks_points.task1 AS task from %s ' % (bucket.name) + \
                             'WHERE join_mo>7 and  task_points.task1 > 0'
                res = self.run_cbq_query()
                self.assertTrue(res["results"][0]["~children"][0]["index"] == index_name,
                                "Index should be %s, but is: %s" % (index_name, res["results"]))
            finally:
                self.query = "DROP INDEX %s.%s" % (bucket.name, index_name)
                try:
                    self.run_cbq_query()
                except:
                    pass

    def _is_index_in_list(self, bucket, index_name):
        query = "SELECT * FROM system:indexes"
        res = self.run_cbq_query(query)
        for item in res['results']:
            if 'keyspace_id' not in item['indexes']:
                self.log.error(item)
                continue
            if item['indexes']['keyspace_id'] == bucket.name and item['indexes']['name'] == index_name:
                return True
        return False