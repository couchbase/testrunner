import logging
import threading
import logger
import json
import uuid
import copy
import math
import re
import os

import testconstants
import datetime
import time
from datetime import date
from couchbase_helper.tuq_generators import TuqGenerators
from couchbase_helper.tuq_generators import JsonGenerator
from remote.remote_util import RemoteMachineShellConnection
from basetestcase import BaseTestCase
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.api.exception import CBQError, ReadDocumentException
from membase.api.rest_client import RestConnection
from memcached.helper.data_helper import MemcachedClientHelper

class QueryTests(BaseTestCase):
    def setUp(self):
        if not self._testMethodName == 'suite_setUp':
            self.skip_buckets_handle = True
        super(QueryTests, self).setUp()
        self.version = self.input.param("cbq_version", "sherlock")
        if self.input.tuq_client and "client" in self.input.tuq_client:
            self.shell = RemoteMachineShellConnection(self.input.tuq_client["client"])
        else:
            self.shell = RemoteMachineShellConnection(self.master)
        if not self._testMethodName == 'suite_setUp' and self.input.param("cbq_version", "sherlock") != 'sherlock':
            self._start_command_line_query(self.master)
        self.use_rest = self.input.param("use_rest", True)
        self.max_verify = self.input.param("max_verify", None)
        self.buckets = RestConnection(self.master).get_buckets()
        self.docs_per_day = self.input.param("doc-per-day", 49)
        self.item_flag = self.input.param("item_flag", 4042322160)
        self.n1ql_port = self.input.param("n1ql_port", 8093)
        self.analytics = self.input.param("analytics",False)
        self.dataset = self.input.param("dataset", "default")
        self.primary_indx_type = self.input.param("primary_indx_type", 'GSI')
        self.index_type = self.input.param("index_type", 'GSI')
        self.primary_indx_drop = self.input.param("primary_indx_drop", False)
        self.monitoring = self.input.param("monitoring",False)
        self.isprepared = False
        self.named_prepare = self.input.param("named_prepare", None)
        self.skip_primary_index = self.input.param("skip_primary_index",False)
        self.scan_consistency = self.input.param("scan_consistency", 'REQUEST_PLUS')
        self.cbas_node = self.input.cbas
        shell = RemoteMachineShellConnection(self.master)
        type = shell.extract_remote_info().distribution_type
        self.path = testconstants.LINUX_COUCHBASE_BIN_PATH
        if type.lower() == 'windows':
            self.path = testconstants.WIN_COUCHBASE_BIN_PATH
        elif type.lower() == "mac":
            self.path = testconstants.MAC_COUCHBASE_BIN_PATH
        self.threadFailure = False
        if self.primary_indx_type.lower() == "gsi":
            self.gsi_type = self.input.param("gsi_type", None)
        else:
            self.gsi_type = None
        if self.input.param("reload_data", False):
            for bucket in self.buckets:
                self.cluster.bucket_flush(self.master, bucket=bucket,
                                          timeout=self.wait_timeout * 5)
            self.gens_load = self.generate_docs(self.docs_per_day)
            self.load(self.gens_load, flag=self.item_flag)
        self.gens_load = self.generate_docs(self.docs_per_day)
        if self.input.param("gomaxprocs", None):
            self.configure_gomaxprocs()
        self.gen_results = TuqGenerators(self.log, self.generate_full_docs_list(self.gens_load))
        if (self.analytics == False):
                self.create_primary_index_for_3_0_and_greater()
        if (self.analytics):
            self.setup_analytics()
            self.sleep(30,'wait for analytics setup')

    def suite_setUp(self):
        try:
            self.load(self.gens_load, flag=self.item_flag)
            if not self.input.param("skip_build_tuq", True):
                self._build_tuq(self.master)
            self.skip_buckets_handle = True
        except:
            self.log.error('SUITE SETUP FAILED')
            self.tearDown()

    def tearDown(self):
        if self._testMethodName == 'suite_tearDown':
            self.skip_buckets_handle = False
        if self.analytics:
            data = 'use Default ;' + "\n"
            for bucket in self.buckets:
                data += 'disconnect bucket {0} if connected;'.format(bucket.name) + "\n"
                data += 'drop dataset {0} if exists;'.format(bucket.name+ "_shadow") + "\n"
                data += 'drop bucket {0} if exists;'.format(bucket.name) + "\n"
            filename = "file.txt"
            f = open(filename,'w')
            f.write(data)
            f.close()
            url = 'http://{0}:8095/analytics/service'.format(self.cbas_node.ip)
            cmd = 'curl -s --data pretty=true --data-urlencode "statement@file.txt" ' + url
            os.system(cmd)
            os.remove(filename)
        super(QueryTests, self).tearDown()

    def suite_tearDown(self):
        if not self.input.param("skip_build_tuq", False):
            if hasattr(self, 'shell'):
                self.shell.execute_command("killall /tmp/tuq/cbq-engine")
                self.shell.execute_command("killall tuqtng")
                self.shell.disconnect()


    def setup_analytics(self):
        #data = ""
        # for bucket in self.buckets:
        #         data += 'disconnect bucket {0} ;'.format(bucket.name) + "\n"
        #         data += 'connect bucket {0};'.format(bucket.name) + "\n"
        # filename = "file.txt"
        # f = open(filename,'w')
        # f.write(data)
        # f.close()
        # url = 'http://{0}:8095/analytics/service'.format(self.master.ip)
        # cmd = 'curl -s --data pretty=true --data-urlencode "statement@file.txt" ' + url
        # os.system(cmd)
        # os.remove(filename)
        data = 'use Default;' + "\n"
        for bucket in self.buckets:
            data += 'create bucket {0} with {{"bucket":"{0}","nodes":"{1}"}} ;'.format(bucket.name,self.master.ip)  + "\n"
            data += 'create shadow dataset {1} on {0}; '.format(bucket.name,bucket.name+"_shadow") + "\n"
            data +=  'connect bucket {0} ;'.format(bucket.name) + "\n"
        filename = "file.txt"
        f = open(filename,'w')
        f.write(data)
        f.close()
        url = 'http://{0}:8095/analytics/service'.format(self.cbas_node.ip)
        cmd = 'curl -s --data pretty=true --data-urlencode "statement@file.txt" ' + url
        os.system(cmd)
        os.remove(filename)

    def run_active_requests(self, e, t):
        while not e.isSet():
            logging.debug('wait_for_event_timeout starting')
            event_is_set = e.wait(t)
            logging.debug('event set: %s', event_is_set)
            if event_is_set:
                result = self.run_cbq_query("select * from system:active_requests")
                print result
                self.assertTrue(result['metrics']['resultCount'] == 1)
                requestId = result['requestID']
                result = self.run_cbq_query(
                    'delete from system:active_requests where requestId  =  "%s"' % requestId)
                time.sleep(20)
                result = self.run_cbq_query(
                    'select * from system:active_requests  where requestId  =  "%s"' % requestId)
                self.assertTrue(result['metrics']['resultCount'] == 0)
                result = self.run_cbq_query("select * from system:completed_requests")
                print result
                requestId = result['requestID']
                result = self.run_cbq_query(
                    'delete from system:completed_requests where requestId  =  "%s"' % requestId)
                time.sleep(10)
                result = self.run_cbq_query(
                    'select * from system:completed_requests where requestId  =  "%s"' % requestId)
                print result
                self.assertTrue(result['metrics']['resultCount'] == 0)

##############################################################################################
#
#   SIMPLE CHECKS
##############################################################################################
    def test_simple_check(self):
        for bucket in self.buckets:
            if self.monitoring:
                e = threading.Event()
                t1 = threading.Thread(name='run_simple', target=self.run_active_requests,args=(e,2))
                t1.start()

            query = 'select * from %s' %(bucket.name)
            self.run_cbq_query(query)
            logging.debug("event is set")
            if self.monitoring:
                e.set()
                t1.join(100)
            query_template = 'FROM %s select $str0, $str1 ORDER BY $str0,$str1 ASC' % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_joins_monitoring(self):
        for bucket in self.buckets:
            e = threading.Event()
            if self.monitoring:
                e = threading.Event()
                t2 = threading.Thread(name='run_joins', target=self.run_active_requests,args=(e,2))
                t2.start()
            query = 'select * from %s b1 inner join %s b2 on keys b1.CurrencyCode inner join %s b3 on keys b1.CurrencyCode left outer join %s b4 on keys b1.CurrencyCode' % (bucket.name,bucket.name,bucket.name,bucket.name)
            actual_result = self.run_cbq_query(query)
            logging.debug("event is set")
            if self.monitoring:
                e.set()
                t2.join(100)

    def test_simple_negative_check(self):
        queries_errors = {'SELECT $str0 FROM {0} WHERE COUNT({0}.$str0)>3' :
                          'Aggregates not allowed in WHERE',
                          'SELECT *.$str0 FROM {0}' : 'syntax error',
                          'SELECT *.* FROM {0} ... ERROR' : 'syntax error',
                          'FROM %s SELECT $str0 WHERE id=null' : 'syntax error',}
        self.negative_common_body(queries_errors)

    def test_unnest(self):
        for bucket in self.buckets:
            query_template = 'SELECT emp.$int0, task FROM %s emp UNNEST emp.$nested_list_3l0 task' % bucket.name
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(sorted(actual_result['results']), sorted(expected_result))

    def test_subquery_select(self):
        for bucket in self.buckets:
            self.query = 'SELECT $str0, $subquery(SELECT COUNT($str0) cn FROM %s d USE KEYS $5) as names FROM %s' % (bucket.name,
                                                                                                                     bucket.name)
            actual_result, expected_result = self.run_query_with_subquery_select_from_template(self.query)
            self._verify_results(actual_result['results'], expected_result)

    def test_subquery_from(self):
        for bucket in self.buckets:
            self.query = 'SELECT tasks.$str0 FROM $subquery(SELECT $str0, $int0 FROM %s) as tasks' % (bucket.name)
            actual_result, expected_result = self.run_query_with_subquery_from_template(self.query)
            self._verify_results(actual_result['results'], expected_result)

    def test_consistent_simple_check(self):
        queries = [self.gen_results.generate_query('SELECT $str0, $int0, $int1 FROM %s ' +\
                    'WHERE $str0 IS NOT NULL AND $int0<10 ' +\
                    'OR $int1 = 6 ORDER BY $int0, $int1'),
                   self.gen_results.generate_query('SELECT $str0, $int0, $int1 FROM %s ' +\
                    'WHERE $int1 = 6 OR $str0 IS NOT NULL AND ' +\
                    '$int0<10 ORDER BY $int0, $int1')]
        for bucket in self.buckets:
            actual_result1 = self.run_cbq_query(queries[0] % bucket.name)
            actual_result2 = self.run_cbq_query(queries[1] % bucket.name)
            self.assertTrue(actual_result1['results'] == actual_result2['results'],
                              "Results are inconsistent.Difference: %s %s %s %s" %(
                                    len(actual_result1['results']), len(actual_result2['results']),
                                    actual_result1['results'][:100], actual_result2['results'][:100]))

    def test_simple_nulls(self):
        queries = ['SELECT id FROM %s WHERE id=NULL or id="null"']
        for bucket in self.buckets:
            if self.monitoring:
                e = threading.Event()
                t3 = threading.Thread(name='run_simple_nulls', target=self.run_active_requests,args=(e,2))
                t3.start()
            for query in queries:
                actual_result = self.run_cbq_query(query % (bucket.name))
                logging.debug("event is set")
                if self.monitoring:
                     e.set()
                     t3.join(100)
                self._verify_results(actual_result['results'], [])

##############################################################################################
#
#   LIMIT OFFSET CHECKS
##############################################################################################

    def test_limit_negative(self):
        #queries_errors = {'SELECT * FROM default LIMIT {0}' : ('Invalid LIMIT value 2.5', 5030)}
        queries_errors = {'SELECT ALL * FROM %s' : ('syntax error', 3000)}
        self.negative_common_body(queries_errors)

    def test_limit_offset(self):
        for bucket in self.buckets:
            if self.monitoring:
                e = threading.Event()
                t4 = threading.Thread(name='run_limit_offset', target=self.run_active_requests,args=(e,2))
                t4.start()
            query_template = 'SELECT DISTINCT $str0 FROM %s ORDER BY $str0 LIMIT 10' % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            if self.monitoring:
                e.set()
                t4.join(100)
            self._verify_results(actual_result['results'], expected_result)
            query_template = 'SELECT DISTINCT $str0 FROM %s ORDER BY $str0 LIMIT 10 OFFSET 10' % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)

    def test_limit_offset_zero(self):
        for bucket in self.buckets:
            query_template = 'SELECT DISTINCT $str0 FROM %s ORDER BY $str0 LIMIT 0' % (bucket.name)
            self.query = self.gen_results.generate_query(query_template)
            actual_result = self.run_cbq_query()
            self.assertEquals(actual_result['results'], [],
                              "Results are incorrect.Actual %s.\n Expected: %s.\n" % (
                                        actual_result['results'], []))
            query_template = 'SELECT DISTINCT $str0 FROM %s ORDER BY $str0 LIMIT 10 OFFSET 0' % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self.assertEquals(actual_result['results'], expected_result,
                              "Results are incorrect.Actual %s.\n Expected: %s.\n" % (
                                        actual_result['results'], expected_result))

    def test_limit_offset_negative_check(self):
        queries_errors = {'SELECT DISTINCT $str0 FROM {0} LIMIT 1.1' :
                          'Invalid LIMIT value 1.1',
                          'SELECT DISTINCT $str0 FROM {0} OFFSET 1.1' :
                          'Invalid OFFSET value 1.1'}
        self.negative_common_body(queries_errors)

    def test_limit_offset_sp_char_check(self):
        queries_errors = {'SELECT DISTINCT $str0 FROM {0} LIMIT ~' :
                          'syntax erro',
                          'SELECT DISTINCT $str0 FROM {0} OFFSET ~' :
                          'syntax erro'}
        self.negative_common_body(queries_errors)
##############################################################################################
#
#   ALIAS CHECKS
##############################################################################################

    def test_simple_alias(self):
        for bucket in self.buckets:
            if self.monitoring:
                e = threading.Event()
                t5 = threading.Thread(name='run_limit_offset', target=self.run_active_requests,args=(e,2))
                t5.start()
            query_template = 'SELECT COUNT($str0) AS COUNT_EMPLOYEE FROM %s' % (bucket.name)
            if self.analytics:
                query_template = 'SELECT COUNT(`$str0`) AS COUNT_EMPLOYEE FROM %s' % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self.assertEquals(actual_result['results'], expected_result,
                              "Results are incorrect.Actual %s.\n Expected: %s.\n" % (
                                        actual_result['results'], expected_result))

            query_template = 'SELECT COUNT(*) + 1 AS COUNT_EMPLOYEE FROM %s' % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            if self.monitoring:
                e.set()
                t5.join(100)
            expected_result = [ { "COUNT_EMPLOYEE": expected_result[0]['COUNT_EMPLOYEE'] + 1 } ]
            self.assertEquals(actual_result['results'], expected_result,
                              "Results are incorrect.Actual %s.\n Expected: %s.\n" % (
                                        actual_result['results'], expected_result))

    def test_simple_negative_alias(self):
        queries_errors = {'SELECT $str0._last_name as *' : 'syntax error',
                          'SELECT $str0._last_name as DATABASE ?' : 'syntax error',
                          'SELECT $str0 AS NULL FROM {0}' : 'syntax error',
                          'SELECT $str1 as $str0, $str0 FROM {0}' :
                                'Duplicate result alias name',
                          'SELECT test.$obj0 as points FROM {0} AS TEST ' +
                           'GROUP BY $obj0 AS GROUP_POINT' :
                                'syntax error'}
        self.negative_common_body(queries_errors)

    def test_alias_from_clause(self):
        queries_templates = ['SELECT $obj0.$_obj0_int0 AS points FROM %s AS test ORDER BY points',
                   'SELECT $obj0.$_obj0_int0 AS points FROM %s AS test WHERE test.$int0 >0'  +\
                   ' ORDER BY points',
                   'SELECT $obj0.$_obj0_int0 AS points FROM %s AS test ' +\
                   'GROUP BY test.$obj0.$_obj0_int0 ORDER BY points']
        # if self.analytics:
        #      queries_templates = ['SELECT test.$obj0.$_obj0_int0 AS points FROM %s AS test ORDER BY test.points',
        #            'SELECT test.$obj0.$_obj0_int0 AS points FROM %s AS test WHERE test.$int0 >0'  +\
        #            ' ORDER BY test.points',
        #            'SELECT test.$obj0.$_obj0_int0 AS points FROM %s AS test ' +\
        #            'GROUP BY test.$obj0.$_obj0_int0 ORDER BY test.points']
        for bucket in self.buckets:
            if self.monitoring:
                e = threading.Event()
                t6 = threading.Thread(name='run_limit_offset', target=self.run_active_requests,args=(e,2))
                t6.start()
            for query_template in queries_templates:
                actual_result, expected_result = self.run_query_from_template(query_template  % (bucket.name))
                if self.monitoring:
                    e.set()
                    t6.join(100)
                self._verify_results(actual_result['results'], expected_result)

    def test_alias_from_clause_group(self):
        for bucket in self.buckets:
            query_template = 'SELECT $obj0.$_obj0_int0 AS points FROM %s AS test ' %(bucket.name) +\
                         'GROUP BY $obj0.$_obj0_int0 ORDER BY points'
            actual_result, expected_result = self.run_query_from_template(query_template)
            print "actual results are {0}".format(actual_result['results'])
            print "expected results are {0}".format(expected_result)
            self._verify_results(actual_result['results'], expected_result)

    def test_alias_order_desc(self):
        for bucket in self.buckets:
            if self.monitoring:
                e = threading.Event()
                t7 = threading.Thread(name='run_limit_offset', target=self.run_active_requests,args=(e,2))
                t7.start()
            query_template = 'SELECT $str0 AS name_new FROM %s AS test ORDER BY name_new DESC' %(
                                                                                bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            if self.monitoring:
                    e.set()
                    t7.join(100)
            self._verify_results(actual_result['results'], expected_result)

    def test_alias_order_asc(self):
        for bucket in self.buckets:
            query_template = 'SELECT $str0 AS name_new FROM %s AS test ORDER BY name_new ASC' %(
                                                                                bucket.name)
            if self.analytics:
                query_template = 'SELECT `$str0` AS name_new FROM %s AS test ORDER BY name_new ASC' %(
                                                                                bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_alias_aggr_fn(self):
        for bucket in self.buckets:
            if self.monitoring:
                e = threading.Event()
                t8 = threading.Thread(name='run_limit_offset', target=self.run_active_requests,args=(e,2))
                t8.start()
            query_template = 'SELECT COUNT(TEST.$str0) from %s AS TEST' %(bucket.name)
            if self.analytics:
                 query_template = 'SELECT COUNT(TEST.`$str0`) from %s AS TEST' %(bucket.name)

            actual_result, expected_result = self.run_query_from_template(query_template)
            if self.monitoring:
                    e.set()
                    t8.join(100)
            self._verify_results(actual_result['results'], expected_result)

    def test_alias_unnest(self):
        for bucket in self.buckets:
            query_template = 'SELECT count(skill) FROM %s AS emp UNNEST emp.$list_str0 AS skill' %(
                                                                            bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

            query_template = 'SELECT count(skill) FROM %s AS emp UNNEST emp.$list_str0 skill' %(
                                                                            bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

##############################################################################################
#
#   ORDER BY CHECKS
##############################################################################################

    def test_order_by_check(self):
        for bucket in self.buckets:
            query_template = 'SELECT $str0, $str1, $obj0.$_obj0_int0 points FROM %s'  % (bucket.name) +\
            ' ORDER BY $str1, $str0, $obj0.$_obj0_int0'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)
            query_template = 'SELECT $str0, $str1 FROM %s'  % (bucket.name) +\
            ' ORDER BY $obj0.$_obj0_int0, $str0, $str1'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_order_by_alias(self):
        for bucket in self.buckets:
            query_template = 'SELECT $str1, $obj0 AS points FROM %s'  % (bucket.name) +\
            ' AS test ORDER BY $str1 DESC, points DESC'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_order_by_alias_arrays(self):
        for bucket in self.buckets:
            query_template = 'SELECT $str1, $obj0, $list_str0[0] AS SKILL FROM %s'  % (
                                                                            bucket.name) +\
            ' AS TEST ORDER BY SKILL, $str1, TEST.$obj0'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_order_by_alias_aggr_fn(self):
        for bucket in self.buckets:
            query_template = 'SELECT $int0, $int1, count(*) AS emp_per_month from %s'% (
                                                                            bucket.name) +\
            ' WHERE $int1 >7 GROUP BY $int0, $int1 ORDER BY emp_per_month, $int1, $int0'  
            actual_result, expected_result = self.run_query_from_template(query_template)
            print "actual results are {0}".format(actual_result['results'])
            print "expected results are {0}".format(expected_result)
            self._verify_results(actual_result['results'], expected_result)

    def test_order_by_aggr_fn(self):
        for bucket in self.buckets:
            query_template = 'SELECT $str1 AS TITLE, min($int1) day FROM %s GROUP'  % (bucket.name) +\
            ' BY $str1 ORDER BY MIN($int1), $str1'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

            if self.analytics:
                self.query = 'SELECT d.email AS TITLE, min(d.join_day) day FROM %s d GROUP'  % (bucket.name) +\
            ' BY d.$str1 ORDER BY MIN(d.join_day), d.$str1'
                actual_result1 = self.run_cbq_query()
                self._verify_results(actual_result1['results'], actual_result['results'])


    def test_order_by_precedence(self):
        for bucket in self.buckets:
            query_template = 'SELECT $str0, $str1 FROM %s'  % (bucket.name) +\
            ' ORDER BY $str0, $str1'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

            query_template = 'SELECT $str0, $str1 FROM %s'  % (bucket.name) +\
            ' ORDER BY $str1, $str0'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_order_by_satisfy(self):
        for bucket in self.buckets:
            query_template = 'SELECT $str0, $list_obj0 FROM %s AS employee ' % (bucket.name) +\
                        'WHERE ANY vm IN employee.$list_obj0 SATISFIES vm.$_list_obj0_int0 > 5 AND' +\
                        ' vm.$_list_obj0_str0 = "ubuntu" END ORDER BY $str0, $list_obj0[0].$_list_obj0_int0'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

##############################################################################################
#
#   DISTINCT
##############################################################################################

    def test_distinct(self):
        for bucket in self.buckets:
            query_template = 'SELECT DISTINCT $str1 FROM %s ORDER BY $str1'  % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_distinct_nested(self):
        for bucket in self.buckets:
            query_template = 'SELECT DISTINCT $obj0.$_obj0_int0 as VAR FROM %s '  % (bucket.name) +\
                         'ORDER BY $obj0.$_obj0_int0'
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

            query_template = 'SELECT DISTINCT $list_str0[0] as skill' +\
                         ' FROM %s ORDER BY $list_str0[0]'  % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

            self.query = 'SELECT DISTINCT $obj0.* FROM %s'  % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

##############################################################################################
#
#   COMPLEX PATHS
##############################################################################################

    def test_simple_complex_paths(self):
        for bucket in self.buckets:
            query_template = 'SELECT $_obj0_int0 FROM %s.$obj0'  % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_alias_complex_paths(self):
        for bucket in self.buckets:
            query_template = 'SELECT $_obj0_int0 as new_attribute FROM %s.$obj0'  % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

    def test_where_complex_paths(self):
        for bucket in self.buckets:
            query_template = 'SELECT $_obj0_int0 FROM %s.$obj0 WHERE $_obj0_int0 = 1'  % (bucket.name)
            actual_result, expected_result = self.run_query_from_template(query_template)
            self._verify_results(actual_result['results'], expected_result)

##############################################################################################
#
#   COMMON FUNCTIONS
##############################################################################################

    def run_query_from_template(self, query_template):
        self.query = self.gen_results.generate_query(query_template)
        expected_result = self.gen_results.generate_expected_result()
        actual_result = self.run_cbq_query()
        return actual_result, expected_result

    def run_query_with_subquery_select_from_template(self, query_template):
        subquery_template = re.sub(r'.*\$subquery\(', '', query_template)
        subquery_template = subquery_template[:subquery_template.rfind(')')]
        keys_num = int(re.sub(r'.*KEYS \$', '', subquery_template).replace('KEYS $', ''))
        subquery_full_list = self.generate_full_docs_list(gens_load=self.gens_load,keys=self._get_keys(keys_num))
        subquery_template = re.sub(r'USE KEYS.*', '', subquery_template)
        sub_results = TuqGenerators(self.log, subquery_full_list)
        self.query = sub_results.generate_query(subquery_template)
        expected_sub = sub_results.generate_expected_result()
        alias = re.sub(r',.*', '', re.sub(r'.*\$subquery\(.*\)', '', query_template))
        alias = re.sub(r'.*as','', re.sub(r'FROM.*', '', alias)).strip()
        if not alias:
            alias = '$1'
        for item in self.gen_results.full_set:
            item[alias] = expected_sub[0]
        query_template = re.sub(r',.*\$subquery\(.*\).*%s' % alias, ',%s' % alias, query_template)
        self.query = self.gen_results.generate_query(query_template)
        expected_result = self.gen_results.generate_expected_result()
        actual_result = self.run_cbq_query()
        return actual_result, expected_result

    def run_query_with_subquery_from_template(self, query_template):
        subquery_template = re.sub(r'.*\$subquery\(', '', query_template)
        subquery_template = subquery_template[:subquery_template.rfind(')')]
        subquery_full_list = self.generate_full_docs_list(gens_load=self.gens_load)
        sub_results = TuqGenerators(self.log, subquery_full_list)
        self.query = sub_results.generate_query(subquery_template)
        expected_sub = sub_results.generate_expected_result()
        alias = re.sub(r',.*', '', re.sub(r'.*\$subquery\(.*\)', '', query_template))
        alias = re.sub(r'.*as ', '', alias).strip()
        self.gen_results = TuqGenerators(self.log, expected_sub)
        query_template = re.sub(r'\$subquery\(.*\).*%s' % alias, ' %s' % alias, query_template)
        self.query = self.gen_results.generate_query(query_template)
        expected_result = self.gen_results.generate_expected_result()
        actual_result = self.run_cbq_query()
        return actual_result, expected_result

    def negative_common_body(self, queries_errors={}):
        if not queries_errors:
            self.fail("No queries to run!")
        for bucket in self.buckets:
            for query_template, error in queries_errors.iteritems():
                try:
                    query = self.gen_results.generate_query(query_template)
                    actual_result = self.run_cbq_query(query.format(bucket.name))
                except CBQError as ex:
                    self.log.error(ex)
                    self.assertTrue(str(ex).find(error) != -1,
                                    "Error is incorrect.Actual %s.\n Expected: %s.\n" %(
                                                                str(ex).split(':')[-1], error))
                else:
                    self.fail("There were no errors. Error expected: %s" % error)

    def run_cbq_query(self, query=None, min_output_size=10, server=None):
        if query is None:
            query = self.query
        if server is None:
           server = self.master
           if server.ip == "127.0.0.1":
            self.n1ql_port = server.n1ql_port
        else:
            if server.ip == "127.0.0.1":
                self.n1ql_port = server.n1ql_port
            if self.input.tuq_client and "client" in self.input.tuq_client:
                server = self.tuq_client
        if self.n1ql_port == None or self.n1ql_port == '':
            self.n1ql_port = self.input.param("n1ql_port", 8093)
            if not self.n1ql_port:
                self.log.info(" n1ql_port is not defined, processing will not proceed further")
                raise Exception("n1ql_port is not defined, processing will not proceed further")
        query_params = {}
        cred_params = {'creds': []}
        rest = RestConnection(server)
        username = rest.username
        password = rest.password
        cred_params['creds'].append({'user': username, 'pass': password})
        for bucket in self.buckets:
            if bucket.saslPassword:
                cred_params['creds'].append({'user': 'local:%s' % bucket.name, 'pass': bucket.saslPassword})
        query_params.update(cred_params)
        if self.use_rest:
            query_params.update({'scan_consistency': self.scan_consistency})
            self.log.info('RUN QUERY %s' % query)

            if self.analytics:
                query = query + ";"
                for bucket in self.buckets:
                    query = query.replace(bucket.name,bucket.name+"_shadow")
                result = rest.analytics_tool(query, 8095, query_params=query_params)

            else :
                result = rest.query_tool(query, self.n1ql_port, query_params=query_params)


        else:
            if self.version == "git_repo":
                output = self.shell.execute_commands_inside("$GOPATH/src/github.com/couchbase/query/" +\
                                                            "shell/cbq/cbq ","","","","","","")
            else:
                os = self.shell.extract_remote_info().type.lower()
                #if (query.find("VALUES") > 0):
                if not(self.isprepared):
                    query = query.replace('"', '\\"')
                    query = query.replace('`', '\\`')

                cmd =  "%s/cbq  -engine=http://%s:%s/ -q -u %s -p %s" % (self.path, server.ip, server.port, username, password)

                output = self.shell.execute_commands_inside(cmd,query,"","","","","")
                if not(output[0] == '{'):
                    output1 = '{'+str(output)
                else:
                    output1 = output
                result = json.loads(output1)
            #result = self._parse_query_output(output)
        if isinstance(result, str) or 'errors' in result:
            raise CBQError(result, server.ip)
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
                cmd = "cd %s/src/github.com/couchbase/query/server/main; " % (gopath) +\
                "./cbq-engine.exe -datastore http://%s:%s/ >/dev/null 2>&1 &" %(
                                                                server.ip, server.port)
            else:
                cmd = "cd %s/src/github.com/couchbase/query//server/main; " % (gopath) +\
                "./cbq-engine -datastore http://%s:%s/ >n1ql.log 2>&1 &" %(
                                                                server.ip, server.port)
            self.shell.execute_command(cmd)
        elif self.version == "sherlock":
            if self.services_init.find('n1ql') != -1:
                return
            os = self.shell.extract_remote_info().type.lower()
            if os != 'windows':
                couchbase_path = testconstants.LINUX_COUCHBASE_BIN_PATH
            else:
                couchbase_path = testconstants.WIN_COUCHBASE_BIN_PATH
            if self.input.tuq_client and "sherlock_path" in self.input.tuq_client:
                couchbase_path = "%s/bin" % self.input.tuq_client["sherlock_path"]
                print "PATH TO SHERLOCK: %s" % couchbase_path
            if os == 'windows':
                cmd = "cd %s; " % (couchbase_path) +\
                "./cbq-engine.exe -datastore http://%s:%s/ >/dev/null 2>&1 &" %(
                                                                server.ip, server.port)
            else:
                cmd = "cd %s; " % (couchbase_path) +\
                "./cbq-engine -datastore http://%s:%s/ >n1ql.log 2>&1 &" %(
                                                                server.ip, server.port)
                n1ql_port = self.input.param("n1ql_port", None)
                if server.ip == "127.0.0.1" and server.n1ql_port:
                    n1ql_port = server.n1ql_port
                if n1ql_port:
                    cmd = "cd %s; " % (couchbase_path) +\
                './cbq-engine -datastore http://%s:%s/ -http=":%s">n1ql.log 2>&1 &' %(
                                                                server.ip, server.port, n1ql_port)
            self.shell.execute_command(cmd)
        else:
            os = self.shell.extract_remote_info().type.lower()
            if os != 'windows':
                cmd = "cd /tmp/tuq;./cbq-engine -couchbase http://%s:%s/ >/dev/null 2>&1 &" %(
                                                                server.ip, server.port)
            else:
                cmd = "cd /cygdrive/c/tuq;./cbq-engine.exe -couchbase http://%s:%s/ >/dev/null 2>&1 &" %(
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

    def generate_docs(self, num_items, start=0):
        try:
            return getattr(self, 'generate_docs_' + self.dataset)(num_items, start)
        except:
            self.fail("There is no dataset %s, please enter a valid one" % self.dataset)

    def generate_docs_default(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee(docs_per_day, start)

    def generate_docs_sabre(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_sabre(docs_per_day, start)

    def generate_docs_employee(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee_data(docs_per_day = docs_per_day, start = start)

    def generate_docs_simple(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee_simple_data(docs_per_day = docs_per_day, start = start)

    def generate_docs_sales(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_employee_sales_data(docs_per_day = docs_per_day, start = start)

    def generate_docs_bigdata(self, docs_per_day, start=0):
        json_generator = JsonGenerator()
        return json_generator.generate_docs_bigdata(end=(1000*docs_per_day), start=start, value_size=self.value_size)


    def _verify_results(self, actual_result, expected_result, missing_count = 1, extra_count = 1):
        if len(actual_result) != len(expected_result):
            missing, extra = self.check_missing_and_extra(actual_result, expected_result)
            self.log.error("Missing items: %s.\n Extra items: %s" % (missing[:missing_count], extra[:extra_count]))
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

    def sort_nested_list(self, result):
        actual_result = []
        for item in result:
            curr_item = {}
            for key, value in item.iteritems():
                if isinstance(value, list) or isinstance(value, set):
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

    def create_primary_index_for_3_0_and_greater(self):
        self.log.info("CREATE PRIMARY INDEX using %s" % self.primary_indx_type)
        rest = RestConnection(self.master)
        versions = rest.get_nodes_versions()
        if versions[0].startswith("4") or versions[0].startswith("3") or versions[0].startswith("5"):
            for bucket in self.buckets:
                if self.primary_indx_drop:
                    self.log.info("Dropping primary index for %s using %s ..." % (bucket.name,self.primary_indx_type))
                    self.query = "DROP PRIMARY INDEX ON %s USING %s" % (bucket.name,self.primary_indx_type)
                    #self.run_cbq_query()
                    self.sleep(3, 'Sleep for some time after index drop')
                self.query = 'select * from system:indexes where name="#primary" and keyspace_id = "%s"' % bucket.name
                res = self.run_cbq_query()
                self.sleep(10)
                #print res
                if self.monitoring:
                    self.query = "delete from system:completed_requests"
                    self.run_cbq_query()
                if not self.skip_primary_index:
                    if (res['metrics']['resultCount'] == 0):
                        self.query = "CREATE PRIMARY INDEX ON %s USING %s" % (bucket.name, self.primary_indx_type)
                        self.log.info("Creating primary index for %s ..." % bucket.name)
                    # if self.gsi_type:
                    #     self.query += " WITH {'index_type': 'memdb'}"
                        try:
                            self.run_cbq_query()
                            self.primary_index_created = True
                            if self.primary_indx_type.lower() == 'gsi':
                                self._wait_for_index_online(bucket, '#primary')
                        except Exception, ex:
                            self.log.info(str(ex))
                if self.monitoring:
                        self.query = "select * from system:active_requests"
                        result = self.run_cbq_query()
                        #print result
                        self.assertTrue(result['metrics']['resultCount'] == 1)
                        self.query = "select * from system:completed_requests"
                        time.sleep(20)
                        result = self.run_cbq_query()
                        #print result




    def _wait_for_index_online(self, bucket, index_name, timeout=6000):
        end_time = time.time() + timeout
        while time.time() < end_time:
            query = "SELECT * FROM system:indexes where name='%s'" % index_name
            res = self.run_cbq_query(query)
            for item in res['results']:
                if 'keyspace_id' not in item['indexes']:
                    self.log.error(item)
                    continue
                if item['indexes']['keyspace_id'] == bucket.name:
                    if item['indexes']['state'] == "online":
                        return
            self.sleep(5, 'index is pending or not in the list. sleeping... (%s)' % [item['indexes'] for item in res['results']])
        raise Exception('index %s is not online. last response is %s' % (index_name, res))


    def _get_keys(self, key_num):
        keys = []
        for gen in self.gens_load:
            gen_copy = copy.deepcopy(gen)
            for i in xrange(gen_copy.end):
                key, _ = gen_copy.next()
                keys.append(key)
                if len(keys) == key_num:
                    return keys
        return keys
