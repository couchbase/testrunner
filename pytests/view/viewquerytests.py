from TestInput import TestInputSingleton
from autocompaction import AutoCompactionTests
from basetestcase import BaseTestCase
from couchbase_helper.cluster import Cluster
from couchbase_helper.document import View
from couchbase_helper.documentgenerator import DocumentGenerator
from membase.api.rest_client import RestConnection, RestHelper, Bucket
from membase.helper.failover_helper import FailoverHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import VBucketAwareMemcached, \
    KVStoreAwareSmartClient
from memcached.helper.old_kvstore import ClientKeyValueStore
from old_tasks import task, taskmanager
from remote.remote_util import RemoteMachineShellConnection
from threading import Thread, Event
import copy
import datetime
import json
import logger
import math
import random
import re
import sys
import threading
import time
import types
import unittest
import uuid

class StoppableThread(Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(StoppableThread, self).__init__(group=group, target=target,
                        name=name, args=args, kwargs=kwargs)
        self._stopper = Event()
        self.tasks = []

    def stopit(self):
        for task in self.tasks:
            task.cancel();
        self._stopper.set()

    def stopped(self):
        return self._stopper.isSet()

class ViewQueryTests(BaseTestCase):
    def setUp(self):
        try:
            super(ViewQueryTests, self).setUp()
            self.num_docs = self.input.param("num-docs", 10000)
            self.limit = self.input.param("limit", None)
            self.reduce_fn = self.input.param("reduce_fn", None)
            self.skip_rebalance = self.input.param("skip_rebalance", False)
            self.wait_persistence = self.input.param("wait_persistence", False)
            self.docs_per_day = self.input.param('docs-per-day', 200)
            self.retries = self.input.param('retries', 100)
            self.timeout = self.input.param('timeout', None)
            self.error = None
            self.master = self.servers[0]
            self.thread_crashed = Event()
            self.thread_stopped = Event()
            self.server = None
        except Exception as ex:
            self.log.error("SETUP WAS FAILED. TEST WILL BE SKIPPED")
            self.fail(ex)

    def tearDown(self):
        try:
            super(ViewQueryTests, self).tearDown()
        finally:
            self.cluster.shutdown()



    def test_simple_dataset_stale_queries(self):
        '''
        Test uses simple data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. Start load data
            2. Simultaneously start querying(this test use all 3 options
               of stale at the same time
        '''
        # init dataset for test
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)
        data_set.add_stale_queries(limit=self.limit)
        self._query_test_init(data_set)

    def test_simple_dataset_stale_queries_data_modification(self):
        '''
        Test uses simple data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. load data
            2. when data is loaded query view with stale=false
            3. verify all keys are as expected
            4. Delete a part of items
            5. query view with stale=false again
            6. Verify that only non-deleted keys appeared
        '''
        # init dataset for test
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)
        data_set.add_stale_queries(stale_param="false", limit=self.limit)
        #  generate items
        generator_load = data_set.generate_docs(data_set.views[0], start=0,
                                                end=self.num_docs // 2)
        generator_delete = data_set.generate_docs(data_set.views[0],
                                                  start=self.num_docs // 2,
                                                  end=self.num_docs)

        self.load(data_set, generator_load)
        self.load(data_set, generator_delete)

        gen_query = copy.deepcopy(generator_load)
        gen_query.extend(generator_delete)
        self._query_all_views(data_set.views, gen_query,
                              verify_expected_keys=True)

        #delete docs
        self.load(data_set, generator_delete, op_type='delete')
        self._query_all_views(data_set.views, generator_load,
                              verify_expected_keys=True)

    def test_simple_dataset_startkey_endkey_queries(self):
        '''
        Test uses simple data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. Start load data
            2. Simultaneously start querying(different combinations of
               stratkey. endkey, descending, inclusive_end, parameters)
        '''
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)
        data_set.add_startkey_endkey_queries(limit=self.limit)
        self._query_test_init(data_set)

    def test_simple_dataset_startkey_endkey_queries_with_pass_change(self):
        '''
        Test uses simple data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. Start load data
            2. Start querying(different combinations of
               stratkey. endkey, descending, inclusive_end, parameters)
            3. Change cluster password
            4.Perform same queries
        '''
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)
        data_set.add_startkey_endkey_queries(limit=self.limit)
        generator_load = data_set.generate_docs(data_set.views[0], start=0,
                                                end=self.num_docs)
        self.load(data_set, generator_load)
        self._query_all_views(data_set.views, generator_load,
                              verify_expected_keys=True)
        old_pass = self.master.rest_password
        self.change_password(new_password=self.input.param("new_password", "new_pass"))
        try:
            self._query_all_views(data_set.views, generator_load,
                                  verify_expected_keys=True, retries=1)
        finally:
            self.change_password(new_password=old_pass)

    def test_simple_dataset_startkey_endkey_queries_with_port_change(self):
        '''
        Test uses simple data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. Start load data
            2. Start querying(different combinations of
               stratkey. endkey, descending, inclusive_end, parameters)
            3. Change cluster port
            4.Perform same queries
        '''
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)
        data_set.add_startkey_endkey_queries(limit=self.limit)
        generator_load = data_set.generate_docs(data_set.views[0], start=0,
                                                end=self.num_docs)
        self.load(data_set, generator_load)
        self._query_all_views(data_set.views, generator_load,
                              verify_expected_keys=True)

        self.change_port(new_port=self.input.param("new_port", "9090"))
        try:
            self._query_all_views(data_set.views, generator_load,
                                  verify_expected_keys=True, retries=1)
        finally:
            self.change_port(new_port='8091',
                             current_port=self.input.param("new_port", "9090"))

    def test_reproduce_mb_7193_queries(self):
        '''
        Test uses simple data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Based on MB-7193
        '''
        ddoc_name = 'ddoc/test'
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs,
                                 name_ddoc=ddoc_name)
        data_set.add_startkey_endkey_queries(limit=self.limit)
        generator_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, generator_load)
        self._query_all_views(data_set.views, generator_load,
                              verify_expected_keys=True)

    def test_simple_dataset_startkey_endkey_non_json_queries(self):
        '''
        Test uses simple data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. Start load data
            2. Simultaneously start querying(different combinations of
               stratkey. endkey, descending, inclusive_end, parameters with non-json char)
        '''
        symbols = ["\xf1", "\xe1", "\xfc", "\xbf", "\xf1", "\xe1", "\xfc",
                   "\xbf"]
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs,
                                 json_case=True)
        generator_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, generator_load)
        for symbol in symbols:
            data_set.add_startkey_endkey_non_json_queries(symbol.encode("utf8", "ignore"), limit=self.limit)
            self._query_all_views(data_set.views, generator_load,
                                  verify_expected_keys=True)

    def test_simple_dataset_all_queries(self):
        '''
        Test uses simple data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. Start load data
            2. Simultaneously start querying(include stale and startkey endkey queries)
        '''
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)
        data_set.add_all_query_sets(limit=self.limit)
        self._query_test_init(data_set)

    def test_simple_dataset_negative_queries(self):
        '''
        Test uses simple data set:
             -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. Start load data
            2. Simultaneously start querying(different invalid query parameters)
            3. Verifies expected error message matches with actual
        '''
        # init dataset for test
        query_params = self.input.param("query_params", None)
        error = self.input.param("error", None)

        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)
        data_set.add_negative_query(query_params, error)
        self._query_test_init(data_set)

    def test_simple_dataset_stale_queries_extended(self):
        '''
        Test uses simple data set:
             -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. Start load data
            2. Simultaneously start querying(stale = ok, false, or update_after)
            3. Load some more data
            4. Verify stale ok and update_after returns old index, stale=false returns new index
        '''
        # init dataset for test
        stale = str(self.input.param("stale_param", "update_after"))
        num_docs_to_add = self.input.param("num_docs_to_add", 10)
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)
        generator_load = data_set.generate_docs(data_set.views[0])
        generator_extra = data_set.generate_docs(data_set.views[0],
                                                 start=self.num_docs,
                                                 end=self.num_docs + num_docs_to_add)
        data_set.add_stale_queries()
        self.load(data_set, generator_load)
        self._query_all_views(data_set.views, generator_load,
                                  verify_expected_keys=True)

        #load one more portion of data
        self.load(data_set, generator_extra)
        #query once again
        for view in data_set.views:
            view.queries = []
        data_set.add_stale_queries(stale_param=stale, limit=self.limit)
        if stale == 'False':
            gen_query = copy.deepcopy(generator_load)
            gen_query.extend(generator_extra)
            self._query_all_views(data_set.views, gen_query,
                                  verify_expected_keys=True, retries=1)
        elif stale in ['update_after', 'ok']:
            self._query_all_views(data_set.views, generator_load,
                                  verify_expected_keys=True, retries=1)
        self.log.info("Stale %s query passed as expected" % stale)


    def test_employee_dataset_startkey_endkey_queries(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. Simultaneously start querying(starkey endkey descending
            inclusive_end combinations)
        '''

        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
        data_set.add_startkey_endkey_queries(limit=self.limit)
        self._query_test_init(data_set)

    def test_employee_dataset_check_consistency(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Load data
            2. Wait for persistence
            3. Start querying(starkey endkey descending
            inclusive_end combinations) - all result shoul appear consistent
            4. Start rebalance
        '''
        options = {"updateMinChanges" : self.docs_per_day,
                   "replicaUpdateMinChanges" : self.docs_per_day}
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day,
                                   ddoc_options=options)
        gen_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, gen_load)

        for view in data_set.views:
            # run queries to create indexes
            self.cluster.query_view(self.servers[0], view.name, view.name,
                                    {"connectionTimeout" : 60000})
            active_tasks = self.cluster.async_monitor_active_task(self.servers,
                                                                 "indexer",
                                                                 "_design/" + view.name,
                                                                 wait_task=False)
            for active_task in active_tasks:
                 active_task.result()

        data_set.add_stale_queries(stale_param="ok")

        for view in data_set.views:
            view.consistent_view = True
        self.log.info("QUERY BEFORE REBALANCE")
        self._query_all_views(data_set.views, gen_load,
                                  verify_expected_keys=True, retries=1)
        self.log.info("START REBALANCE AND QUERY")
        rebalance = self.cluster.async_rebalance(self.servers[:1], self.servers[1:], [])
        while rebalance.state != "FINISHED":
            self._query_all_views(data_set.views, gen_load,
                                  verify_expected_keys=True, retries=1)
        rebalance.result()

    def test_employee_dataset_min_changes_check(self):
        '''
        Test uses simple data set:
             -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. Create views with updateMinChanges option
            2. Load data less that changes
            3. Check index is not started
            4. load data more that changes
            5.Check index is triggered
        '''
        min_changes = self.input.param('min-changes', 1000)
        min_changes_r = self.input.param('min-changes-replica', 2000)
        options = {"updateMinChanges" : min_changes,
                   "replicaUpdateMinChanges" : min_changes_r}
        point_1 = min(min_changes, min_changes_r) - 100
        point_2 = point_1 + (max(min_changes, min_changes_r) - min(min_changes, min_changes_r)) / 2
        point_3 = max(min_changes, min_changes_r) + 200
        index_types = (('main', 'replica'), ('replica', 'main'))[min_changes > min_changes_r]
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs,
                                 ddoc_options=options)
        gen_load_1 = data_set.generate_docs(data_set.views[0], start=0,
                                            end=point_1)

        self.load(data_set, gen_load_1)
        self.assertFalse(data_set.is_index_triggered_or_ran(data_set.views[0], index_types[0]),
                        "View %s, Index %s was triggered. Options %s" % (data_set.views[0],
                                                                    index_types[0], options))
        self.assertFalse(data_set.is_index_triggered_or_ran(data_set.views[0], index_types[1]),
                        "View %s, Index %s was triggered. Options %s" % (data_set.views[0],
                                                                    index_types[1], options))
        self.log.info("Indexes are not triggered")
        changes_done = point_1
        changes_done = self._load_until_point(point_2, changes_done, data_set)
        self.assertTrue(data_set.is_index_triggered_or_ran(data_set.views[0], index_types[0]),
                        "View %s, Index %s wasn't triggered. Options %s" % (data_set.views[0],
                                                                    index_types[0], options))
        self.assertFalse(data_set.is_index_triggered_or_ran(data_set.views[0], index_types[1]),
                        "View %s, Index %s was triggered. Options %s" % (data_set.views[0],
                                                                index_types[1], options))
        self.log.info("View %s, Index %s was triggered. Options %s" % (data_set.views[0],
                                                                    index_types[0], options))
        retry = 0
        self._load_until_point(point_3, changes_done, data_set)
        self.assertTrue(data_set.is_index_triggered_or_ran(data_set.views[0], index_types[1]),
                        "View %s, Index %s wasn't triggered. Options %s" % (data_set.views[0],
                                                                index_types[1], options))
        self.log.info("View %s, Index %s was triggered. Options %s" % (data_set.views[0],
                                                                       index_types[1], options))

    def _load_until_point(self, point, changes_done, data_set):
        max_retry = 20
        retry = 0
        while (data_set.get_replica_partition_seq(data_set.views[0]) < point and retry < max_retry):
            changes_done += (100 * len(self.servers))
            gen_load = data_set.generate_docs(data_set.views[0],
                                            start=changes_done,
                                            end=changes_done + (100 * len(self.servers)))
            changes_done += (100 * len(self.servers))
            self.load(data_set, gen_load)
            self.log.info("Partition sequence is: %s" % data_set.get_replica_partition_seq(data_set.views[0]))
        return changes_done

    def test_employee_dataset_key_quieres(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. Simultaneously start key queries
        '''
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
        data_set.add_key_queries(limit=self.limit)
        self._query_test_init(data_set)

    def test_employee_dataset_negative_queries(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. Simultaneously start querying(parameters values are invalid)
            3. Verify that expected error equals to actual
        '''
        # init dataset for test
        query_params = self.input.param("query_params", None)
        error = self.input.param("error", None)

        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
        data_set.add_negative_query(query_params, error)
        self._query_test_init(data_set)

    def test_big_dataset_negative_queries(self):
        '''
        Test uses big data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. Start load data
            2. Simultaneously start querying(queries should raise an error)
        '''
        error = self.input.param('error', 'too large')
        self.value_size = 34603008
        data_set = BigDataSet(self.master, self.cluster, self.num_docs, self.value_size)
        data_set.add_stale_queries(error)
        self._query_test_init(data_set)

    def test_employee_dataset_invalid_startkey_docid_endkey_docid_queries(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. Simultaneously start key queries with invalid startkey_docid or endkey_docid
        '''
        # init dataset for test
        valid_params = self.input.param("valid_params", None)
        invalid_params = self.input.param("invalid_params", None)

        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
        data_set.add_query_invalid_startkey_endkey_docid(valid_params, invalid_params)
        self._query_test_init(data_set)

    def test_employee_dataset_startkey_endkey_docid_queries(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. simultaneously run queries (staartkey endkey startkey_docid endkey_docid
            inclusive_end, descending combinations)
            '''
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)

        data_set.add_startkey_endkey_docid_queries(limit=self.limit)
        self._query_test_init(data_set)

    def test_employee_dataset_group_queries(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. simultaneously start queries with group and group_level params
        '''
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)

        data_set.add_group_count_queries(limit=self.limit)
        self._query_test_init(data_set)


    def test_employee_dataset_startkey_endkey_queries_rebalance_in(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            3. start rebalance in
            4. Start querying
        '''
        num_nodes_to_add = self.input.param('num_nodes_to_add', 1)
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)

        data_set.add_startkey_endkey_queries(limit=self.limit)
        gen_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, gen_load)
        self._query_all_views(data_set.views, gen_load)

        # rebalance_in and verify loaded data
        rebalance = self.cluster.async_rebalance(self.servers[:1],
                                                 self.servers[1:num_nodes_to_add + 1],
                                                 [])
        self._query_all_views(data_set.views, gen_load)
        rebalance.result()

    def test_employee_dataset_startkey_endkey_queries_rebalance_out(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start rebalance out
            3. Start querying
        '''

        num_nodes_to_rem = self.input.param('num_nodes_to_rem', 1)
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)

        data_set.add_startkey_endkey_queries(limit=self.limit)
        gen_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, gen_load)
        self._query_all_views(data_set.views, gen_load)

        # rebalance_out and verify loaded data
        rebalance = self.cluster.async_rebalance(self.servers,
                                                 [],
                                                 self.servers[-num_nodes_to_rem:])
        self._query_all_views(data_set.views, gen_load)
        rebalance.result()

    def test_employee_dataset_startkey_endkey_queries_failover_queries(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. wait until is persisted
            3. failover some nodes and start rebalance
            4. Start querying
        '''
        self.retries += 50
        failover_factor = self.input.param("failover-factor", 1)
        failover_nodes = self.servers[1 : failover_factor + 1]
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
        data_set.add_startkey_endkey_queries(limit=self.limit)
        gen_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, gen_load)
        self._query_all_views(data_set.views, gen_load)
        try:
            # failover and verify loaded data
            self.cluster.failover(self.servers, failover_nodes)
            self.sleep(10, "10 seconds sleep after failover before invoking rebalance...")
            rebalance = self.cluster.async_rebalance(self.servers,
                                                     [], failover_nodes)

            self._query_all_views(data_set.views, gen_load)

            msg = "rebalance failed while removing failover nodes {0}".format(failover_nodes)
            self.assertTrue(rebalance.result(), msg=msg)

            #verify queries after failover
            self._query_all_views(data_set.views, gen_load)
        finally:
            for server in failover_nodes:
                shell = RemoteMachineShellConnection(server)
                shell.start_couchbase()
                time.sleep(10)
                shell.disconnect()

    def test_employee_dataset_startkey_endkey_queries_incremental_failover_queries(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. wait until is persisted
            3. failover nodes incrementaly in a loop and start rebalance in
            4. Start querying
        '''
        failover_nodes = []
        failover_factor = self.input.param("failover-factor", 1)
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
        data_set.add_startkey_endkey_queries(limit=self.limit)
        gen_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, gen_load)
        self._query_all_views(data_set.views, gen_load)
        servers = self.servers
        try:
            # incrementaly failover nodes and verify loaded data
            for i in range(failover_factor):
                failover_node = self.servers[i]
                self.cluster.failover(self.servers, [failover_node])
                failover_nodes.append(failover_node)
                self.log.info("10 seconds sleep after failover before invoking rebalance...")
                time.sleep(10)
                servers = self.servers[i:]
                self.master = self.servers[i + 1]

                rebalance = self.cluster.async_rebalance(servers,
                                                         [], failover_nodes)

                self._query_all_views(data_set.views, gen_load, server_to_query=i + 1)

                del(servers[i])

                msg = "rebalance failed while removing failover nodes {0}".format(failover_nodes)
                self.assertTrue(rebalance.result(), msg=msg)

        finally:
            for server in failover_nodes:
                shell = RemoteMachineShellConnection(server)
                shell.start_couchbase()
                self.sleep(10, "10 seconds for couchbase server to startup...")
                shell.disconnect()

    def test_employee_dataset_startkey_endkey_queries_start_stop_rebalance_in_incremental(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. wait data for persistence
            3. start rebalance in
            4. stop rebalance
            5. Start querying
        '''
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)

        data_set.add_startkey_endkey_queries(limit=self.limit)
        gen_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, gen_load)
        self._query_all_views(data_set.views, gen_load)

        rest = self._rconn()

        for server in self.servers[1:]:
            nodes = rest.node_statuses()
            self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
            self.log.info("adding node {0}:{1} to cluster".format(server.ip, server.port))
            otpNode = rest.add_node(self.master.rest_username, self.master.rest_password, server.ip, server.port)
            msg = "unable to add node {0}:{1} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip, server.port))

            # Just doing 2 iterations
            for expected_progress in [30, 60]:
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
                reached = RestHelper(rest).rebalance_reached(expected_progress)
                self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
                stopped = rest.stop_rebalance(wait_timeout=100)
                self.assertTrue(stopped, msg="unable to stop rebalance")
                self._query_all_views(data_set.views, gen_load)

            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
            self.assertTrue(rest.monitorRebalance(), msg="rebalance operation failed restarting")
            self._query_all_views(data_set.views, gen_load)

            self.assertTrue(len(rest.node_statuses()) - len(nodes) == 1, msg="number of cluster's nodes is not correct")

    def test_employee_dataset_startkey_endkey_queries_start_stop_rebalance_out_incremental(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. wait data for persistence
            3. start rebalance out
            4. stop rebalance
            5. Start querying
        '''
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)

        data_set.add_startkey_endkey_queries(limit=self.limit)
        gen_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, gen_load)
        self._query_all_views(data_set.views, gen_load)

        rest = self._rconn()

        for server in self.servers[1:]:
            nodes = rest.node_statuses()
            ejectedNodes = []
            self.log.info("removing node {0}:{1} from cluster".format(server.ip, server.port))
            for node in nodes:
                if "{0}:{1}".format(node.ip, node.port) == "{0}:{1}".format(server.ip, server.port):
                    ejectedNodes.append(node.id)
                    break

            # Just doing 2 iterations
            for expected_progress in [30, 60]:
                rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=ejectedNodes)
                reached = RestHelper(rest).rebalance_reached(expected_progress)
                self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
                stopped = rest.stop_rebalance()
                self.assertTrue(stopped, msg="unable to stop rebalance")

                #for cases if rebalance ran fast
                if RestHelper(rest).is_cluster_rebalanced():
                    self.log.info("Rebalance is finished already.")
                    break

                self._query_all_views(data_set.views, gen_load)

            #for cases if rebalance ran fast
            if RestHelper(rest).is_cluster_rebalanced():
                self.log.info("Rebalance is finished already.")
                nodes = rest.node_statuses()
                continue
            rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=ejectedNodes)

            self.assertTrue(rest.monitorRebalance(), msg="rebalance operation failed restarting")
            self._query_all_views(data_set.views, gen_load)

            self.assertTrue(len(nodes) - len(rest.node_statuses()) == 1, msg="number of cluster's nodes is not correct")


    def test_employee_dataset_stale_queries(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start stale queries(ok, update_after, false)
        '''
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)

        data_set.add_stale_queries()
        self._query_test_init(data_set)

    def test_employee_dataset_all_queries(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start queries: stale, group, starkey/endkey, stratkey_docid
        '''
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
        data_set.add_all_query_sets()
        if self.wait_persistence:
            gen_load = data_set.generate_docs(data_set.views[0])
            self.load(data_set, gen_load)
            for server in self.servers:
                self.log.info("-->RebalanceHelper.wait_for_persistence({},{}".format(server, data_set.bucket))
                RebalanceHelper.wait_for_persistence(server, data_set.bucket)
            self.log.info("-->_query_all_views...")
            self._query_all_views(data_set.views, gen_load)
        else:
            self._query_test_init(data_set)

    def test_employee_dataset_skip_queries(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start queries with skip
        '''
        skip = self.input.param('skip', 200)
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)

        data_set.add_skip_queries(skip)
        self._query_test_init(data_set)

    def test_employee_dataset_skip_incremental_queries(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start skip (pagination)
        '''
        skip = 0
        max_skip = self.limit * 10
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
        gen_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, gen_load)

        for view in data_set.views:
            if view.reduce_fn:
                data_set.views.remove(view)

        while skip < max_skip:
            for view in data_set.views:
                data_set.add_skip_queries(skip, limit=self.limit)
            self._query_all_views(data_set.views, gen_load)
            skip += self.limit

    def test_all_datasets_all_queries(self):
        '''
        Test uses employee data sets:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
             - documents are like {"name":name<string>, "age":age<int>}
        Steps to repro:
            1. Start load data
            2. start all possible combinations of querying
        '''
        ds1 = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
        ds2 = SimpleDataSet(self.master, self.cluster, self.num_docs)
        data_sets = [ds1, ds2]

        # load and query all views and datasets
        test_threads = []
        for ds in data_sets:
            ds.add_all_query_sets()
            t = Thread(target=self._query_test_init,
                       name=ds.name,
                       args=(ds))
            test_threads.append(t)
            t.start()

        [t.join() for t in test_threads]

    def test_employee_dataset_query_all_nodes(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start querying all nodes
        '''
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
        data_set.add_startkey_endkey_queries(limit=self.limit)
        gen_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, gen_load)

        query_nodes_threads = []
        for i in range(len(self.servers)):
            t = StoppableThread(target=self._query_all_views,
               name="query-node-{0}".format(i),
               args=(data_set.views, gen_load,
                     None, False, [], 100, i))
            query_nodes_threads.append(t)
            t.start()

        while True:
            if not query_nodes_threads:
                break
            self.thread_stopped.wait(60)
            if self.thread_crashed.is_set():
                for t in query_nodes_threads:
                    t.stopit()
                break
            else:
                query_nodes_threads = [d for d in query_nodes_threads if d.is_alive()]
                self.thread_stopped.clear()

    def test_query_node_warmup(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start querying
            3. stop and start one node
            4. start queriyng again
        '''
        try:
            data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)

            data_set.add_startkey_endkey_queries(limit=self.limit)
            gen_load = data_set.generate_docs(data_set.views[0])
            self.load(data_set, gen_load)
            self._query_all_views(data_set.views, gen_load)

            # Pick a node to warmup
            server = self.servers[-1]
            shell = RemoteMachineShellConnection(server)
            self.log.info("Node {0} is being stopped".format(server.ip))
            shell.stop_couchbase()
            time.sleep(20)
            shell.start_couchbase()
            self.log.info("Node {0} should be warming up".format(server.ip))

            self._query_all_views(data_set.views, gen_load)
        finally:
            shell.disconnect()

    def test_employee_dataset_query_add_nodes(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start querying
            3. add some nodes but don't rebalance
            4. start queriyng
        '''
        how_many_add = self.input.param('how_many_add', 0)

        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
        data_set.add_startkey_endkey_queries(limit=self.limit)
        gen_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, gen_load)
        self._query_all_views(data_set.views, gen_load)

        rest = self._rconn()

        for server in self.servers[1:how_many_add + 1]:
            self.log.info("adding node {0}:{1} to cluster".format(server.ip, server.port))
            otpNode = rest.add_node(self.servers[0].rest_username, self.servers[0].rest_password,
                                    server.ip, server.port)
            msg = "unable to add node {0}:{1} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip, server.port))

        self._query_all_views(data_set.views, gen_load)

    def test_employee_dataset_startkey_endkey_queries_rebalance_incrementaly(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start querying
            3. rebalance incrementally
        '''
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)

        data_set.add_startkey_endkey_queries(limit=self.limit)
        gen_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, gen_load)
        self._query_all_views(data_set.views, gen_load)

        # rebalance_in and verify loaded data
        for i in range(1, len(self.servers)):
                rebalance = self.cluster.async_rebalance(self.servers[:i + 1], [self.servers[i]], [])
                self.server = self.servers[i]
                self.log.info("Queries started!")
                self._query_all_views(data_set.views, gen_load)
                self.log.info("Queries finished")
                rebalance.result()
                self.log.info("Rebalance finished")
    '''
    Test verifies querying when other thread is adding/updating/deleting other view
    Parameters:
        num-views-to-modify - number of views to add/edit/delete
        action - can be create/update/delete
    '''
    def test_employee_dataset_query_during_modifying_other_views(self):
        views_num = self.input.param('num-views-to-modify', 1)
        action = self.input.param('action', 'create')
        ddoc_name = "view_ddoc"
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
        data_set.add_startkey_endkey_queries()
        gen_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, gen_load)

        view_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        views = [View("view_name_" + str(i), view_map_func, None, True)for i in range(views_num)]

        tasks = []
        for view in views:
            tasks.append(self.cluster.async_create_view(self.servers[0], ddoc_name, view))

        if action in ['update', 'delete']:
            for task in tasks:
                task.result()
            tasks = []
            #update/delete
            if action == 'update':
                view_map_func_new = "function (doc) {if(doc.age !== undefined) { emit(doc.age, doc.name);}}"
                views = [View("view_name_" + str(i), view_map_func_new, None, True)for i in range(views_num)]
                for view in views:
                    tasks.append(self.cluster.async_create_view(self.servers[0], ddoc_name, view))
            if action == 'delete':
                for view in views:
                    tasks.append(self.cluster.async_delete_view(self.servers[0], ddoc_name, view))

        self._query_all_views(data_set.views, gen_load)

        for task in tasks:
                task.result()


    def test_employee_dataset_startkey_compaction_queries(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start querying
            3. start compaction
        '''
        percent_compaction = self.input.param('percent_compaction', 10)
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
        data_set.add_startkey_endkey_queries()

        self.disable_compaction()

        gen_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, gen_load)
        self._query_all_views(data_set.views, gen_load)

        for view in data_set.views:
            fragmentation_monitor = self.cluster.async_monitor_view_fragmentation(self.master,
                                                                                  view.name,
                                                                                  percent_compaction,
                                                                                  view.bucket)
            # generate load until fragmentation reached
            while fragmentation_monitor.state != "FINISHED":
                # update docs to create fragmentation
                self.load(data_set, gen_load, op_type="update")
            fragmentation_monitor.result()

            compaction_task = self.cluster.async_compact_view(self.master, view.name, view.bucket)
            self._query_all_views(data_set.views, gen_load)
            compaction_task.result()

    def test_employee_dataset_failover_pending_queries(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start querying
            3. gor node into pending failover state
        '''
        failover_factor = self.input.param("failover-factor", 1)
        failover_nodes = self.servers[1 : failover_factor + 1]
        try:
            data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)

            data_set.add_startkey_endkey_queries(limit=self.limit)
            gen_load = data_set.generate_docs(data_set.views[0])
            self.load(data_set, gen_load)
            self._query_all_views(data_set.views, gen_load)

            # failover and verify loaded data
            self.cluster.failover(self.servers, failover_nodes)
            self.log.info("5 seconds sleep after failover ...")
            time.sleep(5)

            self._query_all_views(data_set.views, gen_load)
        finally:
            for server in failover_nodes:
                shell = RemoteMachineShellConnection(server)
                shell.start_couchbase()
                time.sleep(10)
                shell.disconnect()

    def test_employee_dataset_query_one_nodes_different_threads(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start querying one node - in different threads
        '''
        num_threads = self.input.param('num_threads', 2)
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
        data_set.add_startkey_endkey_queries()
        gen_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, gen_load)

        query_nodes_threads = []
        for i in range(num_threads):
            t = StoppableThread(target=self._query_all_views,
                   name="query-node-%s" % i,
                   args=(data_set.views, gen_load))
            query_nodes_threads.append(t)
            t.start()

        while True:
            if not query_nodes_threads:
                break
            self.thread_stopped.wait(60)
            if self.thread_crashed.is_set():
                for t in query_nodes_threads:
                    t.stopit()
                break
            else:
                query_nodes_threads = [d for d in query_nodes_threads if d.is_alive()]
                self.thread_stopped.clear()
    '''
    Test verifies querying when other thread is updating/deleting its view
       Parameters:
           action - can be create/update/delete
           error - expected error message for queries
    '''
    def test_simple_dataset_query_during_modifying_its_view(self):
        action = self.input.param('action', 'update')
        error = self.input.param('error', None)
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)
        data_set.add_startkey_endkey_queries()
        generator_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, generator_load)
        self._query_all_views(data_set.views, generator_load)

        tasks = []
        #update/delete
        if action == 'update':
            view_map_func_new = "function (doc) {if(doc.age !== undefined) { emit(doc.age);}}"
            views = [View(view.name, view_map_func_new, None, False) for view in data_set.views]
            for view in views:
                tasks.append(self.cluster.async_create_view(self.servers[0], view.name, view))
                self._query_all_views(data_set.views, generator_load)
        if action == 'delete':
            views = [View(view.name, None, None, False) for view in data_set.views]
            for view in views:
                tasks.append(self.cluster.async_delete_view(self.servers[0], view.name, view))
                time.sleep(1)
                for view in data_set.views:
                    for q in view.queries:
                        q.error = error
                self._query_all_views(data_set.views, generator_load)
        for task in tasks:
            task.result()

    def test_simple_dataset_queries_during_modifying_docs(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start querying
            3. start ddocs modifications
        '''
        skip = 0
        action = self.input.param('action', 'recreate')
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)

        data_set.add_skip_queries(skip, limit=self.limit)
        generator_load = data_set.generate_docs(data_set.views[0], start=0,
                                                  end=self.num_docs // 2)
        generator_update_delete = data_set.generate_docs(data_set.views[0],
                                                  start=self.num_docs // 2,
                                                  end=self.num_docs)
        self.load(data_set, generator_load)
        self.load(data_set, generator_update_delete)

        if action == 'recreate':
            self.load(data_set, generator_update_delete, op_type="update")
            gen_query = copy.deepcopy(generator_load)
            gen_query.extend(generator_update_delete)
            self._query_all_views(data_set.views, gen_query)
        if action == 'delete':
            self.load(data_set, generator_update_delete, op_type="delete")
            for server in self.servers:
                RebalanceHelper.wait_for_persistence(server, data_set.bucket)
            self._query_all_views(data_set.views, generator_load)


    def test_employee_dataset_query_stop_master(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start querying
            3. stop master
        '''
        try:
            error = self.input.param('error', None)
            data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
            data_set.add_startkey_endkey_queries()
            generator_load = data_set.generate_docs(data_set.views[0])
            self.load(data_set, generator_load)
            self._query_all_views(data_set.views, generator_load)

            shell = RemoteMachineShellConnection(self.master)
            self.log.info("Master Node is being stopped")
            shell.stop_couchbase()
            #error should be returned in results
            for view in data_set.views:
                for q in view.queries:
                    q.error = error
            time.sleep(20)
            self._query_all_views(data_set.views, generator_load, server_to_query=1)
        finally:
            shell = RemoteMachineShellConnection(self.master)
            shell.start_couchbase()
            time.sleep(10)
            shell.disconnect()

    def test_start_end_key_docid_extra_params(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start querying (startkey/endkey with stale, skip, limit)
        '''
        extra_params = self.input.param('extra_params', None)
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)
        data_set.add_stale_queries()
        generator_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, generator_load)
        self._query_all_views(data_set.views, generator_load)
        data_set.add_startkey_endkey_docid_queries_extra_params(extra_params=extra_params)
        self._query_all_views(data_set.views, generator_load)


    '''
    load documents, run a view query with 1M results
    limit =1000 , skip = 0 -> 200 and then 200->0
    '''
    def test_employee_dataset_skip_bidirection_queries(self):
        skip = self.input.param('skip', 200)
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day)

        data_set.add_skip_queries(skip, limit=self.limit)
        data_set.add_skip_queries(skip)
        generator_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, generator_load)
        self._query_all_views(data_set.views, generator_load)

        for view in data_set.views:
            view.queries = []
        data_set.add_skip_queries(skip, limit=self.limit)
        self._query_all_views(data_set.views, generator_load)

    def test_employee_dataset_query_different_buckets(self):
        '''
        Test uses employee data set:
            -documents are structured as {"name": name<string>,
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "email": email<string>,
                                       "job_title" : title<string>,
                                       "type" : type<string>,
                                       "desc" : desc<tring>}
        Steps to repro:
            1. Start load data
            2. start querying multiply buckets
        '''
        data_sets = []
        generators = []
        for bucket in self.buckets:
            data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day, bucket=bucket)
            data_sets.append(data_set)
            generators.append(data_set.generate_docs(data_set.views[0]))
        iterator = 0
        for data_set in data_sets:
            data_set.add_startkey_endkey_queries()
            self.load(data_set, generators[iterator])
            self._query_all_views(data_set.views, generators[iterator])
            iterator += 1

        query_bucket_threads = []
        iterator = 0
        for data_set in data_sets:
            t = StoppableThread(target=self._query_all_views,
                                name="query-bucket-{0}".format(data_set.bucket.name),
                                args=(data_set.views, generators[iterator]))
            query_bucket_threads.append(t)
            t.start()
            iterator += 1

        while True:
            if not query_bucket_threads:
                break
            self.thread_stopped.wait(60)
            if self.thread_crashed.is_set():
                for t in query_bucket_threads:
                    t.stopit()
                break
            else:
                query_bucket_threads = [d for d in query_bucket_threads if d.is_alive()]
                self.thread_stopped.clear()

    '''
     Test verifies querying when other thread is adding/updating/deleting other view
     Parameters:
         num-ddocs-to-modify - number of views to add/edit/delete
         action - can be create/update/delete
    '''
    def test_simple_dataset_query_during_modifying_other_ddoc(self):
        ddoc_num = self.input.param('num-ddocs-to-modify', 1)
        action = self.input.param('action', 'create')
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)
        data_set.add_startkey_endkey_queries()
        generator_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, generator_load)
        self._query_all_views(data_set.views, generator_load)

        #create ddoc
        view_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        ddoc_name = "ddoc_test"
        view = View(ddoc_name, view_map_func, None, False)

        tasks = []
        for i in range(ddoc_num):
            tasks.append(self.cluster.async_create_view(self.servers[0], ddoc_name + str(i), view))

        #update/delete
        if action in ['update', 'delete']:
            for task in tasks:
                task.result()
            tasks = []
            #update/delete
            if action == 'update':
                view_map_func_new = "function (doc) {if(doc.age !== undefined) { emit(doc.age, doc.name);}}"
                view = View(ddoc_name, view_map_func_new, None, True)
                for i in range(ddoc_num):
                    tasks.append(self.cluster.async_create_view(self.servers[0], ddoc_name + str(i), view))
            if action == 'delete':
                for i in range(ddoc_num):
                    prefix = ("", "dev_")[view.dev_view]
                    tasks.append(self.cluster.async_delete_view(self.servers[0], prefix + ddoc_name + str(i), None))

        self._query_all_views(data_set.views, generator_load)

        for task in tasks:
            task.result()

    '''
    Test verifies querying when other thread is updating/deleting its ddoc
    Parameters:
        action - can be create/update/delete
        error - expected error message for queries
    '''
    def test_simple_dataset_query_during_modifying_its_ddoc(self):
        action = self.input.param('action', 'update')
        error = self.input.param('error', None)
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)
        data_set.add_startkey_endkey_queries()
        generator_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, generator_load)
        self._query_all_views(data_set.views, generator_load)

        tasks = []
        #update/delete
        if action == 'update':
            view_map_func_new = "function (doc) {if(doc.age !== undefined) { emit(doc.age);}}"
            views = [View(view.name, view_map_func_new, None, False) for view in data_set.views]
            for view in views:
                tasks.append(self.cluster.async_create_view(self.servers[0], view.name, view))
                self._query_all_views(data_set.views, generator_load)
        if action == 'delete':
            for view in data_set.views:
                tasks.append(self.cluster.async_delete_view(self.servers[0], view.name, None))
                time.sleep(1)
            for view in data_set.views:
                for q in view.queries:
                    q.error = error
            self._query_all_views(data_set.views, generator_load)
        for task in tasks:
            task.result()

    def test_sales_dataset_query_reduce(self):
        '''
        Test uses sales data set:
            -documents are structured as {
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "sales" : sales <int>}
        Steps to repro:
            1. Start load data
            2. start querying for views with reduce
        '''
        params = self.input.param('query_params', {})
        use_custom_reduce = self.input.param('use_custom_reduce', False)
        data_set = SalesDataSet(self.master, self.cluster, self.docs_per_day, custom_reduce=use_custom_reduce)
        data_set.add_reduce_queries(params, limit=self.limit)
        self._query_test_init(data_set)

    def test_sales_dataset_skip_query_datatypes(self):
        '''
        Test uses sales data set:
            -documents are structured as {
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "sales" : sales <int>
                                       "delivery_date" : string for date),
                                        "is_support_included" : boolean,
                                        "client_name" : string,
                                        "client_reclaims_rate" : float}}
        Steps to repro:
            1. Start load data
            2. start querying for views with reduce
        '''
        skip = self.input.param('skip', 2000)
        data_set = SalesDataSet(self.master, self.cluster, self.docs_per_day)
        data_set.add_skip_queries(skip=skip, limit=self.limit)
        generator_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, generator_load)
        if self.wait_persistence:
            for server in self.servers:
                RebalanceHelper.wait_for_persistence(server, data_set.bucket)
        self._query_all_views(data_set.views, generator_load)

    def test_sales_dataset_start_end_key_query_datatypes(self):
        '''
        Test uses sales data set:
            -documents are structured as {
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "sales" : sales <int>
                                       "delivery_date" : string for date),
                                        "is_support_included" : boolean,
                                        "client_name" : string,
                                        "client_reclaims_rate" : float}}
        Steps to repro:
            1. Start load data
            2. start querying for views
        '''
        data_set = SalesDataSet(self.master, self.cluster, self.docs_per_day, test_datatype=True)
        data_set.add_startkey_endkey_queries(limit=self.limit)
        generator_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, generator_load)
        if self.wait_persistence:
            for server in self.servers:
                RebalanceHelper.wait_for_persistence(server, data_set.bucket.name)
        self._query_all_views(data_set.views, generator_load)

    def test_sales_dataset_multiply_items(self):
        '''
        Test uses sales data set:
            -documents are structured as {
                                       "join_yr" : year<int>,
                                       "join_mo" : month<int>,
                                       "join_day" : day<int>,
                                       "sales" : sales <int>
                                       <n> string items}}
        Steps to repro:
            1. Start load data
            2. start querying for views
        '''
        skip = self.input.param('skip', 2000)
        num_items = self.input.param('num_items', 55)
        data_set = SalesDataSet(self.master, self.cluster, self.docs_per_day, template_items_num=num_items)
        data_set.add_skip_queries(skip=skip, limit=self.limit)
        generator_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, generator_load)
        self._query_all_views(data_set.views, generator_load)

    def test_sales_dataset_group_queries(self):
        '''
        Steps to repro:
            1. Start load data
            2. simultaneously start queries with group and group_level params
        '''
        data_set = SalesDataSet(self.master, self.cluster, self.docs_per_day)

        data_set.add_group_queries(limit=self.limit)
        self._query_test_init(data_set)

    def test_sales_dataset_startkey_endkey_docid_queries(self):
        '''
        Steps to repro:
            1. Start load data
            2. simultaneously run queries (staartkey endkey startkey_docid endkey_docid
            inclusive_end, descending combinations)
            '''
        data_set = SalesDataSet(self.master, self.cluster, self.docs_per_day)

        data_set.add_startkey_endkey_docid_queries(limit=self.limit)
        self._query_test_init(data_set)

    def test_sales_dataset_stale_queries(self):
        '''
        Steps to repro:
            1. Start load data
            2. start stale queries(ok, update_after, false)
        '''
        data_set = SalesDataSet(self.master, self.cluster, self.docs_per_day)

        data_set.add_stale_queries()
        self._query_test_init(data_set)

    def test_sales_dataset_all_queries(self):
        '''
        Steps to repro:
            1. Start load data
            2. start queries: stale, group, starkey/endkey, stratkey_docid
        '''
        data_set = SalesDataSet(self.master, self.cluster, self.docs_per_day)
        data_set.add_all_query_sets()
        if self.wait_persistence:
            gen_load = data_set.generate_docs(data_set.views[0])
            self.load(data_set, gen_load)
            for server in self.servers:
                RebalanceHelper.wait_for_persistence(server, data_set.bucket)
            self._query_all_views(data_set.views, gen_load)
        else:
            self._query_test_init(data_set)

    def test_sales_dataset_skip_queries(self):
        '''
        Steps to repro:
            1. Start load data
            2. start queries with skip
        '''
        skip = self.input.param('skip', 200)
        data_set = SalesDataSet(self.master, self.cluster, self.docs_per_day)

        data_set.add_skip_queries(skip)
        self._query_test_init(data_set)


    def test_expiration_docs_queries(self):
        data_set = ExpirationDataSet(self.master, self.cluster, self.num_docs)
        generator_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, generator_load, exp=data_set.expire)
        for server in self.servers:
            RebalanceHelper.wait_for_persistence(server, data_set.bucket)
        data_set.set_expiration_time()
        data_set.query_verify_value(self)

    def test_flags_docs_queries(self):
        data_set = FlagsDataSet(self.master, self.cluster, self.num_docs)
        generator_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, generator_load, flag=data_set.item_flag)
        print(data_set.item_flag)
        for server in self.servers:
            RebalanceHelper.wait_for_persistence(server, data_set.bucket)
        data_set.query_verify_value(self)

    def test_boudary_rebalance_queries(self):
        '''
        Test uses simple data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            http://hub.internal.couchbase.com/confluence/display/QA/QE+Test+Enhancements+for+2.0.2
            Presetup: 1 bucket, 1 node
            10 production ddocs, 1 view per ddoc(different map functions with and without reduce function)
            Start load/mutate items, 10M items
            Add to cluster 2 nodes and start rebalance
            Run queries with startkey/endkey for views without reduce and group/group_level for views with reduce fn
        '''
        views = [QueryView(self.master, self.cluster,
                           fn_str='function (doc) {if(doc.age !== undefined) { emit(doc.age, [doc.name,doc.age]);}}'),
                 QueryView(self.master, self.cluster,
                           fn_str='function (doc) {if(doc.age !== undefined) { emit(doc.name, doc.age);}}'),
                 QueryView(self.master, self.cluster,
                           fn_str='function (doc) {if(doc.age !== undefined) { emit(doc.name, "value");}}'),
                 QueryView(self.master, self.cluster,
                           fn_str='function (doc) {if(doc.age !== undefined) { emit(doc.name, 100);}}'),
                 QueryView(self.master, self.cluster,
                           fn_str='function (doc) {if(doc.age !== undefined) { emit(doc.age, "value");}}'),
                 QueryView(self.master, self.cluster,
                           fn_str='function (doc) {if(doc.age !== undefined) { emit(doc.age, 100);}}'),
                 QueryView(self.master, self.cluster,
                           fn_str='function (doc) {if(doc.age !== undefined) { emit(doc.name, doc.age);}}',
                          reduce_fn='_stats'),
                 QueryView(self.master, self.cluster,
                           fn_str='function (doc) {if(doc.age !== undefined) { emit(doc.name, doc.age);}}',
                          reduce_fn='_sum'),
                 QueryView(self.master, self.cluster,
                           fn_str='function (doc) {if(doc.age !== undefined) { emit(doc.name, doc.age);}}',
                          reduce_fn='_count')]
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)
        data_set.views.extend(views)
        generator_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, generator_load)
        data_set.add_startkey_endkey_queries(limit=self.limit)
        data_set.add_group_queries(limit=self.limit)
        rebalance = self.cluster.async_rebalance([self.servers[0]], self.servers[1:self.nodes_in + 1], [])
        self._query_all_views(data_set.views, generator_load)
        rebalance.result()

    def test_boudary_failover_queries(self):
        '''
        Test uses simple data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            http://hub.internal.couchbase.com/confluence/display/QA/QE+Test+Enhancements+for+2.0.2
            Presetup: 5 nodes, 1 bucket with 2 replica
            10 production ddocs, 1 view per ddoc(different map functions with and without reduce function), put minChangesUpdate param to 6M
            Load 5M items
            Perform queries with stale=false param for each view
            Stop server on 2 nodes, failover both, add back one of them, start rebalance
            Run queries with stale=false param
        '''
        options = {"updateMinChanges" : self.docs_per_day * 1009,
                   "replicaUpdateMinChanges" : self.docs_per_day * 1009}
        views = [QueryView(self.master, self.cluster,
                           fn_str='function (doc) { if(doc.job_title !== undefined) emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, 100] ); }'),
                 QueryView(self.master, self.cluster,
                           fn_str='function (doc) { if(doc.job_title !== undefined) emit([doc.join_yr, doc.join_mo, doc.join_day], ["name", doc.name] ); }'),
                 QueryView(self.master, self.cluster,
                           fn_str='function (doc) { if(doc.job_title !== undefined) emit([doc.join_yr, doc.join_mo, doc.join_day], ["email", doc.email] ); }'),
                 QueryView(self.master, self.cluster,
                           fn_str='function (doc) { if(doc.job_title !== undefined) emit([doc.join_yr, doc.join_mo, doc.join_day], doc.join_day); }',
                          reduce_fn='_stats'),
                 QueryView(self.master, self.cluster,
                           fn_str='function (doc) { if(doc.job_title !== undefined) emit([doc.join_yr, doc.join_mo, doc.join_day], doc.join_day); }',
                          reduce_fn='_sum'),
                 QueryView(self.master, self.cluster,
                           fn_str='function (doc) { if(doc.job_title !== undefined) emit([doc.join_yr, doc.join_mo, doc.join_day], doc.join_day); }',
                          reduce_fn='_count')]
        failover_factor = self.input.param("failover-factor", 1)
        failover_nodes = self.servers[1 : failover_factor + 1]
        data_set = EmployeeDataSet(self.master, self.cluster, self.docs_per_day, ddoc_options=options)
        data_set.views.extend(views)
        generator_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, generator_load)
        data_set.add_stale_queries(stale_param="false", limit=self.limit)

        nodes_all = self._rconn().node_statuses()
        nodes = []
        for failover_node in failover_nodes:
            nodes.extend([node for node in nodes_all
                if node.ip != failover_node.ip or str(node.port) != failover_node.port])
        self.cluster.failover(self.servers, failover_nodes)
        for node in nodes:
            self._rconn().add_back_node(node.id)
        rebalance = self.cluster.async_rebalance(self.servers, [], [])
        self._query_all_views(data_set.views, generator_load, retries=200)
        rebalance.result()

    def test_MB_7978_reproducer(self):
        '''
        MB-7978 reproducer
            Certain combinations of views in the same design document
            with the same map function and different reduce functions are not working
        Test uses simple data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. load data
            2. start querying with stale=false
        '''
        new_views = [QueryView(self.master, self.cluster,
                               fn_str='function (doc) {if(doc.age !== undefined) { emit(doc.name, doc.age);}}',
                               reduce_fn='_count',
                               name='test_view_0', ddoc_name='test_ddoc'),
                     QueryView(self.master, self.cluster,
                               fn_str='function (doc) {if(doc.age !== undefined) { emit(doc.name, doc.age);}}',
                               reduce_fn='function(key, values, rereduce) { if (rereduce) {var s = 0;for (var i = 0; i < values.length; ++i) {s += Number(values[i]);} return String(s);} return String(values.length);}',
                               name='test_view_1', ddoc_name='test_ddoc'),
                     QueryView(self.master, self.cluster,
                               fn_str='function (doc) {if(doc.age !== undefined) { emit([doc.name, doc.age], doc.age);}}',
                               reduce_fn='_sum',
                               name='test_view_2', ddoc_name='test_ddoc'),
                     QueryView(self.master, self.cluster,
                               fn_str='function (doc) {if(doc.age !== undefined) { emit([doc.name, doc.age], doc.age);}}',
                               reduce_fn='function(key, values, rereduce) { if (rereduce) return sum(values); return -values.length;}',
                               name='test_view_3', ddoc_name='test_ddoc'),
                     QueryView(self.master, self.cluster,
                               fn_str='function (doc) {if(doc.age !== undefined) { emit([doc.name, doc.age], doc.age);}}',
                               reduce_fn='_count',
                               name='test_view_4', ddoc_name='test_ddoc')]
        # init dataset for test
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)
        data_set.views.extend(new_views)
        data_set.add_stale_queries(stale_param="false", limit=self.limit)
        #  generate items
        generator_load = data_set.generate_docs(data_set.views[0], start=0,
                                                end=self.num_docs)
        self.load(data_set, generator_load)

        self._query_all_views(data_set.views, generator_load,
                              verify_expected_keys=True)

    def test_add_MB_7764_reproducer(self):
        data_set = SalesDataSet(self.master, self.cluster, self.docs_per_day, custom_reduce=True)
        data_set.add_reduce_queries({'reduce' :'true'})
        generator_load = data_set.generate_docs(data_set.views[0])
        self.load(data_set, generator_load)
        self._query_all_views(data_set.views, generator_load)

    def test_concurrent_threads(self):
        num_ddocs = self.input.param("num_ddocs", 8)
        #threads per view
        num_threads = self.input.param("threads", 11)

        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)
        generator_load = data_set.generate_docs(data_set.views[0], start=0,
                                                end=self.num_docs)
        for ddoc_index in range(num_ddocs):
            view_fn = 'function (doc) {if(doc.age !== undefined) { emit(doc.age, "value_%s");}}' % ddoc_index
            data_set.views.append(QueryView(self.master, self.cluster, fn_str=view_fn))
        data_set.add_stale_queries(stale_param="false", limit=self.limit)
        self.load(data_set, generator_load)

        query_threads = []
        for t_index in range(num_threads):
            t = StoppableThread(target=self._query_all_views,
                                name="query-{0}".format(t_index),
                                args=(data_set.views, generator_load, None,
                                False, [], 1))
            query_threads.append(t)
            t.start()

        while True:
            if not query_threads:
                break
            self.thread_stopped.wait(60)
            if self.thread_crashed.is_set():
                for t in query_threads:
                    t.stopit()
                break
            else:
                query_threads = [d for d in query_threads if d.is_alive()]
                self.thread_stopped.clear()

    '''
    test changes ram quota during index.
    http://www.couchbase.com/issues/browse/CBQE-1649
    '''
    def test_index_in_with_cluster_ramquota_change(self):
        data_set = SimpleDataSet(self.master, self.cluster, self.num_docs)
        data_set.add_stale_queries(stale_param="false", limit=self.limit)
        generator_load = data_set.generate_docs(data_set.views[0], end=self.num_docs)
        self.load(data_set, generator_load)

        for view in data_set.views:
            self.cluster.query_view(self.master, view.name, view.name,
                                    {'stale' : 'false', 'limit' : 1000})
        remote = RemoteMachineShellConnection(self.master)
        cli_command = "setting-cluster"
        options = "--cluster-ramsize=%s" % (self.quota + 10)
        output, error = remote.execute_couchbase_cli(cli_command=cli_command, options=options, cluster_host="localhost",
                                                     user=self.master.rest_username, password=self.master.rest_password)
        self.assertTrue('\n'.join(output).find('SUCCESS') != -1, 'ram wasn\'t changed')
        self.log.info('Quota changed')
        gen_query = copy.deepcopy(generator_load)
        self._query_all_views(data_set.views, gen_query, verify_expected_keys=True)


    ###
    # load the data defined for this dataset.
    # create views and query the data as it loads.
    # verification is optional, and best practice is to
    # set to False if you plan on running _query_all_views()
    # later in the test case
    ###
    def _query_test_init(self, data_set, verify_results=False):
        try:
            views = data_set.views
            rest = self._rconn()
            load_task = None

            generators = data_set.generate_docs(views[0])
            load_task = StoppableThread(target=self.load,
                                       name="load_data_set",
                                       args=(data_set, generators))
            load_task.start()

            # run queries while loading data
            self._query_all_views(views, generators, data_set.kv_store,
                                  verify_expected_keys=verify_results,
                                  threads=[load_task])
            if 'result' in dir(load_task):
                load_task.result()
            else:
                load_task.join()

            self._check_view_intergrity(views)
            return generators
        finally:
            data_set.cluster.shutdown()

    def load(self, data_set, generators_load, exp=0, flag=0,
             kv_store=1, only_store_hash=True, batch_size=1, pause_secs=1,
             timeout_secs=30, op_type='create'):
        try:
            gens_load = []
            for generator_load in generators_load:
                gens_load.append(copy.deepcopy(generator_load))
            task = None
            bucket = data_set.bucket
            if not isinstance(bucket, Bucket):
                bucket = Bucket(name=bucket, authType="sasl", saslPassword="")

            # initialize the template for document generator
            items = 0
            for gen_load in gens_load:
                items += (gen_load.end - gen_load.start)
            self.log.info("%s %s documents..." % (op_type, items))
            task = self.cluster.async_load_gen_docs(data_set.server, bucket.name, gens_load,
                                                 bucket.kvs[kv_store], op_type, exp, flag,
                                                 only_store_hash, batch_size, pause_secs,
                                                 timeout_secs, compression=self.sdk_compression)
            if 'stop' in dir(threading.currentThread()) and\
                isinstance(threading.currentThread(), StoppableThread):
                threading.currentThread().tasks.append(task)
            task.result()
            RebalanceHelper.wait_for_persistence(data_set.server, bucket.name)
            self.log.info("LOAD IS FINISHED")
            return gens_load
        except Exception as ex:
            data_set.views[0].results.addError(self, (Exception, str(ex), sys.exc_info()[2]))
            self.log.error("At least one of load data threads is crashed: {0}".format(ex))
            self.thread_crashed.set()
            if task:
                if 'stop' in dir(threading.currentThread()) and\
                   isinstance(threading.currentThread(), StoppableThread):
                    threading.currentThread().tasks.append(task)
                    threading.currentThread().stopit()
                else:
                    task.cancel()
            raise ex
        finally:
            if not self.thread_stopped.is_set():
                self.thread_stopped.set()

    ##
    # run all queries for all views in parallel
    ##
    def _query_all_views(self, views, generators, kv_store=None,
                         verify_expected_keys=False, threads=[], retries=100,
                         server_to_query=0):

        query_threads = []
        for view in views:
            t = StoppableThread(target=view.run_queries,
               name="query-{0}".format(view.name),
               args=(self, kv_store, verify_expected_keys, generators, retries,
                     server_to_query))
            query_threads.append(t)
            t.start()

        if self.timeout:
            end_time = time.time() + float(self.timeout)

        while True:
            if self.timeout:
                if time.time() > end_time:
                    raise Exception("Test failed to finish in %s seconds" % self.timeout)

            if not query_threads:
                return
            self.thread_stopped.wait(60)
            if self.thread_crashed.is_set():
                self.log.error("Will stop all threads!")
                for t in threads:
                    t.stopit()
                    self.log.error("Thread %s stopped" % str(t))
                for t in query_threads:
                    t.stopit()
                    self.log.error("Thread %s stopped" % str(t))
                self._check_view_intergrity(views)
                return
            else:
                query_threads = [d for d in query_threads if d.is_alive()]
                self.log.info("Current amount of threads %s" % len(query_threads))
                self.thread_stopped.clear()

        self._check_view_intergrity(views)

    ##
    # if an error occured loading or querying data for a view
    # it is queued and checked here
    ##
    def _check_view_intergrity(self, views):
        for view in views:
            if view.results.failures or view.results.errors:
                failures = view.results.failures
                failures += view.results.errors
                self.fail(self._form_report_failure(failures, views))

    def _form_report_failure(self, errors, views):
        #TODO
        report = ''
        for ex in errors:
            views_str = ['%s : map_fn=%s, reduce_fn=%s' %
                         (view.name, view.fn_str, view.reduce_fn) for view in views]
            view_struct = 'Views : %s' % views_str
            msg = "\n****************** Error report *********************\n"
            msg += "Failure message is: %s\nTest case info:\n%s\nViews structure are:\n%s\n\n" % (
                                ex[1], getattr(self, self._testMethodName).__doc__, view_struct)
            report += msg
        return report

    # retrieve default rest connection associated with the master server
    def _rconn(self, server=None):
        if not server:
            server = self.master
        return RestConnection(server)

    @staticmethod
    def parse_string_to_dict(string_to_parse, separator_items=';', seprator_value='-'):
        if string_to_parse.find(separator_items) < 0:
            return dict([string_to_parse.split(seprator_value)])
        else:
            return dict(item.split(seprator_value) for item in string_to_parse.split(separator_items))

class QueryView:
    def __init__(self, server,
                 cluster,
                 bucket="default",
                 prefix=None,
                 name=None,
                 fn_str=None,
                 reduce_fn=None,
                 queries=None,
                 create_on_init=True,
                 type_filter=None,
                 consistent_view=False,
                 ddoc_options=None,
                 wait_timeout=60,
                 is_dev_view=False,
                 ddoc_name=None):
        self.cluster = cluster
        default_prefix = str(uuid.uuid4())[:7]
        self.prefix = (prefix, default_prefix)[prefix is None]
        default_name = "test_view-{0}".format(self.prefix)

        self.log = logger.Logger.get_logger()
        default_fn_str = 'function (doc) {if(doc.name) { emit(doc.name, doc);}}'

        self.bucket = bucket
        self.name = (name, default_name)[name is None]
        self.fn_str = (fn_str, default_fn_str)[fn_str is None]
        self.reduce_fn = reduce_fn
        self.results = unittest.TestResult()
        self.type_filter = type_filter or None
        self.consisent_view = consistent_view
        self.wait_timeout = wait_timeout
        self.server = server
        self.is_dev_view = is_dev_view
        self.view = View(self.name, self.fn_str, self.reduce_fn, dev_view=is_dev_view)
        # queries defined for this view
        self.queries = (queries, list())[queries is None]
        self.ddoc_name = ddoc_name or self.name
        if create_on_init:
            task = self.cluster.async_create_view(self.server, self.ddoc_name, self.view,
                                                 bucket, ddoc_options=ddoc_options)
            task.result()

    def __str__(self):
        return "DDoc=%s; View=%s;" % (self.ddoc_name, str(self.view))

    def run_queries(self, tc, kv_store=None,
                    verify_expected_keys=False, doc_gens=None,
                    retries=100, server_to_query=0):
        if tc.retries:
            retries = tc.retries
        task = None
        try:
            for query in self.queries:
                tc.log.info("%s: query=%s: Generate results" % (self, query))
                if query.error:
                    expected_results = []
                else:
                    params_gen_results = copy.deepcopy(query.params)
                    if query.non_json:
                        params_gen_results = {}
                        for key, value in query.params.items():
                            params_gen_results[key] = value.replace('"', '')
                    task = tc.cluster.async_generate_expected_view_results(
                        doc_gens, self.view, params_gen_results)
                    if 'stop' in dir(threading.currentThread()) and\
                   isinstance(threading.currentThread(), StoppableThread):
                        threading.currentThread().tasks.append(task)
                    expected_results = task.result()
                tc.log.info("%s:%s: Generated results contains %s items" % (
                                                                self, query, len(expected_results)))
                prefix = ("", "dev_")[self.is_dev_view]
                task = tc.cluster.async_monitor_view_query(
                    tc.servers, self.ddoc_name, self.view,
                    query.params, expected_docs=expected_results,
                    bucket=self.bucket, retries=retries,
                    error=query.error, verify_rows=verify_expected_keys,
                    server_to_query=server_to_query)
                if 'stop' in dir(threading.currentThread()) and\
                   isinstance(threading.currentThread(), StoppableThread):
                    if threading.currentThread().tasks:
                        threading.currentThread().tasks[0] = task
                    else:
                        threading.currentThread().tasks.append(task)
                result_query = task.result()

                if not result_query["passed"]:
                    msg = ''
                    if result_query["errors"]:
                        msg = "%s:%s: ERROR: %s" % (self, query, result_query["errors"])
                        self.log.error(msg)
                    if 'results' in result_query and result_query["results"]:
                        task = tc.cluster.async_view_query_verification(
                                                   self.ddoc_name, self.view.name,
                                                   query.params, expected_results,
                                                   server=tc.servers[0],
                                                   num_verified_docs=len(expected_results),
                                                   bucket=self.bucket,
                                                   results=result_query["results"])
                        if 'stop' in dir(threading.currentThread()) and\
                   isinstance(threading.currentThread(), StoppableThread):
                            threading.currentThread().tasks[0] = task
                        debug_info = task.result()
                        msg += "DEBUG INFO: %s" % debug_info["errors"]
                    self.results.addFailure(tc, (Exception, msg, sys.exc_info()[2]))
                    tc.thread_crashed.set()
        except Exception as ex:
            self.log.error("Error {0} appeared during query run".format(ex))
            self.results.addError(tc, (Exception, str(ex), sys.exc_info()[2]))
            tc.thread_crashed.set()
            if task:
                if 'stop' in dir(threading.currentThread()) and\
                   isinstance(threading.currentThread(), StoppableThread):
                    threading.currentThread().tasks.append(task)
                    threading.currentThread().stop()
                else:
                    task.cancel()
            raise ex
        finally:
            if not tc.thread_stopped.is_set():
                tc.thread_stopped.set()

class EmployeeDataSet:
    def __init__(self, server, cluster, docs_per_day=200, bucket="default", ddoc_options=None):
        self.docs_per_day = docs_per_day
        self.years = 1
        self.months = 12
        self.days = 28
        self.sys_admin_info = {"title" : "System Administrator and heliport manager",
                              "desc" : "...Last but not least, as the heliport manager, you will help maintain our growing fleet of remote controlled helicopters, that crash often due to inexperienced pilots.  As an independent thinker, you may be free to replace some of the technologies we currently use with ones you feel are better. If so, you should be prepared to discuss and debate the pros and cons of suggested technologies with other stakeholders",
                              "type" : "admin"}
        self.ui_eng_info = {"title" : "UI Engineer",
                           "desc" : "Couchbase server UI is one of the crown jewels of our product, which makes the Couchbase NoSQL database easy to use and operate, reports statistics on real time across large clusters, and much more. As a Member of Couchbase Technical Staff, you will design and implement front-end software for cutting-edge distributed, scale-out data infrastructure software systems, which is a pillar for the growing cloud infrastructure.",
                            "type" : "ui"}
        self.senior_arch_info = {"title" : "Senior Architect",
                               "desc" : "As a Member of Technical Staff, Senior Architect, you will design and implement cutting-edge distributed, scale-out data infrastructure software systems, which is a pillar for the growing cloud infrastructure. More specifically, you will bring Unix systems and server tech kung-fu to the team.",
                               "type" : "arch"}
        self.server = server
        self.cluster = cluster
        self.bucket = bucket
        self.ddoc_options = ddoc_options
        self.views = self.create_views()
        self.name = "employee_dataset"
        self.kv_store = None

    def calc_total_doc_count(self):
        return self.years * self.months * self.days * self.docs_per_day * len(self.get_data_sets())

    def add_negative_query(self, query_params, error, views=None):
        views = views or self.views
        query_params_dict = ViewQueryTests.parse_string_to_dict(query_params, seprator_value='~')
        for view in views:
            view.queries += [QueryHelper(query_params_dict, error=error)]

    def add_query_invalid_startkey_endkey_docid(self, valid_query_params, invalid_query_params, views=None):
        views = views or self.views
        if valid_query_params:
            if valid_query_params.find(';') < 0:
                query_params_dict = dict([valid_query_params.split("-")])
            else:
                query_params_dict = dict(item.split("-") for item in valid_query_params.split(";"))
        if invalid_query_params.find(';') < 0:
            invalid_query_params_dict = dict([invalid_query_params.split("-")])
        else:
            invalid_query_params_dict = dict(item.split("-") for item in invalid_query_params.split(";"))

        for view in views:
            if query_params_dict:
                query_params_dict.update(invalid_query_params_dict)
            else:
                query_params_dict = invalid_query_params_dict
            view.queries += [QueryHelper(query_params_dict)]

    def add_startkey_endkey_docid_queries_extra_params(self, views=None, extra_params=None):
        # only select views that will index entire dataset
        views = views or self.views
        extra_params_dict = {}
        import types
        if extra_params and not isinstance(extra_params, dict):
            extra_params_dict = ViewQueryTests.parse_string_to_dict(extra_params)

        for view in views:
            if re.match(r'.*new RegExp\("\^.*', view.fn_str) is None:
                # pre-calculating expected key size of query between
                # [2008,2,20] and [2008,7,1] with descending set
                # based on dataset
                startkey = "[2008,2,20]"
                endkey = "[2008,7,1]"
                startkey_docid = "%s1" % self.views[0].name
                endkey_docid = "%sc" % self.views[0].name

                if 'descending' in extra_params_dict and \
                    extra_params_dict['descending'] == 'true':
                    tmp = endkey
                    endkey = startkey
                    startkey = tmp
                    tmp = endkey_docid
                    endkey_docid = startkey_docid
                    startkey_docid = tmp

                if 'skip' in extra_params_dict:
                    if view.reduce_fn:
                        continue

                view.queries += [QueryHelper({"startkey" : startkey,
                                              "startkey_docid" : startkey_docid,
                                              "endkey"   : endkey,
                                              "endkey_docid"   : endkey_docid})]
                if extra_params_dict:
                    for q in view.queries:
                        q.params.update(extra_params_dict)

    def add_startkey_endkey_queries(self, views=None, limit=None):
        if views is None:
            views = self.views

        for view in views:
            view.queries += [QueryHelper({"startkey" : "[2008,7,null]"}),
                             QueryHelper({"startkey" : "[2008,0,1]",
                                          "endkey"   : "[2008,7,1]",
                                          "inclusive_end" : "false"}),
                             QueryHelper({"startkey" : "[2008,0,1]",
                                          "endkey"   : "[2008,7,1]",
                                          "inclusive_end" : "true"}),
                             QueryHelper({"startkey" : "[2008,7,1]",
                                          "endkey"   : "[2008,1,1]",
                                          "descending"   : "true",
                                          "inclusive_end" : "false"}),
                             QueryHelper({"startkey" : "[2008,7,1]",
                                          "endkey"   : "[2008,1,1]",
                                          "descending"   : "true",
                                          "inclusive_end" : "true"}),
                             QueryHelper({"startkey" : "[2008,1,1]",
                                          "endkey"   : "[2008,7,1]",
                                          "descending"   : "false",
                                          "inclusive_end" : "false"}),
                             QueryHelper({"startkey" : "[2008,1,1]",
                                          "endkey"   : "[2008,7,1]",
                                          "descending"   : "false",
                                          "inclusive_end" : "true"})]
            if limit:
                for query in view.queries:
                    query.params["limit"] = limit

    def add_skip_queries(self, skip, limit=None, views=None):
        if views is None:
            views = self.views

        for view in views:
            #empty results will be returned
            if view.reduce_fn:
                views.remove(view)
            else:
                view.queries += [QueryHelper({"skip" : skip})]
            if limit:
                for q in view.queries:
                    q.params['limit'] = limit

    def add_key_queries(self, views=None, limit=None):
        if views is None:
            views = self.views
        for view in views:
            view.queries += [QueryHelper({"key" : "[2008,7,1]"})]
            if limit:
                for q in view.queries:
                    q.params['limit'] = limit


    """
        Create queries for testing docids on duplicate start_key result ids.
        Only pass in view that indexes all employee types as they are
        expected in the query params...i.e (ui,admin,arch)
    """
    def add_startkey_endkey_docid_queries(self, views=None, limit=None):
        if views is None:
            views = []

        for view in views:
            view.queries += [QueryHelper({"startkey" : "[2008,7,1]",
                                          "startkey_docid" : '"%s%s"' % (
                                                                  self.views[0].prefix,
                                                                  str(uuid.uuid4())[:1])}),
                                QueryHelper({"startkey" : "[2008,7,1]",
                                             "startkey_docid" : '"%s%s"' % (
                                                                  self.views[0].prefix,
                                                                  str(uuid.uuid4())[:1]),
                                             "descending" : "false"},),
                                 QueryHelper({"startkey" : "[2008,7,1]",
                                              "startkey_docid" : '"%s%s"' % (
                                                                  self.views[0].prefix,
                                                                  str(uuid.uuid4())[:1]),
                                              "descending" : "true"}),
                                QueryHelper({"startkey" : "[2008,7,1]",
                                             "startkey_docid" : '"%s%s"' % (
                                                                  self.views[0].prefix,
                                                                  str(uuid.uuid4())[:1])}),
                                QueryHelper({"startkey" : "[2008,7,1]",
                                             "startkey_docid" : "ui0000-2008_07_01",
                                             "descending" : "false"}),
                                 QueryHelper({"startkey" : "[2008,7,1]",
                                             "startkey_docid" : '"%s%s"' % (
                                                                  self.views[0].prefix,
                                                                  str(uuid.uuid4())[:1]),
                                             "descending" : "true"}),
                                              # +endkey_docid
                                QueryHelper({"startkey" : "[2008,0,1]",
                                             "endkey"   : "[2008,7,1]",
                                             "endkey_docid" : '"%s%s"' % (
                                                                  self.views[0].prefix,
                                                                  str(uuid.uuid4())[:1]),
                                             "inclusive_end" : "false"}),
                                QueryHelper({"endkey"   : "[2008,7,1]",
                                             "endkey_docid" : '"%s%s"' % (
                                                                  self.views[0].prefix,
                                                                  str(uuid.uuid4())[:1]),
                                             "inclusive_end" : "false"}),
                                              # + inclusive_end
                                QueryHelper({"endkey"   : "[2008,7,1]",
                                             "endkey_docid" : '"%s%s"' % (
                                                                  self.views[0].prefix,
                                                                  str(uuid.uuid4())[:1]),
                                             "inclusive_end" : "true"}),
                                              # + single bounded and descending
                                QueryHelper({"startkey" : "[2008,7,1]",
                                             "endkey"   : "[2008,2,20]",
                                             "endkey_docid"   : '"%s%s"' % (
                                                                  self.views[0].prefix,
                                                                  str(uuid.uuid4())[:1]),
                                             "descending"   : "true",
                                             "inclusive_end" : "true"}),
                                QueryHelper({"startkey" : "[2008,7,1]",
                                             "endkey"   : "[2008,2,20]",
                                             "endkey_docid"   : '"%s%s"' % (
                                                                  self.views[0].prefix,
                                                                  str(uuid.uuid4())[:1]),
                                             "descending"   : "true",
                                             "inclusive_end" : "false"}),
                                              # + double bounded and descending
                                QueryHelper({"startkey" : "[2008,7,1]",
                                             "startkey_docid" : '"%s%s"' % (
                                                                  self.views[0].prefix,
                                                                  str(uuid.uuid4())[:1]),
                                             "endkey"   : "[2008,2,20]",
                                             "endkey_docid"   : '"%s%s"' % (
                                                                  self.views[0].prefix,
                                                                  str(uuid.uuid4())[:1]),
                                             "descending"   : "true",
                                             "inclusive_end" : "false"})]
            if limit:
                for q in view.queries:
                    q.params["limit"] = limit

    def add_stale_queries(self, views=None, limit=None, stale_param=None):
        if views is None:
            views = self.views

        for view in views:
            if stale_param:
                view.queries += [QueryHelper({"stale" :  stale_param})]
            else:
                view.queries += [QueryHelper({"stale" : "false"}),
                                 QueryHelper({"stale" : "ok"}),
                                 QueryHelper({"stale" : "update_after"})]
            if limit:
                for q in view.queries:
                    q.params['limit'] = limit

    """
        group queries should only be added to views with reduce views.
        in this particular case, _count function is expected.
        if no specific views are passed in, we'll just figure it out.

        verification requries that the expected number of groups generated
        by the query is provided.  each group will generate a value that
        when summed, should add up to the number of indexed docs
    """
    def add_group_count_queries(self, views=None, limit=None):

        for view in self.views:
            if view.reduce_fn is None:
                continue
            view.queries += [QueryHelper({"group" : "true"}),
                             QueryHelper({"group_level" : "1"}),
                             QueryHelper({"group_level" : "2"}),
                             QueryHelper({"group_level" : "3"})]
            if limit:
                for q in view.queries:
                    q.params['limit'] = limit

    def add_all_query_sets(self, views=None, limit=None):
        self.add_stale_queries(views, limit)
        self.add_startkey_endkey_queries(views, limit)
        self.add_startkey_endkey_docid_queries(views, limit)
        self.add_group_count_queries(views)

    # views for this dataset
    def create_views(self):
        vfn1 = 'function (doc) { if(doc.job_title !== undefined) { var myregexp = new RegExp("^UI "); if(doc.job_title.match(myregexp)){ emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}}'
        vfn2 = 'function (doc) { if(doc.job_title !== undefined) { var myregexp = new RegExp("^System "); if(doc.job_title.match(myregexp)){ emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}}'
        vfn3 = 'function (doc) { if(doc.job_title !== undefined) { var myregexp = new RegExp("^Senior "); if(doc.job_title.match(myregexp)){ emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}}'
        vfn4 = 'function (doc) { if(doc.job_title !== undefined) emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] ); }'
        vfn5 = 'function (doc, meta) { if(doc.job_title !== undefined) { var myregexp = new RegExp("^System "); if(doc.job_title.match(myregexp)) { emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}}'

        return [QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn4,
                          ddoc_options=self.ddoc_options),
                QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn1,
                          ddoc_options=self.ddoc_options),
                QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn2,
                          ddoc_options=self.ddoc_options),
                QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn3,
                          ddoc_options=self.ddoc_options),
                QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn4, reduce_fn="_count",
                          ddoc_options=self.ddoc_options),
                QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn5,
                          ddoc_options=self.ddoc_options)]

    def get_data_sets(self):
        return [self.sys_admin_info, self.ui_eng_info, self.senior_arch_info]

    def generate_docs(self, view, start=0, end=None):
        generators = []
        if end is None:
            end = self.docs_per_day
        join_yr = list(range(2008, 2008 + self.years))
        join_mo = list(range(1, self.months + 1))
        join_day = list(range(1, self.days + 1))
        name = ["employee-%s-%s" % (view.prefix, str(i)) for i in range(start, end)]
        email = ["%s-mail@couchbase.com" % str(i) for i in range(start, end)]
        template = '{{ "name":"{0}", "join_yr":{1}, "join_mo":{2}, "join_day":{3},'
        template += ' "email":"{4}", "job_title":"{5}", "type":"{6}", "desc":"{7}"}}'
        for info in self.get_data_sets():
            for year in join_yr:
                for month in join_mo:
                    for day in join_day:
                        prefix = str(uuid.uuid4())[:7]
                        generators.append(DocumentGenerator(view.prefix + prefix,
                                               template,
                                               name, [year], [month], [day],
                                               email, [info["title"]],
                                               [info["type"]], [info["desc"]],
                                               start=start, end=end))
        return generators

class SimpleDataSet:
    def __init__(self, server, cluster, num_docs, reduce_fn=None, bucket='default',
                 name_ddoc=None, json_case=False, ddoc_options=None):
        self.num_docs = num_docs
        self.name = name_ddoc
        self.json_case = json_case
        self.server = server
        self.kv_store = ClientKeyValueStore()
        self.reduce_fn = reduce_fn
        self.bucket = bucket
        self.cluster = cluster
        self.ddoc_options = ddoc_options
        self.views = self.create_views()

    def create_views(self):
        view_fn = 'function (doc) {if(doc.age !== undefined) { emit(doc.age, doc.name);}}'
        if self.json_case:
            view_fn = 'function (doc) {if(doc.age !== undefined) { emit(doc._id, doc.age);}}'
        return [QueryView(self.server, self.cluster, fn_str=view_fn,
                          reduce_fn=self.reduce_fn, name=self.name,
                          ddoc_options=self.ddoc_options)]

    def generate_docs(self, view, start=0, end=None):
        if end is None:
            end = self.num_docs
        age = list(range(start, end))
        name = [view.prefix + '-' + str(i) for i in range(start, end)]
        template = '{{ "age": {0}, "name": "{1}" }}'

        gen_load = DocumentGenerator(view.prefix, template, age, name, start=start,
                                     end=end)
        return [gen_load]

    def get_updates_num(self, view):
        rest = RestConnection(self.server)
        _, stat = rest.set_view_info(view.bucket, view.name)
        return stat['stats']['full_updates']

    def is_index_triggered_or_ran(self, view, index_type):
        rest = RestConnection(self.server)
        is_running = rest.is_index_triggered(view.name, index_type)
        if index_type == 'main':
            return is_running or self.get_updates_num(view) >= 1
        else:
            content = rest.query_view(view.name, view.name, view.bucket, {"stale" : "ok",
                                                                "_type" : "replica"})
            if not 'rows' in content:
                raise Exception("Query doesn't contain 'rows': %s" % content)
            return is_running or len(content['rows']) > 0

    def get_partition_seq(self, view):
        rest = RestConnection(self.server)
        _, stat = rest.set_view_info(view.bucket, view.name)
        partition_seq = 0
        for value in stat['partition_seqs'].values():
                    partition_seq += value
        return partition_seq
    
    def get_replica_partition_seq(self, view):
        rest = RestConnection(self.server)
        _, stat = rest.set_view_info(view.bucket, view.name)
        replica_partition_seq = 0
        for value in stat['replica_group_info']['partition_seqs'].values():
                    replica_partition_seq += value
        return replica_partition_seq

    def add_negative_query(self, query_params, error, views=None):
        views = views or self.views
        query_params_dict = ViewQueryTests.parse_string_to_dict(query_params, seprator_value='~')
        for view in views:
            view.queries += [QueryHelper(query_params_dict, error=error)]

    def add_skip_queries(self, skip, limit=None, views=None):
        if views is None:
            views = self.views

        for view in views:
            view.queries += [QueryHelper({"skip" : skip})]
            if limit is not None:
                    for q in view.queries:
                        q.params['limit'] = limit

    def add_startkey_endkey_queries(self, views=None, limit=None):

        if views is None:
            views = self.views

        for view in views:
            if view.reduce_fn:
                continue
            start_key = self.num_docs // 2
            end_key = self.num_docs - 2

            view.queries += [QueryHelper({"startkey" : end_key,
                                             "endkey" : start_key,
                                             "descending" : "true"}),
                                 QueryHelper({"startkey" : end_key,
                                             "endkey" : start_key,
                                             "descending" : "true"}),
                                 QueryHelper({"endkey" : end_key}),
                                 QueryHelper({"endkey" : end_key,
                                             "inclusive_end" : "false"}),
                                 QueryHelper({"startkey" : start_key})]
            if limit is not None:
                for q in view.queries:
                    q.params['limit'] = limit

    def add_startkey_endkey_non_json_queries(self, symbol, views=None,
                                             limit=None):
        if views is None:
            views = self.views

        for view in views:
            view.queries = list()
            start_key = '"%s-%s"' % (views[0].prefix, str(self.num_docs // 2))
            end_key = '"%s-%s%s"' % (views[0].prefix, str(self.num_docs - 2), symbol)

            view.queries += [QueryHelper({"startkey" : end_key,
                                           "endkey" : start_key,
                                           "descending" : "true"}, non_json=True),
                             QueryHelper({"endkey" : end_key}, non_json=True),
                             QueryHelper({"endkey" : end_key,
                                          "inclusive_end" : "false"}, non_json=True),
                             QueryHelper({"startkey" : start_key}, non_json=True)]
            if limit:
                for q in view.queries:
                    q.params["limit"] = limit

    def add_stale_queries(self, views=None, limit=None, stale_param=None):
        if views is None:
            views = self.views

        for view in views:

            if stale_param:
                view.queries += [QueryHelper({"stale" :  stale_param}), ]
            else:
                view.queries += [QueryHelper({"stale" : "false"}),
                                 QueryHelper({"stale" : "ok"}),
                                 QueryHelper({"stale" : "update_after"})]
                if limit:
                    for q in view.queries:
                        q.params["limit"] = limit

    def add_reduce_queries(self, views=None, limit=None):
        if views is None:
            views = self.views

        for view in views:
            view.queries += [QueryHelper({"reduce" : "false"}),
                             QueryHelper({"reduce" : "true"})]
            if limit:
                for q in view.queries:
                    q.params["limit"] = limit

    def add_group_queries(self, views=None, limit=None):

        for view in self.views:
            if view.reduce_fn is None:
                continue
            view.queries += [QueryHelper({"group" : "true"})]
            if limit:
                for q in view.queries:
                    q.params['limit'] = limit

    def add_all_query_sets(self, views=None, limit=None):
        self.add_startkey_endkey_queries(views, limit)
        self.add_stale_queries(views, limit)

class SalesDataSet:
    def __init__(self, server, cluster, docs_per_day=200, bucket="default", test_datatype=False, template_items_num=None,
                 custom_reduce=False):
        self.server = server
        self.cluster = cluster
        self.docs_per_day = docs_per_day
        self.years = 1
        self.months = 12
        self.days = 28
        self.bucket = bucket
        self.test_datatype = test_datatype
        self.template_items_num = template_items_num
        self.custom_reduce = custom_reduce
        self.views = self.create_views()
        self.name = "sales_dataset"
        self.kv_store = None
#        self.docs_set = []


    # views for this dataset
    def create_views(self):
        vfn = "function (doc) { emit([doc.join_yr, doc.join_mo, doc.join_day], doc.sales);}"

        if self.test_datatype:
            vfn1 = "function (doc) { emit([doc.join_yr, doc.join_mo, doc.join_day], doc['is_support_included']);}"
            vfn2 = "function (doc) { emit([doc.join_yr, doc.join_mo, doc.join_day], [doc['is_support_included'],doc['is_high_priority_client']]);}"
            vfn3 = "function (doc) { emit([doc.join_yr, doc.join_mo, doc.join_day], Date.parse(doc['delivery_date']));}"
            vfn4 = "function (doc) { emit([doc.join_yr, doc.join_mo, doc.join_day], {client : {name : doc['client_name'], contact : doc['client_contact']}});}"
            vfn5 = "function (doc) { emit([doc.join_yr, doc.join_mo, doc.join_day], doc['client_reclaims_rate']);}"
            views = [QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn1),
                     QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn2),
                     QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn3),
                     QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn4),
                     QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn5),
                     QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn5, reduce_fn="_count")]
        elif self.template_items_num:
            vfn1 = "function (doc) { emit([doc.join_yr, doc.join_mo, doc.join_day], doc.sales);}"
            views = [QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn1),
                     QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn1, reduce_fn="_count")]
        elif self.custom_reduce:
            reduce_fn = "function(key, values, rereduce) {  if (rereduce)" + \
             " { var result = 0;  for (var i = 0; i < values.length; i++) " + \
             "{ emit(key,values[0]);  result += values[i]; }   return result;" + \
             " } else { emit(key,values[0]);     return values.length;  }}"
            views = [QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn, reduce_fn=reduce_fn)]
            for view in views:
                view.view.red_func = '_count'
        else:
            views = [QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn, reduce_fn="_count"),
                     QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn, reduce_fn="_sum"),
                     QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=vfn, reduce_fn="_stats")]

        return views

    def generate_docs(self, view, start=0, end=None):
        generators = []
        if end is None:
            end = self.docs_per_day
        join_yr = list(range(2008, 2008 + self.years))
        join_mo = list(range(1, self.months + 1))
        join_day = list(range(1, self.days + 1))

        if self.test_datatype:
            template = '{{ "join_yr" : {0}, "join_mo" : {1}, "join_day" : {2},'
            template += ' "sales" : {3}, "delivery_date" : "{4}", "is_support_included" : {5},'
            template += ' "is_high_priority_client" : {6}, "client_contact" :  "{7}",'
            template += ' "client_name" : "{8}", "client_reclaims_rate" : {9}}}'
            sales = [200000, 400000, 600000, 800000]

            is_support = ['true', 'false']
            is_priority = ['true', 'false']
            contact = str(uuid.uuid4())[:10]
            name = str(uuid.uuid4())[:10]
            rate = [x * 0.1 for x in range(0, 10)]
            for year in join_yr:
                for month in join_mo:
                    for day in join_day:
                        prefix = str(uuid.uuid4())[:7]
                        delivery = str(datetime.date(year, month, day))
                        generators.append(DocumentGenerator(view.prefix + prefix,
                                                  template,
                                                  [year], [month], [day],
                                                  sales, [delivery], is_support,
                                                  is_priority, [contact],
                                                  [name], rate,
                                                  start=start, end=end))
        else:
            template = '{{ "join_yr" : {0}, "join_mo" : {1}, "join_day" : {2},'
            if self.template_items_num:
                for num in range(self.template_items_num - 2):
                    template += '"item_%s" : "value_%s",' % (num, num)
            template += ' "sales" : {3} }}'
            sales = [200000, 400000, 600000, 800000]
            for year in join_yr:
                for month in join_mo:
                    for day in join_day:
                        prefix = str(uuid.uuid4())[:7]
                        generators.append(DocumentGenerator(view.prefix + prefix,
                                                  template,
                                                  [year], [month], [day],
                                                  sales,
                                                  start=start, end=end))
        return generators

    def add_reduce_queries(self, params, views=None, limit=None):
        views = views or self.views
        for view in views:
            if isinstance(params, dict):
                params_dict = params
            else:
                params_dict = ViewQueryTests.parse_string_to_dict(params)
            if limit:
                params_dict['limit'] = limit
            view.queries += [QueryHelper(params_dict)]

    def add_skip_queries(self, skip, limit=None, views=None):
        if views is None:
            views = self.views

        for view in views:
            if view.reduce_fn:
                view.queries += [QueryHelper({"skip" : 0})]
                continue
            if limit:
                view.queries += [QueryHelper({"skip" : skip, "limit" : limit})]
            else:
                view.queries += [QueryHelper({"skip" : skip})]

    def add_stale_queries(self, views=None, limit=None, stale_param=None):
        if views is None:
            views = self.views

        for view in views:
            if stale_param:
                view.queries += [QueryHelper({"stale" :  stale_param})]
            else:
                view.queries += [QueryHelper({"stale" : "false"}),
                                 QueryHelper({"stale" : "ok"}),
                                 QueryHelper({"stale" : "update_after"})]
            if limit:
                for q in view.queries:
                    q.params['limit'] = limit

    """
        Create queries for testing docids on duplicate start_key result ids.
        Only pass in view that indexes all employee types as they are
        expected in the query params...i.e (ui,admin,arch)
    """
    def add_startkey_endkey_docid_queries(self, views=None, limit=None):
        if views is None:
            views = []

        for view in views:
            view.queries += [QueryHelper({"startkey" : "[2008,7,1]",
                                          "startkey_docid" : '"%s%s"' % (
                                              self.views[0].prefix,
                                              str(uuid.uuid4())[:1])}),
                             QueryHelper({"startkey" : "[2008,7,1]",
                                          "startkey_docid" : '"%s%s"' % (
                                              self.views[0].prefix,
                                              str(uuid.uuid4())[:1]),
                                          "descending" : "false"},),
                             QueryHelper({"startkey" : "[2008,7,1]",
                                          "startkey_docid" : '"%s%s"' % (
                                              self.views[0].prefix,
                                              str(uuid.uuid4())[:1]),
                                          "descending" : "true"}),
                             QueryHelper({"startkey" : "[2008,7,1]",
                                          "startkey_docid" : '"%s%s"' % (
                                              self.views[0].prefix,
                                              str(uuid.uuid4())[:1])}),
                             QueryHelper({"startkey" : "[2008,7,1]",
                                          "startkey_docid" : "ui0000-2008_07_01",
                                          "descending" : "false"}),
                             QueryHelper({"startkey" : "[2008,7,1]",
                                          "startkey_docid" : '"%s%s"' % (
                                              self.views[0].prefix,
                                              str(uuid.uuid4())[:1]),
                                          "descending" : "true"}),
                             # +endkey_docid
                             QueryHelper({"startkey" : "[2008,0,1]",
                                          "endkey"   : "[2008,7,1]",
                                          "endkey_docid" : '"%s%s"' % (
                                              self.views[0].prefix,
                                              str(uuid.uuid4())[:1]),
                                          "inclusive_end" : "false"}),
                             QueryHelper({"endkey"   : "[2008,7,1]",
                                          "endkey_docid" : '"%s%s"' % (
                                              self.views[0].prefix,
                                              str(uuid.uuid4())[:1]),
                                          "inclusive_end" : "false"}),
                             # + inclusive_end
                             QueryHelper({"endkey"   : "[2008,7,1]",
                                          "endkey_docid" : '"%s%s"' % (
                                              self.views[0].prefix,
                                              str(uuid.uuid4())[:1]),
                                          "inclusive_end" : "true"}),
                             # + single bounded and descending
                             QueryHelper({"startkey" : "[2008,7,1]",
                                          "endkey"   : "[2008,2,20]",
                                          "endkey_docid"   : '"%s%s"' % (
                                              self.views[0].prefix,
                                              str(uuid.uuid4())[:1]),
                                          "descending"   : "true",
                                          "inclusive_end" : "true"}),
                             QueryHelper({"startkey" : "[2008,7,1]",
                                          "endkey"   : "[2008,2,20]",
                                          "endkey_docid"   : '"%s%s"' % (
                                              self.views[0].prefix,
                                              str(uuid.uuid4())[:1]),
                                          "descending"   : "true",
                                          "inclusive_end" : "false"}),
                             # + double bounded and descending
                             QueryHelper({"startkey" : "[2008,7,1]",
                                          "startkey_docid" : '"%s%s"' % (
                                              self.views[0].prefix,
                                              str(uuid.uuid4())[:1]),
                                          "endkey"   : "[2008,2,20]",
                                          "endkey_docid"   : '"%s%s"' % (
                                              self.views[0].prefix,
                                              str(uuid.uuid4())[:1]),
                                          "descending"   : "true",
                                          "inclusive_end" : "false"})]
            if limit:
                for q in view.queries:
                    q.params["limit"] = limit

    def add_group_queries(self, views=None, limit=None):

        for view in self.views:
            if view.reduce_fn is None:
                continue
            view.queries += [QueryHelper({"group" : "true"})]
            if limit:
                for q in view.queries:
                    q.params["limit"] = limit

    def add_startkey_endkey_queries(self, views=None, limit=None):
        if views is None:
            views = self.views

        for view in views:
            if view.reduce_fn:
                continue
            view.queries += [QueryHelper({"startkey" : "[2008,7,null]"}),
                             QueryHelper({"startkey" : "[2008,0,1]",
                                          "endkey"   : "[2008,7,1]",
                                          "inclusive_end" : "false"}),
                             QueryHelper({"startkey" : "[2008,0,1]",
                                          "endkey"   : "[2008,7,1]",
                                          "inclusive_end" : "true"}),
                             QueryHelper({"startkey" : "[2008,7,1]",
                                          "endkey"   : "[2008,1,1]",
                                          "descending"   : "true",
                                          "inclusive_end" : "false"}),
                             QueryHelper({"startkey" : "[2008,7,1]",
                                          "endkey"   : "[2008,1,1]",
                                          "descending"   : "true",
                                          "inclusive_end" : "true"}),
                             QueryHelper({"startkey" : "[2008,1,1]",
                                          "endkey"   : "[2008,7,1]",
                                          "descending"   : "false",
                                          "inclusive_end" : "false"}),
                             QueryHelper({"startkey" : "[2008,1,1]",
                                          "endkey"   : "[2008,7,1]",
                                          "descending"   : "false",
                                          "inclusive_end" : "true"})]
            if limit is not None:
                for q in view.queries:
                    q.params['limit'] = limit

class ExpirationDataSet:
    def __init__(self, server, cluster, num_docs=10000, bucket="default", expire=60):
        self.num_docs = num_docs
        self.bucket = bucket
        self.server = server
        self.cluster = cluster
        self.views = self.create_views()
        self.name = "expiration_dataset"
        self.kv_store = None
        self.docs_set = []
        self.expire = expire
        self.expire_millis = 0

    def create_views(self):
        view_fn = 'function (doc, meta) {if(doc.age !== undefined) { emit(doc.age, meta.expiration);}}'
        return [QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=view_fn)]

    def generate_docs(self, view, start=0, end=None):
        if end is None:
            end = self.num_docs
        age = list(range(start, end))
        name = [view.prefix + '-' + str(i) for i in range(start, end)]
        template = '{{ "age": {0}, "name": "{1}" }}'

        gen_load = DocumentGenerator(view.prefix, template, age, name, start=start,
                                     end=end)
        return [gen_load]

    def set_expiration_time(self):
        self.expire_millis = int(time.time() + self.expire)

    def query_verify_value(self, tc):
        for view in self.views:
            #query view
            query = {"stale" : "false"}
            if view.is_dev_view:
                self.query["full_set"] = "true"
            results = tc._rconn().query_view(view.name, view.name,
                                             self.bucket, query, timeout=600)
            tc.assertTrue(len(results.get('rows', 0)) == self.num_docs,
                          "Actual results %s, expected %s" % (
                                    len(results.get('rows', 0)), self.num_docs))
            for row in results.get('rows', 0):
                tc.assertTrue(row['value'] in range(self.expire_millis - 200, self.expire_millis + 600),
                                  "Expiration should be %s, but actual is %s" % \
                                   (self.expire_millis, row['value']))
            tc.log.info("Expiration emmited correctly")

class FlagsDataSet:
    def __init__(self, server, cluster, num_docs=10000, bucket="default", item_flag=50331650):
        self.num_docs = num_docs
        self.bucket = bucket
        self.server = server
        self.cluster = cluster
        self.views = self.create_views()
        self.name = "flags_dataset"
        self.kv_store = None
        self.docs_set = []
        self.item_flag = item_flag

    def create_views(self):
        view_fn = 'function (doc, meta) {if(doc.age !== undefined) { emit(doc.age, meta.flags);}}'
        return [QueryView(self.server, self.cluster, bucket=self.bucket, fn_str=view_fn)]

    def generate_docs(self, view, start=0, end=None):
        if end is None:
            end = self.num_docs
        age = list(range(start, end))
        name = [view.prefix + '-' + str(i) for i in range(start, end)]
        template = '{{ "age": {0}, "name": "{1}" }}'

        gen_load = DocumentGenerator(view.prefix, template, age, name, start=start,
                                     end=end)
        return [gen_load]

    def query_verify_value(self, tc):
        for view in self.views:
            #query view
            results = query = {"stale" : "false"}
            if view.is_dev_view:
                self.query["full_set"] = "true"
            results = tc._rconn().query_view(view.name, view.name,
                                             self.bucket, query, timeout=600)
            tc.assertEqual(len(results.get('rows', 0)), self.num_docs,
                              "Returned number of items is incorrect"\
                              "Actual:%s, expected:%s" % (
                                            len(results.get('rows', 0)), self.num_docs))
            for row in results.get('rows', 0):
                tc.assertTrue(row['value'] == self.item_flag,
                                  "Flag should be %s, but actual is %s" % \
                                   (self.item_flag, row['value']))

class BigDataSet:
    def __init__(self, server, cluster, num_docs, value_size, bucket="default"):
        self.server = server
        self.cluster = cluster
        self.bucket = bucket
        self.views = self.create_views()
        self.name = "big_data_set"
        self.num_docs = num_docs
        self.kv_store = None
        self.value_size = value_size

    def create_views(self):
        view_fn = 'function (doc, meta) {if(doc.age !== undefined) { emit(doc.age, doc.name);}}'
        view_fn2 = 'function (doc, meta) {var val_test = "test";  while (val_test.length < 700000000) { val_test = val_test.concat(val_test);  }emit(doc.age, val_test);}'
        reduce_fn = "function(key, values, rereduce) {var val_test = 'test';  while (val_test.length < 70000) { val_test = val_test.concat(val_test);  } return val_test;}"
        return [QueryView(self.server, self.cluster, fn_str=view_fn),
                QueryView(self.server, self.cluster, fn_str=view_fn2),
                QueryView(self.server, self.cluster, fn_str=view_fn, reduce_fn=reduce_fn)]

    def generate_docs(self, view, start=0, end=None):
        if end is None:
            end = self.num_docs
        age = list(range(start, end))
        name = ['a' * self.value_size, ]
        template = '{{ "age": {0}, "name": "{1}" }}'

        gen_load = DocumentGenerator(view.prefix, template, age, name, start=start,
                                     end=end)
        return [gen_load]

    def add_stale_queries(self, error, views=None):
        if views is None:
            views = self.views

        for view in views:
            view.queries += [QueryHelper({"stale" : "false"}, error=error)]

class QueryHelper:
    def __init__(self, params, error=None, non_json=False):

        self.params = params
        self.error = error
        self.non_json = non_json

    def __str__(self):
        return str(self.params)
