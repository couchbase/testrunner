from TestInput import TestInputSingleton
from autocompaction import AutoCompactionTests
from couchbase.cluster import Cluster
from couchbase.document import View
from membase.api.rest_client import RestConnection, RestHelper, Bucket
from membase.helper.failover_helper import FailoverHelper
from membase.helper.rebalance_helper import RebalanceHelper
from memcached.helper.data_helper import VBucketAwareMemcached, \
    DocumentGenerator, KVStoreAwareSmartClient
from memcached.helper.old_kvstore import ClientKeyValueStore
from old_tasks import task, taskmanager
from remote.remote_util import RemoteMachineShellConnection
from threading import Thread, Event
from viewtests import ViewBaseTests
import copy
import datetime
import json
import logger
import math
import random
import sys
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
                        name=name, args=args, kwargs=kwargs, verbose=verbose)
        self._stop = Event()

    def stop(self):
        self._stop.set()
        self._Thread__stop()

    def stopped(self):
        return self._stop.isSet()

class ViewQueryTests(unittest.TestCase):
    def setUp(self):
        try:
            ViewBaseTests.common_setUp(self)
            self.limit = TestInputSingleton.input.param("limit", None)
            self.reduce_fn = TestInputSingleton.input.param("reduce_fn", None)
            self.skip_rebalance = TestInputSingleton.input.param("skip_rebalance", False)
            self.wait_persistence = TestInputSingleton.input.param("wait_persistence", False)
            self.error = None
            self.task_manager = taskmanager.TaskManager()
            self.task_manager.start()
            self.thread_crashed = Event()
            self.thread_stopped = Event()
            self.server = None
            self.cluster = Cluster()
            if not self.skip_rebalance:
                self.cluster.rebalance(self.servers[:], self.servers[1:], [])
            if self.num_buckets == 1:
                ViewBaseTests._create_default_bucket(self, replica=self.replica)
            else:
                ViewBaseTests._create_multiple_buckets(self, replica=self.replica)
        except Exception as ex:
            self.input.test_params["stop-on-failure"] = True
            self.log.error("SETUP WAS FAILED. ALL TESTS WILL BE SKIPPED")
            self.fail(ex)

    def tearDown(self):
        try:
            ViewBaseTests.common_tearDown(self)
        finally:
            self.task_manager.cancel()
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
        data_set = SimpleDataSet(self._rconn(), self.num_docs, self.limit)
        data_set.add_stale_queries()
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
        data_set = SimpleDataSet(self._rconn(), self.num_docs, limit=self.limit)
        data_set.add_stale_queries(stale_param="false", limit=self.limit)
        doc_names = data_set.load(self, data_set.views[0])
        for view in data_set.views:
            for q in view.queries:
                q.expected_keys = doc_names
        self._query_all_views(data_set.views, verify_expected_keys=True)

        #delete docs
        doc_names = ViewBaseTests._delete_docs(self, self.num_docs, self.num_docs / 2,
                                               data_set.views[0].prefix)
        RebalanceHelper.wait_for_persistence(self.servers[0], data_set.bucket)
        for view in data_set.views:
            for q in view.queries:
                q.expected_num_docs = self.num_docs / 2
                if self.limit and self.limit < self.expected_num_docs:
                    q.expected_num_docs = self.limit
                q.expected_keys = doc_names
        self._query_all_views(data_set.views, verify_expected_keys=True)

    def test_simple_dataset_startkey_endkey_queries(self):
        '''
        Test uses simple data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. Start load data
            2. Simultaneously start querying(different combinations of
               stratkey. endkey, descending, inclusive_end, parameters)
        '''
        data_set = SimpleDataSet(self._rconn(), self.num_docs, limit=self.limit)
        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set)

    def test_reproduce_mb_7193_queries(self):
        '''
        Test uses simple data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Based on MB-7193
        '''
        ddoc_name = 'ddoc/test'
        data_set = SimpleDataSet(self._rconn(), self.num_docs, limit=self.limit,
                                 name_ddoc=ddoc_name)
        data_set.add_startkey_endkey_queries()
        data_set.load(self, data_set.views[0], True)
        self._query_all_views(data_set.views, limit=data_set.limit)

    def test_simple_dataset_startkey_endkey_non_json_queries(self):
        '''
        Test uses simple data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. Start load data
            2. Simultaneously start querying(different combinations of
               stratkey. endkey, descending, inclusive_end, parameters with non-json char)
        '''
        symbols = [u"\xf1", u"\xe1", u"\xfc", u"\xbf", u"\xf1", u"\xe1", u"\xfc",
                   u"\xbf", u"\uffff"]
        data_set = SimpleDataSet(self._rconn(), self.num_docs, limit=self.limit,
                                 json_case=True)
        doc_names = data_set.load(self, data_set.views[0], True)
        for symbol in symbols:
            data_set.add_startkey_endkey_non_json_queries(doc_names, symbol)
            self._query_all_views(data_set.views, limit=data_set.limit)

    def test_simple_dataset_all_queries(self):
        '''
        Test uses simple data set:
            -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. Start load data
            2. Simultaneously start querying(include stale and startkey endkey queries)
        '''
        data_set = SimpleDataSet(self._rconn(), self.num_docs, limit=self.limit)
        data_set.add_all_query_sets()
        self._query_test_init(data_set)

    def test_simple_dataset_reduce_queries(self):
        '''
        Test uses simple data set:
             -documents are structured as {name: some_name<string>, age: some_integer_age<int>}
        Steps to repro:
            1. Start load data
            2. Simultaneously start querying(reduce=false, reduce=true)
        '''
        data_set = SimpleDataSet(self._rconn(), self.num_docs,limit = self.limit,reduce_fn = self.reduce_fn)
        data_set.add_reduce_queries()
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
        query_params = TestInputSingleton.input.param("query_params", None)
        error = TestInputSingleton.input.param("error", None)

        data_set = SimpleDataSet(self._rconn(), self.num_docs)
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
        data_set = SimpleDataSet(self._rconn(), self.num_docs)
        data_set.add_stale_queries()
        self._query_test_init(data_set)

        #load one more portion of data
        ViewBaseTests._load_docs(self, num_docs_to_add, "new-data")
        #query once again
        results = ViewBaseTests._get_view_results(self, self._rconn(), data_set.views[0].bucket,
                                                      data_set.views[0].name, extra_params={'stale' : stale})
        if stale == 'False':
            expected_num_docs = self.num_docs + num_docs_to_add
        elif stale in ['update_after', 'ok']:
            expected_num_docs = self.num_docs
        self.assertEqual(len(ViewBaseTests._get_keys(self, results)), expected_num_docs,
                         "Stale %s query failed: expected keys=%d, current keys=%d" % (stale, expected_num_docs,
                                                                                       len(ViewBaseTests._get_keys(self, results))))
        self.log.info("Stale %s query passed: has %d keys as expected" % (stale, expected_num_docs))


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
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

        data_set.add_startkey_endkey_queries()
        data_set.load(self, data_set.views[0])
        data_set.preload_matching_query_keys()
        self._query_all_views(data_set.views, verify_expected_keys=True)

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
        docs_per_day = self.input.param('docs-per-day', 200)
        options = {"updateMinChanges" : docs_per_day,
                   "replicaUpdateMinChanges" : docs_per_day}
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit,
                                   ddoc_options=options)

        data_set.load(self, data_set.views[0])
        RebalanceHelper.wait_for_persistence(self.servers[0], data_set.bucket)

        for view in data_set.views:
            # run queries to create indexes
            self.cluster.query_view(self.servers[0], view.name, view.name,
                                    {"connectionTimeout" : 60000})
            active_task = self.cluster.async_monitor_active_task(self.servers[0],
                                                                 "indexer",
                                                                 "_design/" + view.name,
                                                                 wait_task=False)
            active_task.result()

        data_set.add_stale_queries(stale_param="ok")
        data_set.preload_matching_query_keys()

        for view in data_set.views:
            view.consistent_view = True
        self.log.info("QUERY BEFORE REBALANCE")
        self._query_all_views(data_set.views, verify_expected_keys=True)
        self.log.info("START REBALANCE AND QUERY")
        rebalance_thread = StoppableThread(target=self.cluster.rebalance,
               name="rebalance",
               args=(self.servers[:1], self.servers[1:], []))
        rebalance_thread.start()
        while rebalance_thread.is_alive():
            self._query_all_views(data_set.views, verify_expected_keys=True)

    def test_employee_dataset_min_changes_check(self):
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
            1. Create views with updateMinChanges option
            2. Loader threads starts
            3. Check thread for each view in each node is started
                check thread wait for update_seqs will be more than
                    updateMinChanges optoin
                then checks that index is started
        '''
        docs_per_day = self.input.param('docs-per-day', 200)
        min_changes = self.input.param('min-changes', 1000)
        options = {"updateMinChanges" : min_changes,
                   "replicaUpdateMinChanges" : min_changes}
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit,
                                   ddoc_options=options)

        load_thread = StoppableThread(target=data_set.load,
               name="load_thread",
               args=(self, data_set.views[0]))
        try:
            load_thread.start()

            check_threads = []
            for server in self.servers:
                for view in data_set.views:
                    t = StoppableThread(target=data_set.wait_min_changes,
                           name="wait-{0}".format(view.name),
                           args=(self, server, view, min_changes))
                    check_threads.append(t)
                    t.start()

            while True:
                if not check_threads:
                    return
                self.thread_stopped.wait(60)
                if self.thread_crashed.is_set():
                    for t in check_threads:
                        t.stop()
                    return
                else:
                    check_threads = [d for d in check_threads if d.is_alive()]
                    self.thread_stopped.clear()
        finally:
            load_thread.join()

    def test_employee_dataset_alldocs_queries(self):
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
            2. Simultaneously all_docs queries
        '''
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        data_set.add_all_docs_queries()
        if self.wait_persistence:
            data_set.load(self, data_set.views[0], True)
            for server in self.servers:
                RebalanceHelper.wait_for_persistence(server, data_set.bucket)
            self._query_all_views(data_set.views)
        else:
            self._query_test_init(data_set)

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
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

        data_set.add_key_queries()
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
        query_params = TestInputSingleton.input.param("query_params", None)
        error = TestInputSingleton.input.param("error", None)
        docs_per_day = self.input.param('docs-per-day', 20)

        data_set = EmployeeDataSet(self._rconn(), docs_per_day)
        data_set.add_negative_query(query_params, error)
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
        valid_params = TestInputSingleton.input.param("valid_params", None)
        invalid_params = TestInputSingleton.input.param("invalid_params", None)
        docs_per_day = self.input.param('docs-per-day', 200)

        data_set = EmployeeDataSet(self._rconn(), docs_per_day)
        data_set.add_query_invalid_startkey_endkey_docid(valid_params, invalid_params)
        self._query_test_init(data_set)

    def test_employee_dataset_alldocs_queries_rebalance_in(self):
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
            3. Both threads are finished
            4. Start rebalance in
            5. Start querying
        '''
        docs_per_day = self.input.param('docs-per-day', 200)
        num_nodes_to_add = self.input.param('num_nodes_to_add',0)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        data_set.add_all_docs_queries()
        self._query_test_init(data_set, False)

        # rebalance_in and verify loaded data
        ViewBaseTests._begin_rebalance_in(self, howmany=num_nodes_to_add + 1)
        self._query_all_views(data_set.views)
        ViewBaseTests._end_rebalance(self)

        #verify queries after rebalance
        self._query_test_init(data_set)

    def test_employee_dataset_alldocs_failover_queries(self):
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
        failover_nodes = []
        try:
            docs_per_day = self.input.param('docs-per-day', 200)
            data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

            data_set.add_all_docs_queries()
            self._query_test_init(data_set, False)

            master = self.servers[0]
            RebalanceHelper.wait_for_persistence(master, "default")

            # failover and verify loaded data
            failover_helper = FailoverHelper(self.servers, self)
            failover_nodes = failover_helper.failover(self.failover_factor)
            self.log.info("10 seconds sleep after failover before invoking rebalance...")
            time.sleep(10)

            rest=RestConnection(self.servers[0])
            nodes = rest.node_statuses()
            rest.rebalance(otpNodes=[node.id for node in nodes],
                           ejectedNodes=[node.id for node in failover_nodes])

            self._query_all_views(data_set.views, limit=data_set.limit)

            msg = "rebalance failed while removing failover nodes {0}".format(failover_nodes)
            self.assertTrue(rest.monitorRebalance(), msg=msg)

            #verify queries after failover
            self._query_all_views(data_set.views, limit=data_set.limit)
        finally:
            for server in [server for server in self.servers
                           for node in failover_nodes
                           if node.ip == server.ip and str(node.port) == server.port]:
                shell = RemoteMachineShellConnection(server)
                shell.start_couchbase()
                time.sleep(10)
                shell.disconnect()

    def test_employee_dataset_alldocs_incremental_failover_queries(self):
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
        try:
            docs_per_day = self.input.param('docs-per-day', 200)
            data_set = EmployeeDataSet(self._rconn(), docs_per_day)

            data_set.add_all_docs_queries()
            self._query_test_init(data_set, False)

            servers=self.servers;

            failover_nodes = []
            # incrementaly failover nodes and verify loaded data
            for i in range(self.failover_factor):
                failover_helper = FailoverHelper(servers, self)
                failover_nodes.extend(failover_helper.failover(1))
                self.log.info("10 seconds sleep after failover before invoking rebalance...")
                time.sleep(10)

                rest=RestConnection(self.servers[0])
                nodes = rest.node_statuses()
                rest.rebalance(otpNodes=[node.id for node in nodes],
                           ejectedNodes=[failover_nodes[i].id])

                self._query_all_views(data_set.views)

                temp_node_ips = []
                for item in failover_nodes:
                    temp_node_ips.append(item.ip)

                temp=[]
                for server in servers:
                    if server.ip not in temp_node_ips:
                        rest = RestConnection(server)
                        if not RestHelper(rest).is_ns_server_running(timeout_in_seconds=1):
                            continue
                        temp.append(server)
                servers=temp

                msg = "rebalance failed while removing failover nodes {0}".format(failover_nodes)
                self.assertTrue(RestConnection(self.servers[0]).monitorRebalance(), msg=msg)

        finally:
            for server in [server for server in self.servers
                           for node in failover_nodes
                           if node.ip == server.ip and str(node.port) == server.port]:
                shell = RemoteMachineShellConnection(server)
                shell.start_couchbase()
                self.log.info("10 seconds for couchbase server to startup...")
                time.sleep(10)
                shell.disconnect()

    def test_employee_dataset_alldocs_queries_start_stop_rebalance_in_incremental(self):
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
        docs_per_day = self.input.param('docs-per-day', 20)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

        data_set.add_all_docs_queries()
        self._query_test_init(data_set, False)

        master = self.servers[0]
        RebalanceHelper.wait_for_persistence(master, "default")

        rest = RestConnection(self.servers[0])
        nodes = rest.node_statuses()

        for server in self.servers[1:]:
            self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
            self.log.info("adding node {0}:{1} to cluster".format(server.ip, server.port))
            otpNode = rest.add_node(master.rest_username, master.rest_password, server.ip, server.port)
            msg = "unable to add node {0}:{1} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip, server.port))

            # Just doing 2 iterations
            for expected_progress in [30, 60]:
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
                reached = RestHelper(rest).rebalance_reached(expected_progress)
                self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
                stopped = rest.stop_rebalance()
                self.assertTrue(stopped, msg="unable to stop rebalance")
                self._query_all_views(data_set.views, limit=data_set.limit)

            rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
            self.assertTrue(rest.monitorRebalance(), msg="rebalance operation failed restarting")
            self._query_all_views(data_set.views, limit=data_set.limit)

            self.assertTrue(len(rest.node_statuses()) -len(nodes)==1, msg="number of cluster's nodes is not correct")
            nodes = rest.node_statuses()

    def test_employee_dataset_alldocs_queries_start_stop_rebalance_out_incremental(self):
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
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        docs_per_day = self.input.param('docs-per-day', 20)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

        data_set.add_all_docs_queries()
        self._query_test_init(data_set, False)

        master = self.servers[0]
        RebalanceHelper.wait_for_persistence(master, "default")

        rest=RestConnection(self.servers[0])
        nodes = rest.node_statuses()

        for server in self.servers[1:]:
            ejectedNodes=[]
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

                self._query_all_views(data_set.views, limit=self.limit)

            #for cases if rebalance ran fast
            if RestHelper(rest).is_cluster_rebalanced():
                self.log.info("Rebalance is finished already.")
                nodes = rest.node_statuses()
                continue
            rest.rebalance(otpNodes=[node.id for node in nodes], ejectedNodes=ejectedNodes)

            self.assertTrue(rest.monitorRebalance(), msg="rebalance operation failed restarting")
            self._query_all_views(data_set.views, limit=self.limit)

            self.assertTrue(len(nodes) - len(rest.node_statuses()) == 1, msg="number of cluster's nodes is not correct")
            nodes = rest.node_statuses()

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
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

        data_set.add_startkey_endkey_docid_queries()
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
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

        data_set.add_group_count_queries()
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
        docs_per_day = self.input.param('docs-per-day', 200)
        num_nodes_to_add = self.input.param('num_nodes_to_add', 1)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

        data_set.add_startkey_endkey_queries(limit=self.limit)
        self._query_test_init(data_set, False)

        # rebalance_in and verify loaded data
        ViewBaseTests._begin_rebalance_in(self, howmany=num_nodes_to_add + 1)
        self._query_all_views(data_set.views, limit=data_set.limit)
        ViewBaseTests._end_rebalance(self)

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
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        docs_per_day = self.input.param('docs-per-day', 200)
        num_nodes_to_rem = self.input.param('num_nodes_to_rem', 1)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)


        data_set.add_startkey_endkey_queries(limit=self.limit)
        self._query_test_init(data_set, False)

        # rebalance_out and verify loaded data
        ViewBaseTests._begin_rebalance_out(self, howmany=num_nodes_to_rem + 1)
        self._query_all_views(data_set.views, limit=data_set.limit)
        ViewBaseTests._end_rebalance(self)

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
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

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
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

        data_set.add_all_query_sets()
        if self.wait_persistence:
            data_set.load(self, data_set.views[0], True)
            for server in self.servers:
                RebalanceHelper.wait_for_persistence(server, data_set.bucket)
            self._query_all_views(data_set.views)
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
        docs_per_day = self.input.param('docs-per-day', 200)
        skip = self.input.param('skip', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

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
        docs_per_day = self.input.param('docs-per-day', 200)
        skip = 0
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)
        data_set.load(self, data_set.views[0], True)

        for view in data_set.views:
            if view.reduce_fn:
                data_set.views.remove(view)

        while data_set.views:
            for view in data_set.views:
                data_set.add_skip_queries(skip, limit=self.limit)
            self._query_all_views(data_set.views)
            skip +=self.limit
            data_set.views = [view for view in data_set.views if skip < view.index_size]

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
        ds1 = EmployeeDataSet(self._rconn())
        ds2 = SimpleDataSet(self._rconn(), self.num_docs)
        data_sets = [ds1, ds2]

        # load and query all views and datasets
        test_threads = []
        for ds in data_sets:
            ds.add_all_query_sets()
            t = Thread(target=self._query_test_init,
                       name=ds.name,
                       args=(ds, False))
            test_threads.append(t)
            t.start()

        [t.join() for t in test_threads]

        ViewBaseTests._begin_rebalance_out(self)
        ViewBaseTests._end_rebalance(self)

        # verify
        [self._query_all_views(ds.views) for ds in data_sets]

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
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)
        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set, False)

        query_nodes_threads = []
        for server in self.servers:
            self.server = server
            t = StoppableThread(target=self._query_all_views,
               name="query-node-{0}".format(server.ip),
               args=(data_set.views,))
            query_nodes_threads.append(t)
            t.start()

        while True:
            if not query_nodes_threads:
                break
            self.thread_stopped.wait(60)
            if self.thread_crashed.is_set():
                for t in query_nodes_threads:
                    t.stop()
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
        docs_per_day = self.input.param('docs-per-day', 500)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set, False)

        prefix = str(uuid.uuid4())[:7]
        ViewBaseTests._load_docs(self, self.num_docs, prefix, verify=False)

        # Pick a node to warmup
        server = self.servers[-1]
        shell = RemoteMachineShellConnection(server)
        self.log.info("Node {0} is being stopped".format(server.ip))
        shell.stop_couchbase()
        time.sleep(20)
        shell.start_couchbase()
        self.log.info("Node {0} should be warming up".format(server.ip))

        self._query_all_views(data_set.views)

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
        docs_per_day = self.input.param('docs-per-day', 200)
        how_many_add = self.input.param('how_many_add', 0)

        data_set = EmployeeDataSet(self._rconn(), docs_per_day)
        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set, False)

        rest = RestConnection(self.servers[0])

        for server in self.servers[1:how_many_add + 1]:
            self.log.info("adding node {0}:{1} to cluster".format(server.ip, server.port))
            otpNode = rest.add_node(self.servers[0].rest_username, self.servers[0].rest_password,
                                    server.ip, server.port)
            msg = "unable to add node {0}:{1} to the cluster"
            self.assertTrue(otpNode, msg.format(server.ip, server.port))

        self._query_all_views(data_set.views)

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
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set, False)

        # rebalance_in and verify loaded data
        for i in xrange(1, len(self.servers)):
                rebalance = self.cluster.async_rebalance(self.servers[:i + 1], [self.servers[i]], [])
                self.server = self.servers[i]
                self._query_all_views(data_set.views)
                rebalance.result()
    '''
    Test verifies querying when other thread is adding/updating/deleting other view
    Parameters:
        num-views-to-modify - number of views to add/edit/delete
        action - can be create/update/delete
    '''
    def test_employee_dataset_query_during_modifying_other_views(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        views_num = self.input.param('num-views-to-modify', 1)
        action = self.input.param('action', 'create')
        ddoc_name = "view_ddoc"
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)
        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set, False)

        view_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        views = [View("view_name_" + str(i), view_map_func, None, True)for i in xrange(views_num)]

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
                views = [View("view_name_" + str(i), view_map_func_new, None, True)for i in xrange(views_num)]
                for view in views:
                    tasks.append(self.cluster.async_create_view(self.servers[0], ddoc_name, view))
            if action == 'delete':
                for view in views:
                    tasks.append(self.cluster.async_delete_view(self.servers[0], ddoc_name, view))

        self._query_all_views(data_set.views)

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
        docs_per_day = self.input.param('docs-per-day', 200)
        percent_threshold = self.input.param('percent_compaction', 10)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)
        data_set.add_startkey_endkey_queries()

        self._query_test_init(data_set, False)

        bucket_name = "default"
        timeout = 180
        item_size = 1024
        from membase.helper.bucket_helper import BucketOperationHelper
        node_ram_ratio = BucketOperationHelper.base_bucket_ratio(self.servers)
        rest = RestConnection(self.servers[0])
        info = rest.get_nodes_self()
        available_ram = info.memoryQuota * (node_ram_ratio) / 2
        items = (int(available_ram * 1000) / 2)/item_size
        update_item_size = item_size * ((float(97 - percent_threshold)) / 100)

        self.log.info("set compaction to {0} %".format(percent_threshold))
        rest.set_auto_compaction("false", dbFragmentThresholdPercentage=percent_threshold, viewFragmntThresholdPercentage=percent_threshold)

        self.log.info("start inserting keys for compaction")
        AutoCompactionTests.insert_key(self.servers[0], bucket_name, items, item_size)
        time.sleep(10)

        self.log.info("start updating keys for compaction")
        AutoCompactionTests.insert_key(self.servers[0], bucket_name, items, int(update_item_size))

        end_time = time.time() + timeout
        compaction_started = False
        while time.time() < end_time:
            status, _ = rest.check_compaction_status(bucket_name)
            if status:
                self._query_all_views(data_set.views)
                compaction_started = True
            elif compaction_started:
                self.log.info("compaction is finished")
                break
            else:
                self.log.info("auto compaction is not started yet.")
        self.assertTrue(compaction_started, "auto compaction is not started in {0} sec. Queries were not run".format(timeout))
        self.log.info("run queries after compaction")
        self._query_all_views(data_set.views)

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
        failover_nodes = []
        try:
            docs_per_day = self.input.param('docs-per-day', 200)
            data_set = EmployeeDataSet(self._rconn(), docs_per_day)

            data_set.add_startkey_endkey_queries()
            self._query_test_init(data_set, False)

            master = self.servers[0]
            RebalanceHelper.wait_for_persistence(master, "default")

            # failover and verify loaded data
            failover_helper = FailoverHelper(self.servers, self)
            failover_nodes = failover_helper.failover(self.failover_factor)
            self.log.info("5 seconds sleep after failover ...")
            time.sleep(5)

            rest=RestConnection(master)
            nodes = rest.node_statuses()

            self._query_all_views(data_set.views)
        finally:
            stopped_servers = []
            for node in failover_nodes:
                for server in self.servers:
                    if node.ip == server.ip and str(node.port) == server.port:
                        stopped_servers.append(server)
                        break
                for server in stopped_servers:
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
        docs_per_day = self.input.param('docs-per-day', 200)
        num_threads = self.input.param('num_threads', 2)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)
        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set, False)

        query_nodes_threads = []
        for i in xrange(num_threads):
            t = StoppableThread(target=self._query_all_views,
                   name="query-node-1",
                   args=(data_set.views,))
            query_nodes_threads.append(t)
            t.start()

        while True:
            if not query_nodes_threads:
                break
            self.thread_stopped.wait(60)
            if self.thread_crashed.is_set():
                for t in query_nodes_threads:
                    t.stop()
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
        data_set = SimpleDataSet(self._rconn(), self.num_docs)
        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set, False)

        tasks = []
        #update/delete
        if action == 'update':
            view_map_func_new = "function (doc) {if(doc.age !== undefined) { emit(doc.age);}}"
            views = [View(view.name, view_map_func_new, None, False) for view in data_set.views]
            for view in views:
                tasks.append(self.cluster.async_create_view(self.servers[0], view.name, view))
                self._query_all_views(data_set.views)
        if action == 'delete':
            views = [View(view.name, None, None, False) for view in data_set.views]
            for view in views:
                tasks.append(self.cluster.async_delete_view(self.servers[0], view.name, view))
                time.sleep(1)
                for view in data_set.views:
                    for q in view.queries:
                        q.error = error
                self._query_all_views(data_set.views, False)
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
        data_set = SimpleDataSet(self._rconn(), self.num_docs, limit=self.limit)

        data_set.add_skip_queries(skip, limit=self.limit)
        data_set.load(self, data_set.views[0], True)

        if action == 'recreate':
            data_set.load(self, data_set.views[0], True)
        if action == 'delete':
            ViewBaseTests._delete_docs(self, self.num_docs, self.num_docs / 2, data_set.views[0].prefix)
            for view in data_set.views:
                for q in view.queries:
                    q.expected_num_docs = min(self.num_docs / 2, data_set.limit)

        self._query_all_views(data_set.views, limit=data_set.limit)


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
            docs_per_day = self.input.param('docs-per-day', 200)
            error = self.input.param('error', None)
            data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)
            data_set.add_startkey_endkey_queries()
            self._query_test_init(data_set, False)

            self.server = self.servers[-1]
            shell = RemoteMachineShellConnection(self.servers[0])
            self.log.info("Master Node is being stopped")
            shell.stop_couchbase()
            #error should be returned in results
            for view in data_set.views:
                for q in view.queries:
                    q.error = error
            time.sleep(20)
            self._query_all_views(data_set.views, False, limit=data_set.limit)
        finally:
            shell = RemoteMachineShellConnection(self.servers[0])
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
        docs_per_day = self.input.param('docs-per-day', 200)
        extra_params = self.input.param('extra_params', None)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)
        data_set.add_stale_queries()
        self._query_test_init(data_set)
        data_set.add_startkey_endkey_docid_queries_extra_params(extra_params=extra_params)
        self._query_all_views(data_set.views)


    '''
    load documents, run a view query with 1M results
    limit =1000 , skip = 0 -> 200 and then 200->0
    '''
    def test_employee_dataset_skip_bidirection_queries(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        skip = self.input.param('skip', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

        data_set.add_skip_queries(skip, limit=self.limit)
        data_set.add_skip_queries(skip)
        self._query_test_init(data_set)

        for view in data_set.views:
            view.queries = []
        data_set.add_skip_queries(skip, limit=self.limit)
        self._query_all_views(data_set.views)

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
        docs_per_day = self.input.param('docs-per-day', 200)
        data_sets = []
        for i in xrange(self.num_buckets):
            data_sets.append(EmployeeDataSet(self._rconn(), docs_per_day, bucket="bucket-{0}".format(i)))
        for data_set in data_sets:
            data_set.add_startkey_endkey_queries()
            self._query_test_init(data_set, False)

        query_bucket_threads = []
        for data_set in data_sets:
            t = StoppableThread(target=self._query_all_views,
                                name="query-bucket-{0}".format(data_set.bucket),
                                args=(data_set.views,))
            query_bucket_threads.append(t)
            t.start()

        while True:
            if not query_bucket_threads:
                break
            self.thread_stopped.wait(60)
            if self.thread_crashed.is_set():
                for t in query_bucket_threads:
                    t.stop()
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
        data_set = SimpleDataSet(self._rconn(), self.num_docs)
        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set, False)

        #create ddoc
        view_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        ddoc_name = "ddoc_test"
        view = View(ddoc_name, view_map_func, None, False)

        tasks = []
        for i in xrange(ddoc_num):
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
                for i in xrange(ddoc_num):
                    tasks.append(self.cluster.async_create_view(self.servers[0], ddoc_name + str(i), view))
            if action == 'delete':
                for i in xrange(ddoc_num):
                    prefix = ("","dev_")[view.dev_view]
                    tasks.append(self.cluster.async_delete_view(self.servers[0], prefix + ddoc_name + str(i), None))

        self._query_all_views(data_set.views)

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
        data_set = SimpleDataSet(self._rconn(), self.num_docs)
        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set, False)

        tasks = []
        #update/delete
        if action == 'update':
            view_map_func_new = "function (doc) {if(doc.age !== undefined) { emit(doc.age);}}"
            views = [View(view.name, view_map_func_new, None, False) for view in data_set.views]
            for view in views:
                tasks.append(self.cluster.async_create_view(self.servers[0], view.name, view))
                self._query_all_views(data_set.views)
        if action == 'delete':
            for view in data_set.views:
                tasks.append(self.cluster.async_delete_view(self.servers[0], view.name, None))
                time.sleep(1)
            for view in data_set.views:
                for q in view.queries:
                    q.error = error
            self._query_all_views(data_set.views, False)
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
        docs_per_day = self.input.param('docs-per-day', 200)
        params = self.input.param('query_params', {})
        nodes_num_to_add = self.input.param('nodes_to_add', 0)
        if nodes_num_to_add:
            rebalance = self.cluster.async_rebalance(self.servers[:nodes_num_to_add + 1], self.servers[1 : nodes_num_to_add + 1], [])
            rebalance.result()
        data_set = SalesDataSet(self._rconn(), docs_per_day, limit=self.limit)
        data_set.load(self, data_set.views[0], docs_per_day)
        data_set.add_reduce_queries(params)
        self._query_all_views(data_set.views, limit=self.limit)

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
        docs_per_day = self.input.param('docs-per-day', 200)
        skip = self.input.param('skip', 2000)
        nodes_num_to_add = self.input.param('nodes_to_add', 0)
        if nodes_num_to_add:
            rebalance = self.cluster.async_rebalance(self.servers[:nodes_num_to_add + 1], [self.servers[1 : nodes_num_to_add + 1]], [])
            rebalance.result()
        data_set = SalesDataSet(self._rconn(), docs_per_day, limit=self.limit, test_datatype=True)
        data_set.add_skip_queries(skip=skip, limit=self.limit)
        data_set.load(self, data_set.views[0], docs_per_day)
        if self.wait_persistence:
            for server in self.servers:
                RebalanceHelper.wait_for_persistence(server, data_set.bucket)
        self._query_all_views(data_set.views, limit=self.limit)

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
        docs_per_day = self.input.param('docs-per-day', 200)
        skip = self.input.param('skip', 2000)
        nodes_num_to_add = self.input.param('nodes_to_add', 0)
        num_items = self.input.param('num_items', 55)
        if nodes_num_to_add:
            rebalance = self.cluster.async_rebalance(self.servers[:nodes_num_to_add + 1], self.servers[1 : nodes_num_to_add + 1], [])
            rebalance.result()
        data_set = SalesDataSet(self._rconn(), docs_per_day, limit=self.limit, template_items_num=num_items)
        data_set.add_skip_queries(skip=skip, limit=self.limit)
        data_set.load(self, data_set.views[0], docs_per_day)
        self._query_all_views(data_set.views, limit=self.limit)

    def test_expiration_docs_queries(self):
        data_set = ExpirationDataSet(self._rconn(),self.num_docs)
        data_set.load(self, data_set.views[0])
        data_set.query_verify_value(self)

    def test_flags_docs_queries(self):
        data_set = FlagsDataSet(self._rconn(),self.num_docs)
        data_set.load(self, data_set.views[0])
        data_set.query_verify_value(self)

    def test_employee_dataset_query_different_bucket_types(self):
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
            2. start querying multiply buckets, buckets are sasl with password
        '''
        docs_per_day = self.input.param('docs-per-day', 200)
        bucket_type = self.input.param('bucket-type', None)
        test_buckets = []
        for i in xrange(self.num_buckets):
            if bucket_type and bucket_type == 'sasl':
                test_buckets.append(Bucket(name="bucket-{0}".format(i), saslPassword="password"))
            else:
                test_buckets.append(Bucket(name="bucket-{0}".format(i)))
        data_sets = []
        for test_bucket in test_buckets:
            data_sets.append(EmployeeDataSet(self._rconn(), docs_per_day,
                                             bucket=test_bucket))
        for data_set in data_sets:
            data_set.add_startkey_endkey_queries()
            self._query_test_init(data_set, False)

        query_bucket_threads = []
        for data_set in data_sets:
            t = StoppableThread(target=self._query_all_views,
                                name="query-bucket-{0}".format(data_set.bucket.name),
                                args=(data_set.views,))
            query_bucket_threads.append(t)
            t.start()

        while True:
            if not query_bucket_threads:
                break
            self.thread_stopped.wait(60)
            if self.thread_crashed.is_set():
                for t in query_bucket_threads:
                    t.stop()
                break
            else:
                query_bucket_threads = [d for d in query_bucket_threads if d.is_alive()]
                self.thread_stopped.clear()
    ###
    # load the data defined for this dataset.
    # create views and query the data as it loads.
    # verification is optional, and best practice is to
    # set to False if you plan on running _query_all_views()
    # later in the test case
    ###
    def _query_test_init(self, data_set, verify_results = True, tm = None):
        views = data_set.views
        rest = self._rconn()
        load_task = None

        if tm is None:
            # start loading data using old method
            load_task = StoppableThread(target=data_set.load,
                               name="load_data_set",
                               args=(self, views[0]))
            load_task.start()
        else:
            load_task = data_set.load_with_tm(tm, rest)
            time.sleep(2)

        # run queries while loading data
        while(load_task.is_alive()):
            if (self.thread_crashed.is_set()):
                load_task.stop()
                self._check_view_intergrity(views)
                return
            self._query_all_views(views, False, limit=data_set.limit)
            time.sleep(5)
        if 'result' in dir(load_task):
            load_task.result()
        else:
            load_task.join()

        # results will be verified if verify_results set
        if verify_results:
            self._query_all_views(views, verify_results, data_set.kv_store, limit = data_set.limit)
        else:
            self._check_view_intergrity(views)


    ##
    # run all queries for all views in parallel
    ##
    def _query_all_views(self, views, verify_results = True, kv_store = None, limit=None,
                         verify_expected_keys=False):

        query_threads = []
        for view in views:
            t = StoppableThread(target=view.run_queries,
               name="query-{0}".format(view.name),
               args=(self, verify_results, kv_store, limit, verify_expected_keys))
            query_threads.append(t)
            t.start()

        while True:
            if not query_threads:
                return
            self.thread_stopped.wait(60)
            if self.thread_crashed.is_set():
                for t in query_threads:
                    t.stop()
                self._check_view_intergrity(views)
                return
            else:
                query_threads = [d for d in query_threads if d.is_alive()]
                self.thread_stopped.clear()
#        [t.join() for t in query_threads]

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
            msg += "Failure message is: %s\nTest case info:\n%s\nViews structure are:\n%s\n\n" %(
                                ex[1], getattr(self, self._testMethodName).func_doc, view_struct)
            report += msg
        return report

    # retrieve default rest connection associated with the master server
    def _rconn(self, server=None):
        if not server:
            server = self.servers[0]
        return RestConnection(server)

    @staticmethod
    def parse_string_to_dict(string_to_parse, separator_items=';', seprator_value='-'):
        if string_to_parse.find(separator_items) < 0:
            return dict([string_to_parse.split(seprator_value)] )
        else:
            return dict(item.split(seprator_value) for item in string_to_parse.split(separator_items))

class QueryView:
    def __init__(self, rest,
                 index_size,
                 bucket="default",
                 prefix=None,
                 name=None,
                 fn_str=None,
                 reduce_fn=None,
                 queries=None,
                 create_on_init=True,
                 type_filter=None,
                 consistent_view=False,
                 ddoc_options=None):

        self.index_size = index_size

        self.log = logger.Logger.get_logger()
        default_prefix = str(uuid.uuid4())[:7]
        default_fn_str = 'function (doc) {if(doc.name) { emit(doc.name, doc);}}'

        self.bucket = bucket
        self.prefix = (prefix, default_prefix)[prefix is None]
        default_name = "test_view-{0}".format(self.prefix)
        self.name = (name, default_name)[name is None]
        self.fn_str = (fn_str, default_fn_str)[fn_str is None]
        self.reduce_fn = reduce_fn
        self.results = unittest.TestResult()
        self.type_filter = type_filter or None
        self.consisent_view = consistent_view

        # queries defined for this view
        self.queries = (queries, list())[queries is None]

        if create_on_init:
            rest.create_view(self.name, self.bucket, [View(self.name, self.fn_str, self.reduce_fn, dev_view=False)], ddoc_options)

    # query this view
    def run_queries(self, tc, verify_results = False, kv_store = None, limit=None,
                    verify_expected_keys=False):
        try:
            rest = tc._rconn(tc.server)

            if not len(self.queries) > 0:
                self.log.info("No queries to run for this view")
                return

            view_name = self.name

            max_dupe_result_count = tc.input.param('max-dupe-result-count', 5)
            num_verified_docs = tc.input.param('num-verified-docs', 20)

            for query in self.queries:
                params = query.params
                attempts_num = tc.input.param('attempt-num', 15)
                attempts_num = query.attempt_num or attempts_num
                num_tries = None
                if attempts_num < 15:
                    num_tries = attempts_num
                params["debug"] = "true"

                if self.reduce_fn is not None and "include_docs" in params:
                    del params["include_docs"]

                expected_num_docs = query.expected_num_docs
                num_keys = -1

                if expected_num_docs is not None and verify_results:

                    attempt = 0
                    delay = 15
                    results = None

                    # first verify all doc_names get reported in the view
                    # for windows, we need more than 20+ times
                    result_count_stats = {}
                    while attempt < attempts_num and num_keys != expected_num_docs:
                        if attempt > 11:
                            params["stale"] = 'false'

                        self.log.info("Quering view {0} with params: {1}".format(view_name, params))
                        results = ViewBaseTests._get_view_results(tc, rest, self.bucket, view_name,
                                                                      limit=limit, extra_params=params,
                                                                      type_ = query.type_, num_tries=num_tries)
                       # check if this is a reduced query using _count
                        if self.reduce_fn and (not query.params.has_key("reduce") or query.params.has_key("reduce") and query.params["reduce"] == "true"):
                            if self.reduce_fn == "_count":
                                num_keys = self._verify_count_reduce_helper(query, results)
                                keys = ["group", "group_level", "key", "start_key", "end_key"]
                                if [key for key in keys if key in params]:
                                    self.log.info("{0}: attempt {1} reduced {2} group(s) to value {3} expected: {4}" \
                                        .format(view_name, attempt + 1, query.expected_num_groups,
                                                num_keys, expected_num_docs))
                                else:
                                    self.log.info("{0}: attempt {1} reduced {2} group(s) to value {3} expected: {4}" \
                                        .format(view_name, attempt + 1, query.expected_num_groups,
                                                num_keys, self.index_size))
                                if self.index_size !=  num_keys or expected_num_docs != num_keys:
                                    attempt += 1
                                    continue
                                else:
                                    break
                            elif self.reduce_fn in ("_sum","_stats"):
                                try:
                                    num_keys = self._verify_count_reduce_helper(query, results, reduce_fn=self.reduce_fn)
                                except Exception as ex:
                                    attempt += 1
                                    if attempt == attempts_num:
                                        raise Exception("After 14 attemps expected number of groups {0} is not reached. {1}".format(query.expected_num_groups, ex))
                                    else:
                                        continue
                                else:
                                    break
                        else:

                            num_keys = len(ViewBaseTests._get_keys(self, results))
                            self.log.info("{0}: attempt {1} retrieved value {2} expected: {3}" \
                                .format(view_name, attempt + 1, num_keys, expected_num_docs))

                        attempt += 1
                        if num_keys not in result_count_stats:
                            result_count_stats[num_keys] = 1
                        else:
                            if result_count_stats[num_keys] == max_dupe_result_count:
                                break
                            else:
                                result_count_stats[num_keys] += 1
                        if num_keys != expected_num_docs:
                            if num_keys > expected_num_docs:
                                break
                            else:
                                ViewBaseTests._wait_for_indexer_ddoc(tc, rest, view_name)

                        if num_keys == expected_num_docs and verify_expected_keys and not self.reduce_fn:
                            try:
                                QueryHelper.verify_query_ids_full(rest, query, results)
                                self.log.info("Keys in query results are equal to expected")
                            except Exception as e:
                                self.log.info("Keys in query results are not equal to expected. View {0}".format(view_name))
                                if str(e).find('Current ids and expected are not equal') != -1 and \
                                                not self.consisent_view:
                                    attempt += 1
                                    if attempt == attempts_num:
                                        raise Exception("After 14 attemps expected number of groups {0} is not reached. {1}".format(query.expected_num_groups, e))
                                    else:
                                        continue
                                else:
                                    raise e

                    try:
                        if(num_keys != expected_num_docs):
                            # debug query results
                            if self.reduce_fn is not None:
                                # query again with reduce false
                                params["reduce"] = "false"

                                # remove any grouping
                                if "group" in params:
                                    del params["group"]
                                if "group_level" in params:
                                    del params["group_level"]
                                if "key" in params and "limit" in params:
                                    expected_num_docs = min(expected_num_docs, limit)

                                results = ViewBaseTests._get_view_results(tc, rest,
                                                                          self.bucket,
                                                                          view_name,
                                                                          limit=limit,
                                                                          extra_params=params,
                                                                          type_ = query.type_,
                                                                          num_tries=num_tries)

                            # verify keys
                            key_failures = QueryHelper.verify_query_keys(rest, query,
                                                                         results, self.bucket,
                                                                         num_verified_docs, limit=limit)
                            msg = "unable to retrieve expected results: {0}".format(key_failures)
                            tc.assertEquals(len(key_failures), 0, msg)
                        # verify values for include_docs tests
                        if('include_docs' in params):
                            failures = QueryHelper.verify_query_values(rest, query, results, self.bucket)
                            msg = "data integrity failed: {0}".format(failures)
                            tc.assertEquals(len(failures), 0, msg)

                    except:
                        self.log.error("Query failed: see test result logs for details. View {0}".format(view_name))
                        self.results.addFailure(tc, sys.exc_info())
                        tc.log.error("Query data thread is crashed: %s" % sys.exc_info()[2])
                        tc.thread_crashed.set()

                else:
                    # query without verification
                    self.log.info("Quering view {0} with params: {1}".format(view_name, params));
                    try:
                        results = ViewBaseTests._get_view_results(tc, rest, self.bucket, view_name,
                                                                  limit=limit, extra_params=params,
                                                                  type_ = query.type_,
                                                                  invalid_results=query.error and True or False,
                                                                  num_tries=num_tries)
                    except Exception as ex:
                            if query.error:
                                if ex.message.find(query.error) > -1:
                                    self.log.info("View results contain '{0}' error as expected".format(query.error))
                                    return
                                else:
                                    self.log.error("View results expect '{0}' error but {1} raised".format(query.error, ex.message))
                                    self.results.addFailure(tc,(type(ex), ex.message, sys.exc_info()[2]))
                                    tc.thread_crashed.set()
                                    return
                            else:
                               self.log.error("Error {0} appeared during query run".format(ex))
                               self.results.addError(tc, (Exception, "{0}: {1}".format(ex, ex.message), sys.exc_info()[2]))
                               tc.thread_crashed.set()
                    if query.error:
                        self.log.error("No error raised for negative case. Expected error '{0}'".format(query.error))
                        self.results.addFailure(tc, (Exception, "No error raised for negative case", sys.exc_info()[2]))
                        tc.thread_crashed.set()
        except Exception as ex:
            self.log.error("Error {0} appeared during query run".format(ex))
            self.results.addError(tc, (Exception, "{0}: {1}".format(ex, ex.message), sys.exc_info()[2]))
            tc.thread_crashed.set()
        finally:
            if not tc.thread_stopped.is_set():
                tc.thread_stopped.set()


    """
        helper function for verifying results when _count reduce is used.
        by default 1 group is expected but when more specified
        a summation is derived

        result is compared with expected_num_docs of the query

        TODO: _sum,_stats? :)
    """
    def _verify_count_reduce_helper(self, query, results, reduce_fn='_count'):

        num_keys = 0
        if reduce_fn in ('_count'):
            for i in xrange(query.expected_num_groups):
                if i < len(results["rows"]):
                    num_keys += results["rows"][i]["value"]
        stats = query.expected_statistics
        if reduce_fn == '_sum':
            num_keys = len(results["rows"])
            for i in xrange(query.expected_num_groups):
                if i < len(results["rows"]):
                    if str(results["rows"][i]["key"]) not in stats:
                        raise Exception("key {0} is not expected".format(results["rows"][i]["key"]))
                    if stats[str(results["rows"][i]["key"])]["sum"] != results["rows"][i]["value"]:
                       raise Exception("Value for key '{0}' is {1}, expected is {2}".format(results["rows"][i]["key"],
                                                                                            results["rows"][i]["value"],
                                                                                            stats[str(results["rows"][i]["key"])]["sum"]))
                    self.log.info("Sum for key {0} is verified and matches expected: {1}".format(results["rows"][i]["key"],
                                                                                                 results["rows"][i]["value"]))
        if reduce_fn == '_stats':
            num_keys = len(results["rows"])
            expected_fn = ("sum", "count", "min", "max", "sumsqr")
            for i in xrange(query.expected_num_groups):
                if i < len(results["rows"]):
                    if str(results["rows"][i]["key"]) not in stats:
                            raise Exception("key {0} is not expected".format(results["rows"][i]["key"]))
                    for fn in expected_fn:
                        if stats[str(results["rows"][i]["key"])][fn] != results["rows"][i]["value"][fn]:
                           raise Exception("{3} for key '{0}' is {1}, expected is {2}".format(results["rows"][i]["key"],
                                                                                              results["rows"][i]["value"][fn],
                                                                                              stats[str(results["rows"][i]["key"])][fn],
                                                                                              fn))
                        self.log.info("{2} for key {0} is verified and matches expected: {1}".format(results["rows"][i]["key"],
                                                                                                     results["rows"][i]["value"][fn],
                                                                                                     fn))
        return num_keys

class EmployeeDataSet:
    def __init__(self, rest, docs_per_day = 200, bucket = "default", limit=None, ddoc_options=None):
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
        self.bucket = bucket
        self.ddoc_options = ddoc_options
        self.views = self.create_views(rest, bucket=self.bucket)
        self.rest = rest
        self.name = "employee_dataset"
        self.kv_store = None
        self.doc_id_map = {}
        self.limit = limit

    def calc_total_doc_count(self):
        return self.years * self.months * self.days * self.docs_per_day * len(self.get_data_sets())

    def add_negative_query(self, query_params, error, views=None):
        views = views or self.views
        query_params_dict = ViewQueryTests.parse_string_to_dict(query_params, seprator_value='~')
        for view in views:
            view.queries += [QueryHelper(query_params_dict, None, error=error)]

    def add_query_invalid_startkey_endkey_docid(self, valid_query_params, invalid_query_params, views=None):
        views = views or self.views
        if valid_query_params:
            if valid_query_params.find(';') < 0:
                query_params_dict = dict([valid_query_params.split("-")])
            else:
                query_params_dict = dict(item.split("-") for item in valid_query_params.split(";"))
        if invalid_query_params.find(';') < 0:
            invalid_query_params_dict = dict([invalid_query_params.split("-")] )
        else:
            invalid_query_params_dict = dict(item.split("-") for item in invalid_query_params.split(";"))

        for view in views:
            expected_docs = 0
            offset  = (self.docs_per_day, self.docs_per_day * len(self.get_data_sets()))[view.index_size == self.calc_total_doc_count()]

            if "start_key" in query_params_dict:
                start_year, start_mo, start_day = [int(item) for item in query_params_dict["start_key"][1:-1].split(',')]
            else:
                start_year, start_mo, start_day = [2007, 1, 1]
            if "end_key" in query_params_dict:
                end_year, end_mo, end_day = [int(item) for item in query_params_dict["end_key"][1:-1].split(',')]
            else:
                end_year, end_mo, end_day = [2007 + self.years + 1, self.months, self.days + 1]
            expected_docs = offset * (end_year - start_year) * ((end_mo - start_mo) * self.days  + (end_day - start_day))

            if query_params_dict:
                query_params_dict.update(invalid_query_params_dict)
            else:
                query_params_dict = invalid_query_params_dict
            view.queries += [QueryHelper(query_params_dict, expected_docs)]

    def add_startkey_endkey_docid_queries_extra_params(self, views=None, extra_params=None):
        # only select views that will index entire dataset
        views = views or [view for view in self.views
                          if view.index_size == self.calc_total_doc_count()]
        extra_params_dict = {}
        import types
        if extra_params and type(extra_params) != types.DictType:
            extra_params_dict = ViewQueryTests.parse_string_to_dict(extra_params)

        for view in views:

            # pre-calculating expected key size of query between
            # [2008,2,20] and [2008,7,1] with descending set
            # based on dataset
            all_docs_per_day = len(self.get_data_sets()) * self.docs_per_day
            expected_num_keys = 9 * all_docs_per_day + 4 * self.days * all_docs_per_day - self.docs_per_day + 1

            startkey = "[2008,2,20]"
            endkey = "[2008,7,1]"
            startkey_docid = "arch0000-2008_02_20"
            endkey_docid = "admin0000-2008_07_01"

            if 'limit' in extra_params_dict:
                expected_num_keys = view.reduce_fn and expected_num_keys or min(int(extra_params_dict['limit']), expected_num_keys)

            if 'descending' in extra_params_dict and \
                extra_params_dict['descending'] == 'true':
                tmp = endkey
                endkey = startkey
                startkey = tmp
                tmp = endkey_docid
                endkey_docid = startkey_docid
                startkey_docid = tmp

            if 'inclusive_end' in extra_params_dict and \
                extra_params_dict['inclusive_end'] == 'false':
                expected_num_keys -= 1

            if 'skip' in extra_params_dict:
                if view.reduce_fn:
                    self.views.remove(view)

            view.queries += [QueryHelper({"start_key" : startkey,
                                          "startkey_docid" : startkey_docid,
                                          "end_key"   : endkey,
                                          "endkey_docid"   : endkey_docid},
                                         expected_num_keys)]

            if extra_params_dict:
                for q in view.queries:
                    q.params.update(extra_params_dict)

    def add_startkey_endkey_queries(self, views=None, limit=None):
        if views is None:
            views = self.views
        if limit is None:
            limit = self.limit

        for view in views:
            index_size = view.index_size
            offset = self.docs_per_day

             # offset includes all data types if
             # indexing entire data_set
            if index_size == self.calc_total_doc_count():
                offset = offset * len(self.get_data_sets())

            expected_num_docs = index_size/2
            expected_num_docs_offset = index_size/2 + offset

            if limit and not view.reduce_fn:
                   expected_num_docs = min(expected_num_docs, limit)
                   expected_num_docs_offset = min(expected_num_docs_offset, limit)

            view.queries += [QueryHelper({"start_key" : "[2008,7,null]"},
                                         expected_num_docs),
                             QueryHelper({"start_key" : "[2008,0,1]",
                                          "end_key"   : "[2008,7,1]",
                                          "inclusive_end" : "false"},
                                         expected_num_docs),
                             QueryHelper({"start_key" : "[2008,0,1]",
                                          "end_key"   : "[2008,7,1]",
                                          "inclusive_end" : "true"},
                                         expected_num_docs_offset),
                             QueryHelper({"start_key" : "[2008,7,1]",
                                          "end_key"   : "[2008,1,1]",
                                          "descending"   : "true",
                                          "inclusive_end" : "false"},
                                         expected_num_docs),
                             QueryHelper({"start_key" : "[2008,7,1]",
                                          "end_key"   : "[2008,1,1]",
                                          "descending"   : "true",
                                          "inclusive_end" : "true"},
                                          expected_num_docs_offset),
                             QueryHelper({"start_key" : "[2008,1,1]",
                                          "end_key"   : "[2008,7,1]",
                                          "descending"   : "false",
                                          "inclusive_end" : "false"},
                                         expected_num_docs),
                             QueryHelper({"start_key" : "[2008,1,1]",
                                          "end_key"   : "[2008,7,1]",
                                          "descending"   : "false",
                                          "inclusive_end" : "true"},
                                         expected_num_docs_offset)]
            if limit:
                for query in view.queries:
                    query.params["limit"] = limit

    def add_skip_queries(self, skip, limit=None, views=None):
        if views is None:
            views = self.views

        for view in views:
            limit = self.limit or 1
            limit = min(limit, view.index_size - int(skip))

            #empty results will be returned
            if view.reduce_fn:
                views.remove(view)
            else:
                view.queries += [QueryHelper({"skip" : skip, "limit" : str(limit)}, limit)]

    def add_key_queries(self, views=None, limit=None):
        if views is None:
            views = self.views
        if limit is None:
            limit = self.limit

        for view in views:
             # offset includes all data types if
             # indexing entire data_set

            if view.index_size == self.calc_total_doc_count():
                expected_num_docs = self.docs_per_day * len(self.get_data_sets())
            else:
                expected_num_docs = self.docs_per_day

            if limit:
               if not view.reduce_fn:
                   expected_num_docs = min(expected_num_docs, limit)
               view.queries += [QueryHelper({"key" : "[2008,7,1]", "limit" : limit}, 
                                            expected_num_docs)]
            else:
                view.queries += [QueryHelper({"key" : "[2008,7,1]"},
                                             expected_num_docs)]

    def add_all_docs_queries(self, views=None, limit=None):

        if views is None:
            views = []

            # only select views that will index entire dataset
            # and do not have a reduce function
            # if user doesn't override
            for view in self.views:
                if view.index_size == self.calc_total_doc_count():
                    if view.reduce_fn is None:
                        views.append(view)

        for view in views:
            index_size = view.index_size

            section_size = index_size/len(self.get_data_sets())

            limit = limit or self.limit

            if limit:
                view.queries += [QueryHelper({"start_key" : '"arch0000-2008_10_01"',
                                              "limit" : limit},
                                             min(limit, index_size - section_size - self.days * 9)),
                             QueryHelper({"start_key" : '"ui0000-2008_10_01"',
                                          "limit" : limit},
                                         min(limit, index_size  - section_size * 2 - self.days * 9)),
                             QueryHelper({"start_key" : '"arch0000-2008_10_01"',
                                          "end_key"   : '"ui0000-2008_10_01"',
                                          "inclusive_end" : "false",
                                          "limit" : limit},
                                         min(limit, index_size - section_size * 2))]
            else:
                view.queries += [QueryHelper({"start_key": '"arch0000-2008_10_01"'},
                                             index_size - section_size - self.days * 9),
                                 QueryHelper({"start_key" : '"ui0000-2008_10_01"'},
                                             index_size  - section_size * 2 - self.days * 9),
                                 QueryHelper({"start_key" : '"arch0000-2008_10_01"',
                                              "end_key"   : '"ui0000-2008_10_01"',
                                              "inclusive_end" : "false"},
                                             index_size - section_size * 2)]
                                 # test design docs are included when start_key not specified
                                 # TODO: cannot verify this query unless we store view names in
                                 #       doc_id_map
                                 #QueryHelper({"end_key" : '"ui0000-2008_10_01"',
                                 #             "inclusive_end" : "false"},
                                 #             index_size - section_size + 9*self.days + len(self.views))]

            # set all_docs flag
            for query in view.queries:
                query.type_ = "all_docs"


    """
        Create queries for testing docids on duplicate start_key result ids.
        Only pass in view that indexes all employee types as they are
        expected in the query params...i.e (ui,admin,arch)
    """
    def add_startkey_endkey_docid_queries(self, views=None, limit=None):
        if limit is None:
            limit = self.limit
        if views is None:
            views = []

            # only select views that will index entire dataset
            # if user doesn't override
            for view in self.views:
                if view.index_size == self.calc_total_doc_count():
                    views.append(view)


        for view in views:
            index_size = view.index_size
            offset = self.docs_per_day


            # pre-calculating expected key size of query between
            # [2008,2,20] and [2008,7,1] with descending set
            # based on dataset
            all_docs_per_day = len(self.get_data_sets()) * offset
            complex_query_key_count = 9*all_docs_per_day + 4*self.days*all_docs_per_day \
                                        + all_docs_per_day
            if limit:
                view.queries += [QueryHelper(
                                        {"start_key" : "[2008,7,1]",
                                         "startkey_docid" : "arch0000-2008_07_01",
                                         "limit" : limit}, view.reduce_fn and index_size/2 - offset or min(limit, index_size/2 - offset)),
                                QueryHelper(
                                        {"start_key" : "[2008,7,1]",
                                         "startkey_docid" : "arch0000-2008_07_01",
                                         "descending" : "false",
                                         "limit" : limit}, view.reduce_fn and index_size/2 - offset or min(limit, index_size/2 - offset)),
                                 QueryHelper(
                                        {"start_key" : "[2008,7,1]",
                                         "startkey_docid" : "arch0000-2008_07_01",
                                         "descending" : "true",
                                         "limit" : limit}, view.reduce_fn and index_size/2 + offset + 1 or min(limit, index_size/2 + offset + 1)),
                                QueryHelper({"start_key" : "[2008,7,1]",
                                             "startkey_docid" : "ui0000-2008_07_01",
                                             "limit" : limit}, view.reduce_fn and index_size/2 - offset*2 or min(limit, index_size/2 - offset*2)),
                                QueryHelper({"start_key" : "[2008,7,1]",
                                             "startkey_docid" : "ui0000-2008_07_01",
                                             "descending" : "false",
                                             "limit" : limit}, view.reduce_fn and index_size/2 - offset*2 or min(limit, index_size/2 - offset*2)),
                                 QueryHelper({"start_key" : "[2008,7,1]",
                                             "startkey_docid" : "ui0000-2008_07_01",
                                             "descending" : "true",
                                             "limit" : limit}, view.reduce_fn and index_size/2 + offset*2 +1 or min(limit, index_size/2 + offset*2 + 1)),
                                              # +endkey_docid
                                QueryHelper({"start_key" : "[2008,0,1]",
                                             "end_key"   : "[2008,7,1]",
                                             "endkey_docid" : "arch0000-2008_07_01",
                                             "inclusive_end" : "false",
                                             "limit" : limit}, view.reduce_fn and index_size/2 + offset or min(limit, index_size/2 + offset)),
                                QueryHelper({"end_key"   : "[2008,7,1]",
                                             "endkey_docid" : "ui0000-2008_07_01",
                                             "inclusive_end" : "false",
                                             "limit" : limit}, view.reduce_fn and index_size/2 + offset*2 or min(limit, index_size/2 + offset*2)),
                                              # + inclusive_end
                                QueryHelper({"end_key"   : "[2008,7,1]",
                                             "endkey_docid" : "arch0000-2008_07_01",
                                             "inclusive_end" : "true",
                                             "limit" : limit}, view.reduce_fn and index_size/2 + offset + 1 or min(limit, index_size/2 + offset + 1)),
                                              # + single bounded and descending
                                QueryHelper({"start_key" : "[2008,7,1]",
                                             "end_key"   : "[2008,2,20]",
                                             "endkey_docid"   : "ui0000-2008_02_20",
                                             "descending"   : "true",
                                             "inclusive_end" : "true",
                                             "limit" : limit}, view.reduce_fn and complex_query_key_count - offset * 2 or min(limit, complex_query_key_count - offset * 2)),
                                QueryHelper({"start_key" : "[2008,7,1]",
                                             "end_key"   : "[2008,2,20]",
                                             "endkey_docid"   : "arch0000-2008_02_20",
                                             "descending"   : "true",
                                             "inclusive_end" : "false",
                                             "limit" : limit}, view.reduce_fn and complex_query_key_count - offset - 1 or min(limit, complex_query_key_count - offset - 1)),
                                              # + double bounded and descending
                                QueryHelper({"start_key" : "[2008,7,1]",
                                             "startkey_docid" : "admin0000-2008_07_01",
                                             "end_key"   : "[2008,2,20]",
                                             "endkey_docid"   : "arch0000-2008_02_20",
                                             "descending"   : "true",
                                             "inclusive_end" : "false",
                                             "limit" : limit},
                                              view.reduce_fn and complex_query_key_count - offset - all_docs_per_day or min(limit, complex_query_key_count - offset - all_docs_per_day))]
            else:
                view.queries += [QueryHelper(
                                        {"start_key" : "[2008,7,1]",
                                          "startkey_docid" : "arch0000-2008_07_01"}, index_size/2  - offset),
                                QueryHelper({"start_key" : "[2008,7,1]",
                                              "startkey_docid" : "ui0000-2008_07_01"}, index_size/2  - offset*2),
                                              # +endkey_docid
                                QueryHelper({"start_key" : "[2008,0,1]",
                                             "end_key"   : "[2008,7,1]",
                                             "endkey_docid" : "arch0000-2008_07_01",
                                             "inclusive_end" : "false"}, index_size/2 + offset),
                                QueryHelper({"end_key"   : "[2008,7,1]",
                                             "endkey_docid" : "ui0000-2008_07_01",
                                             "inclusive_end" : "false"}, index_size/2 + offset*2),
                                              # + inclusive_end
                                QueryHelper({"end_key"   : "[2008,7,1]",
                                             "endkey_docid" : "arch0000-2008_07_01",
                                             "inclusive_end" : "true"}, index_size/2 + offset + 1),
                                              # + single bounded and descending
                                QueryHelper({"start_key" : "[2008,7,1]",
                                             "end_key"   : "[2008,2,20]",
                                             "endkey_docid"   : "ui0000-2008_02_20",
                                             "descending"   : "true",
                                             "inclusive_end" : "true"}, complex_query_key_count - offset * 2),
                                QueryHelper({"start_key" : "[2008,7,1]",
                                             "end_key"   : "[2008,2,20]",
                                             "endkey_docid"   : "arch0000-2008_02_20",
                                             "descending"   : "true",
                                             "inclusive_end" : "false"}, complex_query_key_count - offset - 1),
                                              # + double bounded and descending
                                QueryHelper({"start_key" : "[2008,7,1]",
                                             "startkey_docid" : "admin0000-2008_07_01",
                                             "end_key"   : "[2008,2,20]",
                                             "endkey_docid"   : "arch0000-2008_02_20",
                                             "descending"   : "true",
                                             "inclusive_end" : "false"},
                                             complex_query_key_count - offset - all_docs_per_day)]


    def add_stale_queries(self, views=None, limit=None, stale_param=None):
        if views is None:
            views = self.views
        if limit is None:
            limit = self.limit

        for view in views:
            if stale_param:
                view.queries += [QueryHelper({"stale" :  stale_param}, view.index_size),]
            elif limit:
                view.queries += [QueryHelper({"stale" : "false", "limit" : limit}, min(limit, view.index_size)),
                                 QueryHelper({"stale" : "ok", "limit" : limit}, min(limit, view.index_size)),
                                 QueryHelper({"stale" : "update_after", "limit" : limit}, min(limit, view.index_size))]
            else:
                view.queries += [QueryHelper({"stale" : "false"}, view.index_size),
                                 QueryHelper({"stale" : "ok"}, view.index_size),
                                 QueryHelper({"stale" : "update_after"}, view.index_size)]


    """
        group queries should only be added to views with reduce views.
        in this particular case, _count function is expected.
        if no specific views are passed in, we'll just figure it out.

        verification requries that the expected number of groups generated
        by the query is provided.  each group will generate a value that
        when summed, should add up to the number of indexed docs
    """
    def add_group_count_queries(self, views=None, limit=None):
        if limit is None:
            limit = self.limit

        for view in self.views:
            if view.reduce_fn is None:
                continue
            if limit:
                view.queries += [QueryHelper({"group" : "true", "limit" : limit},
                                             min(limit,view.index_size),
                                             min(limit, self.years * self.months * self.days)),
                                 QueryHelper({"group_level" : "1", "limit" : limit},
                                             min(limit,view.index_size),
                                             min(limit, self.years)),
                                 QueryHelper({"group_level" : "2", "limit" : limit},
                                             min(limit,view.index_size),
                                             min(limit, self.years * self.months)),
                                 QueryHelper({"group_level" : "3", "limit" : limit},
                                             min(limit,view.index_size),
                                             min(limit, self.years * self.months * self.days))]
            else:
                view.queries += [QueryHelper({"group" : "true"}, view.index_size,
                                             self.years * self.months * self.days),
                                 QueryHelper({"group_level" : "1"}, view.index_size,
                                             self.years),
                                 QueryHelper({"group_level" : "2"}, view.index_size,
                                             self.years * self.months),
                                 QueryHelper({"group_level" : "3"}, view.index_size,
                                             self.years * self.months * self.days)]
            for q in view.queries:
                if "group" in q.params and not "group_level" in q.params:
                    q.expected_num_groups = 1

    def add_all_query_sets(self, views=None, limit=None):
        self.add_stale_queries(views, limit)
        self.add_startkey_endkey_queries(views, limit)
        self.add_startkey_endkey_docid_queries(views, limit)
        self.add_group_count_queries(views)

    # views for this dataset
    def create_views(self, rest, bucket="default"):
        vfn1 = 'function (doc) { if(doc.job_title !== undefined) { var myregexp = new RegExp("^UI "); if(doc.job_title.match(myregexp)){ emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}}'
        vfn2 = 'function (doc) { if(doc.job_title !== undefined) { var myregexp = new RegExp("^System "); if(doc.job_title.match(myregexp)){ emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}}'
        vfn3 = 'function (doc) { if(doc.job_title !== undefined) { var myregexp = new RegExp("^Senior "); if(doc.job_title.match(myregexp)){ emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}}'
        vfn4 = 'function (doc) { if(doc.job_title !== undefined) emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] ); }'
        vfn5 = 'function (doc, meta) { if(doc.job_title !== undefined) { var myregexp = new RegExp("^admin"); if(meta.id.match(myregexp)) { emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}}'

        full_index_size = self.calc_total_doc_count()
        partial_index_size = full_index_size/3

        return [QueryView(rest, full_index_size,    bucket=bucket, fn_str = vfn4,
                          ddoc_options=self.ddoc_options),
                QueryView(rest, partial_index_size, bucket=bucket, fn_str = vfn1, type_filter = "ui",
                          ddoc_options=self.ddoc_options),
                QueryView(rest, partial_index_size, bucket=bucket, fn_str = vfn2, type_filter = "admin",
                          ddoc_options=self.ddoc_options),
                QueryView(rest, partial_index_size, bucket=bucket, fn_str = vfn3, type_filter = "arch",
                          ddoc_options=self.ddoc_options),
                QueryView(rest, full_index_size,    bucket=bucket, fn_str = vfn4, reduce_fn="_count",
                          ddoc_options=self.ddoc_options),
                QueryView(rest, partial_index_size, bucket=bucket, fn_str = vfn5, type_filter = "admin",
                          ddoc_options=self.ddoc_options)]

    def get_data_sets(self):
        return [self.sys_admin_info, self.ui_eng_info, self.senior_arch_info]

    def load(self, tc, view, verify_docs_loaded = True):
        data_threads = []
        for info in self.get_data_sets():

            self.doc_id_map.update({info['type'] : {"years" : self._doc_map_array()}})
            t = StoppableThread(target=self._iterative_load,
                       name="iterative_load",
                       args=(info, tc, view, self.docs_per_day, verify_docs_loaded))
            data_threads.append(t)
            t.start()
        while True:
            if not data_threads:
                return
            tc.thread_stopped.wait(60)
            if tc.thread_crashed.is_set():
                for t in data_threads:
                    t.stop()
                return
            else:
                data_threads = [d for d in data_threads if d.is_alive()]
                tc.thread_stopped.clear()
        if not view.queries[0].error:
            self.preload_matching_query_keys()


    # create new array with a None item at index 0 for
    # doc_map_id which is used for '1' based lookups
    def _doc_map_array(self):
        array_ = []
        array_.append(None)
        return array_


    def _iterative_load(self, info, tc, view, loads_per_iteration, verify_docs_loaded):
        try:
            smart = VBucketAwareMemcached(self.rest, self.bucket)
            for i in range(1,self.years + 1):
                self.doc_id_map[info['type']]['years'].append(i)
                self.doc_id_map[info['type']]['years'][i] =\
                    { "months" : self._doc_map_array()}
                for j in range(1, self.months + 1):
                    self.doc_id_map[info['type']]['years'][i]['months'].append(j)
                    self.doc_id_map[info['type']]['years'][i]['months'][j] =\
                        {"days" : self._doc_map_array()}
                    doc_sets = []
                    for k in range(1, self.days + 1):
                        self.doc_id_map[info['type']]['years'][i]['months'][j]['days'].append(k)
                        self.doc_id_map[info['type']]['years'][i]['months'][j]['days'][k]=\
                            {"docs" : []}

                        kv_template = {"name": "employee-${prefix}-${seed}",
                                       "join_yr" : 2007+i, "join_mo" : j, "join_day" : k,
                                       "email": "${prefix}@couchbase.com",
                                       "job_title" : info["title"].encode("utf-8","ignore"),
                                       "type" : info["type"].encode("utf-8","ignore"),
                                       "desc" : info["desc"].encode("utf-8", "ignore")}
                        options = {"size": 256, "seed":  str(uuid.uuid4())[:7]}
                        docs = DocumentGenerator.make_docs(loads_per_iteration, kv_template, options)
                        doc_sets.append(docs)
                    # load docs
                    self._load_chunk(smart, doc_sets)
        except Exception as ex:
            view.results.addError(tc, sys.exc_info())
            tc.log.error("At least one of load data threads is crashed: {0}".format(ex))
            tc.thread_crashed.set()
            raise ex
        finally:
            if not tc.thread_stopped.is_set():
                tc.thread_stopped.set()

    def _load_chunk(self, smart, doc_sets):

        doc_ids = []
        for docs in doc_sets:
            idx = 0
            for value in docs:
                value = value.encode("utf-8", "ignore")
                json_map = json.loads(value, encoding="utf-8")
                json_map = json_map["json"]
                type_ = json_map["type"]
                year = json_map["join_yr"]
                month = json_map["join_mo"]
                day = json_map["join_day"]


                doc_id = "{0}{1}-{2}_{3}_{4}".format(type_,
                                                     str(idx).zfill(4),
                                                     year,
                                                     str(month).rjust(2,'0'),
                                                     str(day).rjust(2,'0'))

                smart.memcached(doc_id).set(doc_id, 0, 0, json.dumps(json_map))
                doc_ids.append(doc_id)

                # update doc_id map
                self.doc_id_map[type_]['years'][year-2007]\
                    ['months'][month]['days'][day]['docs'].append(doc_id)

                idx += 1

        return doc_ids

    def preload_matching_query_keys(self):
        # get all queries defined in this data_set
        for v in self.views:
            for q in v.queries:
                self._preload_matching_query_keys(q, v.type_filter)

    def _preload_matching_query_keys(self, query, type_filter = None):
        inclusive_end = True
        descending = False

        q_start_yr  = 1
        q_start_mo  = 1
        q_start_day = 1

        q_end_yr  = self.years
        q_end_mo  = self.months
        q_end_day = self.days

        q_params = copy.deepcopy(query.params)

        if query.type_ == "all_docs":
            if 'start_key' in q_params and q_params['start_key']:
                q_params['startkey_docid'] = q_params['start_key']
                del q_params['start_key']

            if 'end_key' in q_params and q_params['end_key']:
                q_params['endkey_docid'] = q_params['end_key']
                del q_params['end_key']

        if 'start_key' in q_params  and q_params['start_key']:
            params = json.loads(q_params['start_key'])
            if params[0] and not None:
                q_start_yr = params[0] - 2007
            if params[1] and not None:
                q_start_mo = params[1]
            if params[2] and not None:
                q_start_day = params[2]

        if 'end_key' in q_params  and q_params['end_key']:
            params = json.loads(q_params['end_key'])
            if params[0] and not None:
                q_end_yr = params[0] - 2007
            if params[1] and not None:
                q_end_mo = params[1]
            if params[2] and not None:
                q_end_day = params[2]

        if 'descending' in q_params  and q_params['descending']:
            descending = json.loads(q_params['descending'])
            if descending == True:
                q_start_yr, q_end_yr = q_end_yr, q_start_yr
                q_start_mo, q_end_mo = q_end_mo, q_start_mo
                q_start_day, q_end_day = q_end_day, q_start_day

        if type_filter is None:
            types = [type_ for type_ in self.doc_id_map]
        else:
            types = [type_filter]

        type_idx = 0
        for doc_type in types:
            for years in self.doc_id_map[doc_type]['years'][q_start_yr:q_end_yr + 1]:
                mo_idx = 1
                days_skipped_offset = 0
                for months in years['months'][q_start_mo:q_end_mo + 1]:
                    # at end month only include up to N days
                    if (mo_idx + q_start_mo - 1) == q_end_mo:
                        mo_days = months['days'][1:q_end_day + 1]
                    else:
                        if mo_idx == 1:
                            # at beginning of month skip first N docs
                            mo_days = months['days'][q_start_day:]

                            if q_start_day > 1:
                                days_skipped_offset = (q_start_day)*self.docs_per_day*(type_idx + 1) \
                                    - 2*self.docs_per_day
                        else:
                            mo_days = months['days'][1:]

                    day_idx = 0
                    for days in mo_days:

                        # insert expected keys for query
                        doc_idx = 0
                        for id in days['docs']:

                            # order insertion according to view collation algorithm
                            # so that we can do easy comparison and docid matches later
                            # TODO: require pyicu package in testrunner and sort
                            if type_idx > 0:
                                day_offset = (type_idx)*self.docs_per_day
                                month_offset = 0
                                if day_idx > 0:
                                    day_offset = day_offset*(day_idx + 1) + self.docs_per_day*(day_idx)
                                day_offset += doc_idx
                                if mo_idx > 1:
                                    month_offset = 2*(mo_idx - 1)*self.docs_per_day*self.days\
                                        + self.docs_per_day*self.days*(type_idx - 1)*(mo_idx - 1)\
                                        - days_skipped_offset

                                ins_pos = day_offset + month_offset

                                # add record to expected output
                                query.expected_keys.insert(ins_pos, id)
                            else:
                                query.expected_keys.append(id)
                            doc_idx += 1
                        day_idx += 1
                    mo_idx += 1
            type_idx += 1

        if query.type_ == "all_docs":
            # use ascii sort
            query.expected_keys.sort()

        if descending == True:
            query.expected_keys.reverse()

        if 'startkey_docid' in q_params:
            startkey_docid = q_params['startkey_docid'].strip("\"")
            try:
                start_idx = query.expected_keys.index(startkey_docid)
                query.expected_keys = query.expected_keys[start_idx:]
            except ValueError:
                pass

        if 'endkey_docid' in q_params:
            endkey_docid = q_params['endkey_docid'].strip("\"")
            try:
                end_idx = query.expected_keys.index(endkey_docid)
                query.expected_keys = query.expected_keys[:end_idx + 1]
            except ValueError:
                pass
            if inclusive_end == False:
                query.expected_keys = query.expected_keys[:-1]
        if 'skip' in q_params:
            query.expected_keys = query.expected_keys[int(q_params['skip']) + 1:]

        # note: inclusive end check must occur after descending
        if 'inclusive_end' in q_params  and q_params['inclusive_end']:
            inclusive_end = json.loads(q_params['inclusive_end'])
            if inclusive_end == False and 'endkey_docid' not in q_params:
                end_key = eval(q_params['end_key'])
                query.expected_keys.reverse()
                end_idx = -1
                for key in query.expected_keys:
                    if key.find("{0}_{1}_{2}".format(end_key[0],
                                                     str(end_key[1]).zfill(2),
                                                     str(end_key[2]).zfill(2))) > -1:
                        end_idx = query.expected_keys.index(key)
                    else:
                        break
                if end_idx > -1:
                    query.expected_keys = query.expected_keys[end_idx + 1:]
                query.expected_keys.reverse()

    def wait_min_changes(self, tc, server, view, min_change, timeout=600):
        try:
            rest = tc._rconn(server)
            st = time.time()
            while (time.time() - st) < timeout:
                try:
                    update_num = ViewBaseTests.get_update_seq(self, rest, view)
                    tc.log.info("Update seq for %s:%s - %s is %s" %(
                                    server.ip, server.port,
                                    view.name, update_num))
                    if update_num >= min_change:
                        tc.assertTrue(ViewBaseTests.is_index_triggered(self, rest, view.name) or\
                                      ViewBaseTests.get_updates_num(self, rest, view) > 0,
                                  "Index for %s in %s:%s is not triggered!" %(
                                                view.name, server.ip, server.port))
                        tc.log.info("Index for %s in %s:%s is triggered successfully" %(
                                                view.name, server.ip, server.port))
                        return
                except Exception as ex:
                    if ex.message.find('missing') != -1:
                        time.sleep(5)
                    else:
                        raise ex
        except Exception as ex:
            view.results.addError(tc, sys.exc_info())
            tc.log.error("At least one of checking min changes threads is crashed: {0}".format(ex))
            tc.thread_crashed.set()
            raise ex
        finally:
            if not tc.thread_stopped.is_set():
                tc.thread_stopped.set()


class SimpleDataSet:
    def __init__(self, rest, num_docs, limit = None, reduce_fn=None, bucket='default',
                 name_ddoc=None, json_case=False):
        self.num_docs = num_docs
        self.name = name_ddoc
        self.json_case = json_case
        self.views = self.create_views(rest,reduce = reduce_fn)
        self.kv_store = ClientKeyValueStore()
        self.kv_template = {"name": "doc-${prefix}-${seed}-", "age": "${prefix}"}
        self.limit = limit
        self.reduce_fn = reduce_fn
        self.bucket = bucket

    def create_views(self, rest, reduce=None):
        view_fn = 'function (doc) {if(doc.age !== undefined) { emit(doc.age, doc.name);}}'
        if self.json_case:
            view_fn = 'function (doc) {if(doc.age !== undefined) { emit(doc.name, doc.age);}}'
        return [QueryView(rest, self.num_docs, fn_str=view_fn, reduce_fn=reduce, name=self.name)]

    def load(self, tc, view, verify_docs_loaded = True):
        try:
            doc_names = ViewBaseTests._load_docs(tc, self.num_docs, view.prefix, verify_docs_loaded)
            return doc_names
        except Exception as ex:
            view.results.addError(tc, sys.exc_info())
            tc.log.error("At least one of load data threads is crashed: {0}".format(ex))
            tc.thread_crashed.set()
            raise ex
        finally:
            if not tc.thread_stopped.is_set():
                tc.thread_stopped.set()

    def load_with_tm(self, task_manager, rest,
                     bucket = "default",
                     seed = None,
                     monitor = False):
        return \
            DataLoadHelper.add_doc_gen_task(task_manager, rest,
                                            self.num_docs,
                                            bucket = bucket,
                                            kv_template = self.kv_template,
                                            kv_store = self.kv_store,
                                            seed = seed,
                                            monitor = monitor)

    def add_negative_query(self, query_params, error, views=None):
        views = views or self.views
        query_params_dict = ViewQueryTests.parse_string_to_dict(query_params, seprator_value='~')
        for view in views:
            view.queries += [QueryHelper(query_params_dict, None, error=error)]

    def add_skip_queries(self, skip, limit=None, views=None):
        if views is None:
            views = self.views

        for view in views:
            limit = self.limit or 1
            limit = min(limit, view.index_size)

            view.queries += [QueryHelper({"skip" : skip, "limit" : str(limit)}, limit)]

    def add_startkey_endkey_queries(self, views=None, limit=None):

        if views is None:
            views = self.views

        for view in views:
            start_key = view.index_size/2
            end_key = view.index_size - 1000
            if limit is None:
                limit = self.limit

            if limit is not None:
                view.queries += [QueryHelper({"start_key" : end_key,
                                             "end_key" : start_key,
                                             "descending" : "true",
                                             "limit" : str(limit)},
                                             min(limit, end_key - start_key + 1)),
                                 QueryHelper({"start_key" : end_key,
                                             "end_key" : start_key,
                                             "descending" : "true",
                                             "limit" : str(limit)},
                                             min(limit, end_key - start_key + 1)),
                                 QueryHelper({"end_key" : end_key,
                                             "limit" : str(limit)},
                                             min(limit, end_key + 1)),
                                 QueryHelper({"end_key" : end_key,
                                             "inclusive_end" : "false",
                                             "limit" : limit}, min(limit, end_key)),
                                 QueryHelper({"start_key" : start_key,
                                              "limit" : str(limit)},
                                             min(limit, view.index_size - start_key))]
            else:
                view.queries += [QueryHelper({"start_key" : end_key,
                                             "end_key" : start_key,
                                             "descending" : "true"},
                                             end_key - start_key + 1),
                                 QueryHelper({"start_key" : end_key,
                                             "end_key" : start_key,
                                             "descending" : "true"},
                                             end_key - start_key + 1),
                                 QueryHelper({"end_key" : end_key},
                                             end_key + 1),
                                 QueryHelper({"end_key" : end_key,
                                             "inclusive_end" : "false"}, end_key),
                                 QueryHelper({"start_key" : start_key},
                                             view.index_size - start_key)]

    def add_startkey_endkey_non_json_queries(self, doc_names, symbol, views=None,
                                             limit=None):
        if views is None:
            views = self.views

        for view in views:
            view.queries = list()
            start_key = '"%s"' % doc_names[len(doc_names)/2]
            end_key = '"%s"' % (doc_names[len(doc_names)/2] + symbol)

            view.queries += [QueryHelper({"start_key" : end_key,
                                           "end_key" : start_key,
                                           "descending" : "true"},
                                          1),
                             QueryHelper({"end_key" : end_key},
                                         len(doc_names)/2 + 1),
                             QueryHelper({"end_key" : end_key,
                                          "inclusive_end" : "false"},
                                         len(doc_names)/2 + 1),
                             QueryHelper({"start_key" : start_key},
                                         len(doc_names)/2)]
            if limit:
                for q in view.queries:
                    q["limit"] = limit
                    q.expected_num_docs = min(q.expected_num_docs, limit)

    def add_stale_queries(self, views = None, limit= None, stale_param=None):
        if views is None:
            views = self.views

        for view in views:
            index_size = view.index_size
            if limit is None:
                limit = self.limit
                limit = limit < index_size and limit or index_size

            if stale_param:
                view.queries += [QueryHelper({"stale" :  stale_param, "limit" : str(limit)}, limit),]
            else:
                view.queries += [QueryHelper({"stale" : "false" , "limit" : str(limit)}, limit),
                                 QueryHelper({"stale" : "ok" , "limit" : str(limit)}, limit),
                                 QueryHelper({"stale" : "update_after" , "limit" : str(limit)}, limit)]

    def add_reduce_queries(self, views = None, limit= None):
        if views is None:
            views = self.views

        for view in views:
            index_size = view.index_size
            if limit is None:
                limit = self.limit

            view.queries += [QueryHelper({"reduce" : "false" , "limit" : str(limit)}, min(limit, index_size)),
                             QueryHelper({"reduce" : "true" , "limit" : str(limit)}, min(limit, index_size))]

    def add_all_query_sets(self, views=None, limit=None):
        self.add_startkey_endkey_queries(views, limit)
        self.add_stale_queries(views, limit)

class SalesDataSet:
    def __init__(self, rest, docs_per_day = 200, bucket = "default", limit=None, test_datatype=False, template_items_num=None):
        self.docs_per_day = docs_per_day
        self.years = 1
        self.months = 12
        self.days = 28
        self.bucket = bucket
        self.rest = rest
        self.test_datatype = test_datatype
        self.template_items_num = template_items_num
        self.views = self.create_views(rest, bucket=self.bucket)
        self.name = "sales_dataset"
        self.kv_store = None
        self.doc_id_map = {}
        self.docs_set = []
        self.limit = limit


    # views for this dataset
    def create_views(self, rest, bucket="default"):
        vfn = "function (doc) { emit([doc.join_yr, doc.join_mo, doc.join_day], doc['sales']);}"

        full_index_size = self.years * self.months * self.days * self.docs_per_day

        if self.test_datatype:
            vfn1 = "function (doc) { emit([doc.join_yr, doc.join_mo, doc.join_day], doc['is_support_included']);}"
            vfn2 = "function (doc) { emit([doc.join_yr, doc.join_mo, doc.join_day], [doc['is_support_included'],doc['is_high_priority_client']]);}"
            vfn3 = "function (doc) { emit([doc.join_yr, doc.join_mo, doc.join_day], Date.parse(doc['delivery_date']));}"
            vfn4 = "function (doc) { emit([doc.join_yr, doc.join_mo, doc.join_day], {client : {name : doc['client_name'], contact : doc['client_contact']}});}"
            vfn5 = "function (doc) { emit([doc.join_yr, doc.join_mo, doc.join_day], doc['client_reclaims_rate']);}"
            views = [QueryView(rest, full_index_size, bucket=bucket, fn_str= vfn1),
                     QueryView(rest, full_index_size, bucket=bucket, fn_str = vfn2),
                     QueryView(rest, full_index_size, bucket=bucket, fn_str = vfn3),
                     QueryView(rest, full_index_size, bucket=bucket, fn_str = vfn4),
                     QueryView(rest, full_index_size, bucket=bucket, fn_str = vfn5),
                     QueryView(rest, full_index_size, bucket=bucket, fn_str = vfn5, reduce_fn = "_count")]
        elif self.template_items_num:
            vfn1 = "function (doc) { emit([doc.join_yr, doc.join_mo, doc.join_day], doc['sales']);}"
            views = [QueryView(rest, full_index_size, bucket=bucket, fn_str= vfn1),
                     QueryView(rest, full_index_size, bucket=bucket, fn_str = vfn1, reduce_fn = "_count")]
        else:
            views = [QueryView(rest, full_index_size, bucket=bucket, fn_str=vfn, reduce_fn="_count"),
                     QueryView(rest, full_index_size, bucket=bucket, fn_str=vfn, reduce_fn="_sum"),
                     QueryView(rest, full_index_size, bucket=bucket, fn_str=vfn, reduce_fn = "_stats")]

        return views

    def load(self,tc, view, loads_per_iteration):
        try:
            self.doc_id_map['years'] = {}
            smart = VBucketAwareMemcached(self.rest, self.bucket)
            for i in range(1,self.years + 1):
                self.doc_id_map['years'][i] =\
                    { "months" : {}}
                for j in range(1, self.months + 1):
                    self.doc_id_map['years'][i]['months'][j] =\
                        {"days" : {}}
                    for k in range(1, self.days + 1):
                        self.doc_id_map['years'][i]['months'][j]['days'][k]=\
                            {"docs" : []}
                        sales = random.randrange(4000000)
                        if self.test_datatype:
                            kv_template = {"join_yr" : 2007 + i, "join_mo" : j, "join_day" : k,
                                           "sales" : sales,
                                           "delivery_date" : str(datetime.date(2007 + i, j, k)),
                                           "is_support_included" : random.choice([True, False]),
                                           "is_high_priority_client" : random.choice([True, False]),
                                           "client_contact" :  str(uuid.uuid4())[:10],
                                           "client_name" : str(uuid.uuid4())[:10],
                                           "client_reclaims_rate" : random.random()}
                            self.doc_id_map['years'][i]['months'][j]['days'][k]["sales"] = kv_template
                        elif self.template_items_num:
                            kv_template = {"join_yr" : 2007 + i, "join_mo" : j, "join_day" : k,
                                           "sales" : sales}
                            for num in xrange(self.template_items_num - 2):
                                kv_template['item_%s' % num] = 'value_%s' % num
                            self.doc_id_map['years'][i]['months'][j]['days'][k]["sales"] = kv_template
                        else:
                            kv_template = {"join_yr" : 2007+i, "join_mo" : j, "join_day" : k,
                                           "sales" : sales}
                            self.doc_id_map['years'][i]['months'][j]['days'][k]["sales"] = sales
                        options = {"size": 256, "seed":  str(uuid.uuid4())[:7]}
                        docs = DocumentGenerator.make_docs(loads_per_iteration, kv_template, options)
                        self._load_chunk(smart, docs)
        except Exception as ex:
            view.results.addError(tc, sys.exc_info())
            tc.log.error("At least one of load data threads is crashed: {0}".format(ex))
            tc.thread_crashed.set()
            raise ex
        finally:
            if not tc.thread_stopped.is_set():
                tc.thread_stopped.set()

    def _load_chunk(self, smart, docs):
        doc_ids = []
        idx = 0
        for value in docs:
                value = value.encode("utf-8", "ignore")
                json_map = json.loads(value, encoding="utf-8")[u'json']
                year = json_map["join_yr"]
                month = json_map["join_mo"]
                day = json_map["join_day"]


                doc_id = "{0}-{1}_{2}_{3}".format(str(idx).zfill(4),
                                                  year,
                                                  str(month).rjust(2,'0'),
                                                  str(day).rjust(2,'0'))

                smart.memcached(doc_id).set(doc_id, 0, 0, json.dumps(json_map))
                doc_ids.append(doc_id)

                idx += 1
        return doc_ids

    def add_reduce_queries(self, params, views=None):
        views = views or self.views
        for view in views:
            expected_num_groups = 1
            group_level = 0
            expected_num_docs = view.index_size
            if type(params) == types.DictType:
                params_dict = params
            else:
                params_dict = ViewQueryTests.parse_string_to_dict(params)
            if 'group_level' in params:
                group_level = int(params_dict['group_level'])
                if group_level == 1:
                    expected_num_groups = self.years
                elif group_level == 2:
                   expected_num_groups = self.months * self.years
                elif group_level >= 3:
                    expected_num_groups = self.days * self.months * self.years
            if self.limit:
                expected_num_groups = min(self.limit, expected_num_groups)
                expected_num_docs = min(self.limit, expected_num_docs)
            view.queries += [QueryHelper(params_dict,
                                         expected_num_docs,
                                         expected_num_groups,
                                         expected_statistics=self.calculate_stats(group_level))]

    def add_skip_queries(self, skip, limit=None, views=None):
        if views is None:
            views = self.views

        for view in views:
            limit = self.limit

            if view.reduce_fn:
                view.queries += [QueryHelper({"skip" : 0}, view.index_size),]
                continue
            if limit:
                limit = min(limit, view.index_size)
                view.queries += [QueryHelper({"skip" : skip, "limit" : str(limit)}, limit)]
            else:
                view.queries += [QueryHelper({"skip" : skip}, view.index_size - int(skip))]

    def calculate_stats(self, group_level):
        stats = {}
        groups_docs = {}
        if not group_level:
            key = 'None'
            groups_docs[key] = []
        for year in self.doc_id_map['years'].iteritems():
            if group_level == 1:
                key = '[{0}]'.format(year[0] + 2007)
                groups_docs[key] = []
            for month in year[1]['months'].iteritems():
                if group_level == 2:
                    key = '[{0}, {1}]'.format(year[0] + 2007, month[0])
                    groups_docs[key] = []
                for day in month[1]['days'].iteritems():
                    if group_level >= 3:
                        key = '[{0}, {1}, {2}]'.format(year[0] + 2007, month[0], day[0])
                        groups_docs[key] = []
                    for doc in xrange(self.docs_per_day):
                        groups_docs[key].append(day[1]['sales'])

        for group in groups_docs.iterkeys():
           stats[group] = {}
           stats[group]['count'] = len(groups_docs[group])
           stats[group]['sum'] = math.fsum(groups_docs[group])
           stats[group]['max'] = max(groups_docs[group])
           stats[group]['min'] = min(groups_docs[group])
           stats[group]['sumsqr'] =  math.fsum(map(lambda x: x * x, groups_docs[group]))

        return stats

class ExpirationDataSet:
    def __init__(self, rest, num_docs = 10000, bucket = "default", expire=60):
        self.num_docs = num_docs
        self.bucket = bucket
        self.rest = rest
        self.views = self.create_views(rest)
        self.name = "expiration_dataset"
        self.kv_store = None
        self.docs_set = []
        self.expire = expire
        self.expire_millis = 0

    def create_views(self, rest):
        view_fn = 'function (doc, meta) {if(doc.age !== undefined) { emit(doc.age, meta.expiration);}}'
        return [QueryView(self.rest, self.num_docs, fn_str = view_fn)]

    def load(self, tc, view):
        try:
            docs = ViewBaseTests._load_docs(tc, self.num_docs, 'exp_',
                                            bucket=self.bucket, expire=self.expire)
            self.expire_millis = int(time.time() + self.expire)
            return docs
        except Exception as ex:
            view.results.addError(tc, sys.exc_info())
            tc.log.error("At least one of load data threads is crashed: {0}".format(ex))
            tc.thread_crashed.set()
            raise ex
        finally:
            if not tc.thread_stopped.is_set():
                tc.thread_stopped.set()

    def query_verify_value(self,tc):
        for view in self.views:
            #query view
            results = ViewBaseTests._get_view_results(tc, self.rest, self.bucket, view.name,
                                                      extra_params={"stale" : "false"})
            tc.assertEquals(len(results.get(u'rows', 0)), view.index_size,
                              "Returned number of items is incorrect")
            for row in results.get(u'rows',0):
                tc.assertTrue(row['value'] in xrange(self.expire_millis -200, self.expire_millis + 200),
                                  "Expiration should be %s, but actual is %s" %\
                                   (self.expire_millis, row['value']))

class FlagsDataSet:
    def __init__(self, rest, num_docs = 10000, bucket = "default", item_flag=4042322160):
        self.num_docs = num_docs
        self.bucket = bucket
        self.rest = rest
        self.views = self.create_views(rest)
        self.name = "flags_dataset"
        self.kv_store = None
        self.docs_set = []
        self.item_flag = item_flag

    def create_views(self, rest):
        view_fn = 'function (doc, meta) {if(doc.age !== undefined) { emit(doc.age, meta.flags);}}'
        return [QueryView(self.rest, self.num_docs, fn_str = view_fn)]

    def load(self, tc, view):
        try:
            docs = ViewBaseTests._load_docs(tc, self.num_docs, 'fl_',
                                            bucket=self.bucket, flag=self.item_flag)
            return docs
        except Exception as ex:
            view.results.addError(tc, sys.exc_info())
            tc.log.error("At least one of load data threads is crashed: {0}".format(ex))
            tc.thread_crashed.set()
            raise ex
        finally:
            if not tc.thread_stopped.is_set():
                tc.thread_stopped.set()

    def query_verify_value(self,tc):
        for view in self.views:
            #query view
            results = ViewBaseTests._get_view_results(tc, self.rest, self.bucket, view.name,
                                                      extra_params={"stale" : "false"})
            tc.assertEquals(len(results.get(u'rows', 0)), view.index_size,
                              "Returned number of items is incorrect")
            for row in results.get(u'rows',0):
                tc.assertTrue(row['value'] == self.item_flag,
                                  "Flag should be %s, but actual is %s" %\
                                   (self.item_flag, row['value']))

class DataLoadHelper:
    @staticmethod
    def add_doc_gen_task(tm, rest, count, bucket = "default",
                         kv_store = None, store_enabled = True,
                         kv_template = None, seed = None,
                         sizes = None, expiration = None,
                         loop = False, monitor = False,
                         doc_generators = None):

        doc_generators = doc_generators or \
            DocumentGenerator.get_doc_generators(count, kv_template, seed, sizes)
        t = task.LoadDocGeneratorTask(rest, doc_generators, bucket, kv_store,
                                      store_enabled, expiration, loop)

        tm.schedule(t)
        if monitor:
            return t.result()
        return t



class QueryHelper:
    def __init__(self, params,
                 expected_num_docs,
                 expected_num_groups = 1,
                 type_ = "view",
                 error=None,
                 attempt_num=None,
                 expected_statistics = {}):

        self.params = params

        # number of docs this query should return
        self.expected_num_docs = expected_num_docs
        self.expected_num_groups = expected_num_groups
        self.type_ = type_   # "view" or "all_docs"
        self.expected_keys = []
        self.error = error
        self.expected_statistics = expected_statistics
        self.attempt_num = attempt_num

    # less open clients
    @staticmethod
    def verify_query_keys(rest, query, results, bucket = "default", num_verified_docs = 20, limit=None):
        failures = []

        if(len(query.expected_keys) == 0):
            return failures

        kv_client = KVStoreAwareSmartClient(rest, bucket)

        ids=[str(doc['id']) for doc in results['rows']]

        couch_set = set(ids)
        expected_set = limit and set(query.expected_keys[:limit]) or set(query.expected_keys)

        missing_item_set = expected_set - couch_set
        extra_item_set = couch_set - expected_set

        # treat duplicate doc_ids as extra_items
        if len(ids)!=len(couch_set):
            for id_ in couch_set:
                if ids.count(id_) > 1:
                    extra_item_set.add(id_)


        if(len(extra_item_set) > 0 ):

            # report unexpected/duplicate documents
            copy_ids = copy.deepcopy(ids)
            for doc_id in extra_item_set:
                for id_count in range(copy_ids.count(doc_id)):
                    ex_doc_idx = copy_ids.index(doc_id)
                    ex_doc_row = results['rows'][ex_doc_idx + id_count]
                    failures.append("extra documents detected in result: %s " % (ex_doc_row))
                    copy_ids.pop(ex_doc_idx)

        if(len(missing_item_set) > 0):

            # debug missing documents
            for doc_id in list(missing_item_set)[:num_verified_docs]:

                # attempt to retrieve doc from memcached
                mc_item = kv_client.mc_get_full(doc_id)
                if mc_item == None:
                    failures.append("document %s missing from memcached" % (doc_id))

                # attempt to retrieve doc from disk
                else:
                    num_vbuckets = len(rest.get_vbuckets(bucket))
                    doc_meta = kv_client.get_doc_metadata(num_vbuckets, doc_id)

                    if(doc_meta != None):
                        if (doc_meta['key_valid'] != 'valid'):
                            msg = "Error expected in results for key with invalid state %s" % doc_meta
                            failures.append(msg)

                    else:
                        msg = "query doc_id: %s doesn't exist in bucket: %s" % \
                            (doc_id, bucket)
                        failures.append(msg)

                if(len(failures) == 0):
                    msg = "view engine failed to index doc: %s \n query: %s" % (doc_id, query.params)
                    failures.append(msg)

        return failures

    @staticmethod
    def verify_query_ids_full(rest, query, results):
        if(len(query.expected_keys) == 0):
            raise Exception("There are no expected keys defined")

        current_ids = [str(doc['id']) for doc in results['rows']]

        if 'limit' in query.params:
            expected_ids = query.expected_keys[:int(query.params['limit'])]
        else:
            expected_ids = query.expected_keys

        if current_ids != expected_ids:
            raise Exception("Current ids and expected are not equal:\n current:{0} \n expected: {1} "
                            .format(current_ids, expected_ids))

    @staticmethod
    def verify_query_values(rest, query, results, bucket = "default"):

        failures = []
        kv_client = KVStoreAwareSmartClient(rest, bucket)

        if('include_docs' in query.params): # include_docs is deprecated
            docs = [row['doc'] for row in results['rows']]

            # retrieve doc from view result and compare with memcached
            for view_doc in docs:

                doc_id = str(view_doc['meta']['id'])
                mc_item = kv_client.mc_get_full(doc_id)

                if mc_item is not None:
                    mc_doc = json.loads(mc_item["value"])

                    # compare doc content
                    for key in mc_doc.keys():
                        if(mc_doc[key] != view_doc['json'][key]):
                            err_msg =\
                                "error verifying document id %s: retrieved value %s expected %s \n" % \
                                    (doc_id, mc_doc[key], view_doc['json'][key])
                            failures.append(err_msg)
                else:
                    failures.append("doc_id %s could not be retrieved for verification \n" % doc_id)

        else:
            failures.append("cannot verify view result values without include_docs filter \n")

        return failures

