import uuid
import logger
import time
import unittest
import json
import sys
import copy
from threading import Thread
from couchbase.document import View
from membase.api.rest_client import RestConnection, RestHelper
from viewtests import ViewBaseTests
from memcached.helper.data_helper import VBucketAwareMemcached, DocumentGenerator, KVStoreAwareSmartClient
from membase.helper.failover_helper import FailoverHelper
from membase.helper.rebalance_helper import RebalanceHelper
from old_tasks import task, taskmanager
from memcached.helper.old_kvstore import ClientKeyValueStore
from TestInput import TestInputSingleton

class ViewQueryTests(unittest.TestCase):
    skip_setup_failed  = False

    @unittest.skipIf(skip_setup_failed, "setup was failed")
    def setUp(self):
        try:
            ViewBaseTests.common_setUp(self)
            self.limit = TestInputSingleton.input.param("limit", None)
            self.reduce_fn = TestInputSingleton.input.param("reduce_fn", None)
            self.error = None
            self.task_manager = taskmanager.TaskManager()
            self.task_manager.start()
        except Exception as ex:
            skip_setup_failed = True
            self.fail(ex)

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

        self.task_manager.cancel()

    def test_simple_dataset_stale_queries(self):
        # init dataset for test
        data_set = SimpleDataSet(self._rconn(), self.num_docs, self.limit)
        data_set.add_stale_queries()
        self._query_test_init(data_set)

    def test_simple_dataset_startkey_endkey_queries(self):
        data_set = SimpleDataSet(self._rconn(), self.num_docs, limit=self.limit)
        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set)

    def test_simple_dataset_all_queries(self):
        data_set = SimpleDataSet(self._rconn(), self.num_docs, limit=self.limit)
        data_set.add_all_query_sets()
        self._query_test_init(data_set)

    def test_simple_dataset_include_queries(self):
        data_set = SimpleDataSet(self._rconn(), self.num_docs, limit=self.limit)
        data_set.add_include_docs_queries([data_set.views[0]])
        self._query_test_init(data_set, tm = self.task_manager)

    def test_simple_dataset_reduce_queries(self):
        data_set = SimpleDataSet(self._rconn(), self.num_docs,limit = self.limit,reduce_fn = self.reduce_fn)
        data_set.add_reduce_queries()
        self._query_test_init(data_set)

    def test_simple_dataset_negative_queries(self):
        # init dataset for test
        query_param = TestInputSingleton.input.param("query_param", None)
        value = TestInputSingleton.input.param("value", None)
        error = TestInputSingleton.input.param("error", None)

        data_set = SimpleDataSet(self._rconn(), self.num_docs)
        data_set.add_negative_query(query_param, value, error)
        self._query_test_init(data_set)

    def test_employee_dataset_startkey_endkey_queries(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set)

    def test_employee_dataset_alldocs_queries(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        data_set.add_all_docs_queries()
        self._query_test_init(data_set)

    def test_employee_dataset_key_quieres(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

        data_set.add_key_queries()
        self._query_test_init(data_set)

    def test_employee_dataset_negative_queries(self):
        # init dataset for test
        query_param = TestInputSingleton.input.param("query_param", None)
        value = TestInputSingleton.input.param("value", None)
        error = TestInputSingleton.input.param("error", None)
        docs_per_day = self.input.param('docs-per-day', 20)

        data_set = EmployeeDataSet(self._rconn(), docs_per_day)
        data_set.add_negative_query(query_param, value, error)
        self._query_test_init(data_set)

    def test_employee_dataset_alldocs_queries_rebalance_in(self):
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
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

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

        self._query_all_views(data_set.views)

        msg = "rebalance failed while removing failover nodes {0}".format(failover_nodes)
        self.assertTrue(rest.monitorRebalance(), msg=msg)

        #verify queries after failover
        self._query_all_views(data_set.views)

    def test_employee_dataset_alldocs_incremental_failover_queries(self):
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        data_set.add_all_docs_queries()
        self._query_test_init(data_set, False)

        servers=self.servers;

        # incrementaly failover nodes and verify loaded data
        for i in range(self.failover_factor):
            failover_helper = FailoverHelper(servers, self)
            failover_nodes = failover_helper.failover(1)
            self.log.info("10 seconds sleep after failover before invoking rebalance...")
            time.sleep(10)

            rest=RestConnection(self.servers[0])
            nodes = rest.node_statuses()
            rest.rebalance(otpNodes=[node.id for node in nodes],
                       ejectedNodes=[node.id for node in failover_nodes])

            self._query_all_views(data_set.views)

            temp=[]
            for server in servers:
                rest = RestConnection(server)
                if not RestHelper(rest).is_ns_server_running(timeout_in_seconds=1):
                    continue
                temp.append(server)
            servers=temp

            msg = "rebalance failed while removing failover nodes {0}".format(failover_nodes)
            self.assertTrue(RestConnection(self.servers[0]).monitorRebalance(), msg=msg)

        def test_employee_dataset_alldocs_queries_start_stop_rebalance_in_incremental(self):
            docs_per_day = self.input.param('docs-per-day', 20)
            data_set = EmployeeDataSet(self._rconn(), docs_per_day)

            data_set.add_all_docs_queries()
            self._query_test_init(data_set, False)

            master = self.servers[0]
            RebalanceHelper.wait_for_persistence(master, "default")

            rest=RestConnection(self.servers[0])
            nodes = rest.node_statuses()

            for server in self.servers[1:]:
                self.log.info("current nodes : {0}".format([node.id for node in rest.node_statuses()]))
                self.log.info("adding node {0}:{1} to cluster".format(server.ip, server.port))
                otpNode = rest.add_node(master.rest_username, master.rest_password, server.ip, server.port)
                msg = "unable to add node {0}:{1} to the cluster"
                self.assertTrue(otpNode, msg.format(server.ip, server.port))

                # Just doing 2 iterations
                for i in [1, 2]:
                    rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
                    expected_progress = 30*i
                    reached = RestHelper(rest).rebalance_reached(expected_progress)
                    self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
                    stopped = rest.stop_rebalance()
                    self.assertTrue(stopped, msg="unable to stop rebalance")
                    self._query_all_views(data_set.views)

                    rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=[])
                    self.assertTrue(rest.monitorRebalance(), msg="rebalance operation failed restarting")
                    self._query_all_views(data_set.views)

                self.assertTrue(len(rest.node_statuses()) -len(nodes)==1, msg="number of cluster's nodes is not correct")
                nodes = rest.node_statuses()

    def test_employee_dataset_alldocs_queries_start_stop_rebalance_out_incremental(self):
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        docs_per_day = self.input.param('docs-per-day', 20)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

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
            for i in [1, 2]:
                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=ejectedNodes)
                expected_progress = 30*i
                reached = RestHelper(rest).rebalance_reached(expected_progress)
                self.assertTrue(reached, "rebalance failed or did not reach {0}%".format(expected_progress))
                stopped = rest.stop_rebalance()
                self.assertTrue(stopped, msg="unable to stop rebalance")
                self._query_all_views(data_set.views)

                rest.rebalance(otpNodes=[node.id for node in rest.node_statuses()], ejectedNodes=ejectedNodes)
                self.assertTrue(rest.monitorRebalance(), msg="rebalance operation failed restarting")
                self._query_all_views(data_set.views)

            self.assertTrue(len(nodes) - len(rest.node_statuses()) == 1, msg="number of cluster's nodes is not correct")
            nodes = rest.node_statuses()

    def test_employee_dataset_startkey_endkey_docid_queries(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

        data_set.add_startkey_endkey_docid_queries()
        self._query_test_init(data_set)

    def test_employee_dataset_group_queries(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

        data_set.add_group_count_queries()
        self._query_test_init(data_set)


    def test_employee_dataset_startkey_endkey_queries_rebalance_in(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        num_nodes_to_add = self.input.param('num_nodes_to_add',0)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set, False)

        # rebalance_in and verify loaded data
        ViewBaseTests._begin_rebalance_in(self, howmany=num_nodes_to_add + 1)
        self._query_all_views(data_set.views)
        ViewBaseTests._end_rebalance(self)

    def test_employee_dataset_startkey_endkey_queries_rebalance_out(self):
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        docs_per_day = self.input.param('docs-per-day', 200)
        num_nodes_to_add = self.input.param('num_nodes_to_add',0)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)


        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set, False)

        # rebalance_out and verify loaded data
        ViewBaseTests._begin_rebalance_out(self, howmany=num_nodes_to_add + 1)
        self._query_all_views(data_set.views)
        ViewBaseTests._end_rebalance(self)

    def test_employee_dataset_stale_queries(self):
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

        data_set.add_stale_queries()
        self._query_test_init(data_set)

    def test_employee_dataset_all_queries(self):
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day, limit=self.limit)

        data_set.add_all_query_sets()
        self._query_test_init(data_set)

    def test_all_datasets_all_queries(self):
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

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
            load_task = Thread(target=data_set.load,
                               name="load_data_set",
                               args=(self, views[0]))
            load_task.start()
        else:
            load_task = data_set.load_with_tm(tm, rest)
            time.sleep(2)

        # run queries while loading data
        while(load_task.is_alive()):
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
    def _query_all_views(self, views, verify_results = True, kv_store = None, limit=None):

        query_threads = []
        for view in views:
            t = Thread(target=view.run_queries,
               name="query-{0}".format(view.name),
               args=(self, verify_results, kv_store, limit))
            query_threads.append(t)
            t.start()

        [t.join() for t in query_threads]

        self._check_view_intergrity(views)

    ##
    # if an error occured loading or querying data for a view
    # it is queued and checked here
    ##
    def _check_view_intergrity(self, views):
        for view in views:
            self.assertEquals(view.results.failures, [],
                [ex[1] for ex in view.results.failures])
            self.assertEquals(view.results.errors, [],
                [ex[1] for ex in view.results.errors])




    # retrieve default rest connection associated with the master server
    def _rconn(self):
        return RestConnection(self.servers[0])

class QueryView:
    def __init__(self, rest,
                 index_size,
                 bucket = "default",
                 prefix=None,
                 name = None,
                 fn_str = None,
                 reduce_fn = None,
                 queries = None,
                 create_on_init = True,
                 type_filter = None):

        self.index_size = index_size

        self.log = logger.Logger.get_logger()
        default_prefix = str(uuid.uuid4())[:7]
        default_fn_str = 'function (doc) {if(doc.name) { emit(doc.name, doc);}}'

        self.bucket = bucket
        self.prefix = (prefix, default_prefix)[prefix is None]
        default_name = "dev_test_view-{0}".format(self.prefix)
        self.name = (name, default_name)[name is None]
        self.fn_str = (fn_str, default_fn_str)[fn_str is None]
        self.reduce_fn = reduce_fn
        self.results = unittest.TestResult()
        self.type_filter = type_filter or None

        # queries defined for this view
        self.queries = (queries, list())[queries is None]

        if create_on_init:
            rest.create_view(self.name, self.bucket, [View(self.name, self.fn_str, self.reduce_fn)])

    # query this view
    def run_queries(self, tc, verify_results = False, kv_store = None, limit=None):
        rest = tc._rconn()

        if not len(self.queries) > 0 :
            self.log.info("No queries to run for this view")
            return

        view_name = self.name

        max_dupe_result_count = tc.input.param('max-dupe-result-count', 5)

        num_verified_docs = tc.input.param('num-verified-docs', 20)

        for query in self.queries:
            params = query.params

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
                while attempt < 15 and num_keys != expected_num_docs:
                    if attempt > 11:
                        params["stale"] = 'false'

                    self.log.info("Quering view {0} with params: {1}".format(view_name, params))
                    results = ViewBaseTests._get_view_results(tc, rest, self.bucket, view_name,
                                                                  limit=limit, extra_params=params,
                                                                  type_ = query.type_)
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
                    time.sleep(delay)

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
                                                                      type_ = query.type_)

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
                    self.log.error("Query failed: see test result logs for details")
                    self.results.addFailure(tc, sys.exc_info())

            else:
                # query without verification
                self.log.info("Quering view {0} with params: {1}".format(view_name, params));
                try:
                    results = ViewBaseTests._get_view_results(tc, rest, self.bucket, view_name,
                                                              limit=limit, extra_params=params,
                                                              type_ = query.type_,
                                                              invalid_results=query.error and True or False)
                except Exception as ex:
                        if query.error:
                            if ex.message.find(query.error) > -1:
                                self.log.info("View results contain '{0}' error as expected".format(query.error))
                                return
                            else:
                                self.log.error("View results expect '{0}' error but {1} raised".format(query.error, ex.message))
                                self.results.addFailure(tc,(type(ex), ex.message, sys.exc_info()[2]))
                                return
                if query.error:
                    self.log.error("No error raised for negative case. Expected error '{0}'".format(query.error))
                    self.results.addFailure(tc, (Exception, "No error raised for negative case", sys.exc_info()[2]))


    """
        helper function for verifying results when _count reduce is used.
        by default 1 group is expected but when more specified
        a summation is derived

        result is compared with expected_num_docs of the query

        TODO: _sum,_stats? :)
    """
    def _verify_count_reduce_helper(self, query, results):

        num_keys = 0

        for i in xrange(query.expected_num_groups):
            if i <= len(results["rows"]):
                num_keys += results["rows"][i]["value"]

        return num_keys

class EmployeeDataSet:
    def __init__(self, rest, docs_per_day = 200, bucket = "default", limit=None):
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
        self.views = self.create_views(rest)
        self.bucket = bucket
        self.rest = rest
        self.name = "employee_dataset"
        self.kv_store = None
        self.doc_id_map = {}
        self.limit = limit

    def calc_total_doc_count(self):
        return self.years * self.months * self.days * self.docs_per_day * len(self.get_data_sets())

    def add_negative_query(self, query_param, value, error, views=None):
        views = views or self.views
        for view in views:
            view.queries += [QueryHelper({query_param : value}, None, error=error)]

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

    def add_all_docs_queries(self, views = None):

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

            view.queries += [QueryHelper({"start_key": '"arch0000-2008_10_01"'},
                                         index_size - section_size - self.days*9),
                             QueryHelper({"start_key" : '"ui0000-2008_10_01"'},
                                         index_size  - section_size*2 - self.days*9),
                             QueryHelper({"start_key" : '"arch0000-2008_10_01"',
                                          "end_key"   : '"ui0000-2008_10_01"',
                                          "inclusive_end" : "false"},
                                         index_size - section_size*2)]
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


    def add_stale_queries(self, views=None, limit=None):
        if views is None:
            views = self.views
        if limit is None:
            limit = self.limit

        for view in views:
            if limit:
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
        if views is None:
            views = [self.views[-1]]
        if limit is None:
            limit = self.limit

        for view in views:
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

    def add_all_query_sets(self, views=None, limit=None):
        self.add_stale_queries(views, limit)
        self.add_startkey_endkey_queries(views, limit)
        self.add_startkey_endkey_docid_queries(views, limit)
        self.add_group_count_queries(views)

    # views for this dataset
    def create_views(self, rest):
        vfn1 = 'function (doc) { if(doc.job_title !== undefined) { var myregexp = new RegExp("^UI "); if(doc.job_title.match(myregexp)){ emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}}'
        vfn2 = 'function (doc) { if(doc.job_title !== undefined) { var myregexp = new RegExp("^System "); if(doc.job_title.match(myregexp)){ emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}}'
        vfn3 = 'function (doc) { if(doc.job_title !== undefined) { var myregexp = new RegExp("^Senior "); if(doc.job_title.match(myregexp)){ emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}}'
        vfn4 = 'function (doc) { if(doc.job_title !== undefined) emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] ); }'

        full_index_size = self.calc_total_doc_count()
        partial_index_size = full_index_size/3

        return [QueryView(rest, full_index_size,    fn_str = vfn4),
                QueryView(rest, partial_index_size, fn_str = vfn1, type_filter = "ui"),
                QueryView(rest, partial_index_size, fn_str = vfn2, type_filter = "admin"),
                QueryView(rest, partial_index_size, fn_str = vfn3, type_filter = "arch"),
                QueryView(rest, full_index_size,    fn_str = vfn4, reduce_fn="_count")]

    def get_data_sets(self):
        return [self.sys_admin_info, self.ui_eng_info, self.senior_arch_info]

    def load(self, tc, view, verify_docs_loaded = True):
        data_threads = []
        for info in self.get_data_sets():

            self.doc_id_map.update({info['type'] : {"years" : self._doc_map_array()}})
            t = Thread(target=self._iterative_load,
                       name="iterative_load",
                       args=(info, tc, view, self.docs_per_day, verify_docs_loaded))
            data_threads.append(t)
            t.start()

        for t  in data_threads:
            t.join()

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
            raise ex

    def _load_chunk(self, smart, doc_sets):

        doc_ids = []
        for docs in doc_sets:
            idx = 0
            for value in docs:
                value = value.encode("utf-8", "ignore")
                json_map = json.loads(value, encoding="utf-8")
                type_ = json_map["type"]
                year = json_map["join_yr"]
                month = json_map["join_mo"]
                day = json_map["join_day"]


                doc_id = "{0}{1}-{2}_{3}_{4}".format(type_,
                                                     str(idx).zfill(4),
                                                     year,
                                                     str(month).rjust(2,'0'),
                                                     str(day).rjust(2,'0'))

                del json_map["_id"]
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
            if 'start_key' in q_params:
                q_params['startkey_docid'] = q_params['start_key']
                del q_params['start_key']

            if 'end_key' in q_params:
                q_params['endkey_docid'] = q_params['end_key']
                del q_params['end_key']

        if 'start_key' in q_params:
            params = json.loads(q_params['start_key'])
            if params[0] and not None:
                q_start_yr = params[0] - 2007
            if params[1] and not None:
                q_start_mo = params[1]
            if params[2] and not None:
                q_start_day = params[2]

        if 'end_key' in q_params:
            params = json.loads(q_params['end_key'])
            if params[0] and not None:
                q_end_yr = params[0] - 2007
            if params[1] and not None:
                q_end_mo = params[1]
            if params[2] and not None:
                q_end_day = params[2]

        if 'descending' in q_params:
            descending = json.loads(q_params['descending'])
            if descending == True:
                q_start_yr, q_end_yr = q_end_yr, q_start_yr
                q_start_mo, q_end_mo = q_end_mo, q_start_mo
                q_start_day, q_end_day = q_end_day, q_start_day

        # note: inclusive end check must occur after descending
        if 'inclusive_end' in q_params:
            inclusive_end = json.loads(q_params['inclusive_end'])
            if inclusive_end == False and 'endkey_docid' not in q_params:
                if descending == False:
                    # decrement end_key
                    if q_end_day <= 1:
                        q_end_mo -= 1
                        q_end_day = 28
                    else:
                        q_end_day -= 1
                else:
                    # increment start_key
                    if q_start_day == 28:
                        q_start_mo += 1
                        q_start_day = 1
                    else:
                        q_start_day += 1


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



class SimpleDataSet:
    def __init__(self, rest, num_docs, limit = None, reduce_fn=None):
        self.num_docs = num_docs
        self.views = self.create_views(rest,reduce = reduce_fn)
        self.name = "simple_dataset"
        self.kv_store = ClientKeyValueStore()
        self.kv_template = {"name": "doc-${prefix}-${seed}-", "age": "${prefix}"}
        self.limit = limit
        self.reduce_fn = reduce_fn

    def create_views(self, rest, reduce=None):
        view_fn = 'function (doc) {if(doc.age !== undefined) { emit(doc.age, doc.name);}}'
        return [QueryView(rest, self.num_docs, fn_str=view_fn, reduce_fn=reduce)]

    def load(self, tc, view, verify_docs_loaded = True):
        doc_names = ViewBaseTests._load_docs(tc, self.num_docs, view.prefix, verify_docs_loaded)
        return doc_names

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

    def add_negative_query(self, query_param, value, error, views=None):
        views = views or self.views
        for view in views:
            view.queries += [QueryHelper({query_param : value}, None, error=error)]

    def add_include_docs_queries(self, views=None, limit=None):
        views = views or self.views
        if limit is None:
                limit = self.limit
        for view in views:
            if limit is not None:
                view.queries += [QueryHelper({"include_docs" : "true", "limit" : limit}, limit>view.index_size and limit or view.index_size)]
            else:
                view.queries += [QueryHelper({"include_docs" : "true"}, view.index_size)]

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

    def add_stale_queries(self, views = None, limit= None):
        if views is None:
            views = self.views

        for view in views:
            index_size = view.index_size
            if limit is None:
                limit = self.limit
                limit = limit < index_size and limit or index_size


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
                 error=None):

        self.params = params

        # number of docs this query should return
        self.expected_num_docs = expected_num_docs
        self.expected_num_groups = expected_num_groups
        self.type_ = type_   # "view" or "all_docs"
        self.expected_keys = []
        self.error = error

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
    def verify_query_values(rest, query, results, bucket = "default"):

        failures = []
        kv_client = KVStoreAwareSmartClient(rest, bucket)

        if('include_docs' in query.params):
            docs = [row['doc'] for row in results['rows']]

            # retrieve doc from view result and compare with memcached
            for view_doc in docs:

                doc_id = str(view_doc['_id'])
                mc_item = kv_client.mc_get_full(doc_id)

                if mc_item is not None:
                    mc_doc = json.loads(mc_item["value"])

                    # compare doc content
                    for key in mc_doc.keys():
                        if(mc_doc[key] != view_doc[key]):
                            err_msg =\
                                "error verifying document id %s: retrieved value %s expected %s \n" % \
                                    (doc_id, mc_doc[key], view_doc[key])
                            failures.append(err_msg)
                else:
                    failures.append("doc_id %s could not be retrieved for verification \n" % doc_id)

        else:
            failures.append("cannot verify view result values without include_docs filter \n")

        return failures

