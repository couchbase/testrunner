import uuid
import logger
import time
import unittest
import threading
from threading import Thread
import json
import sys

from membase.helper.spatial_helper import SpatialHelper


class SpatialQueryTests(unittest.TestCase):
    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.helper = SpatialHelper(self, "default")
        self.helper.setup_cluster()

    def tearDown(self):
        self.helper.cleanup_cluster()

    def test_simple_dataset_limit_queries(self):
        num_docs = self.helper.input.param("num-docs", 1000)
        self.log.info("description : Make limit queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_limit_queries()
        self._query_test_init(data_set)

    def test_simple_dataset_skip_queries(self):
        num_docs = self.helper.input.param("num-docs", 1000)
        self.log.info("description : Make skip (and limit) queries on a "
                      "simple dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_skip_queries()
        self._query_test_init(data_set)

    def test_simple_dataset_bbox_queries(self):
        num_docs = self.helper.input.param("num-docs", 1000)
        self.log.info("description : Make bounding box queries on a simple "
                      "dataset with {0} docs".format(num_docs))

        data_set = SimpleDataSet(self.helper, num_docs)
        data_set.add_bbox_queries()
        self._query_test_init(data_set)

    ###
    # load the data defined for this dataset.
    # create views and query the data as it loads.
    # verification is optional, and best practice is to
    # set to False if you plan on running _query_all_views()
    # later in the test case
    ###
    def _query_test_init(self, data_set, verify_results = True):
        views = data_set.views

        # start loading data
        t = Thread(target=data_set.load,
                   name="load_data_set",
                   args=())
        t.start()

        # run queries while loading data
        while(t.is_alive()):
            self._query_all_views(views, False)
            time.sleep(5)
        t.join()

        # results will be verified if verify_results set
        if verify_results:
            self._query_all_views(views, verify_results)
        else:
            self._check_view_intergrity(views)


    ##
    # run all queries for all views in parallel
    ##
    def _query_all_views(self, views, verify_results = True):

        query_threads = []
        for view in views:
            t = RunQueriesThread(view, verify_results)
            query_threads.append(t)
            t.start()

        [t.join() for t in query_threads]

        self._check_view_intergrity(query_threads)

    ##
    # If an error occured loading or querying data for a view
    # it is queued and checked here. Fail on the first one that
    # occurs.
    ##
    def _check_view_intergrity(self, thread_results):
        for result in thread_results:
            if result.test_results.errors:
                self.fail(result.test_results.errors[0][1])
            if result.test_results.failures:
                self.fail(result.test_results.failures[0][1])



class View:
    def __init__(self, helper, index_size, fn_str, prefix=None, name=None,
                 queries=list(), create_on_init=True):
        self.helper = helper
        self.index_size = index_size

        self.log = logger.Logger.get_logger()

        # Store failures in here. Don't forget to add them manually,
        # else the failed assertions won't make the whole test fail
        self._test_results = unittest.TestResult()

        # queries defined for this view
        self.queries = queries

        if prefix is None:
            prefix = str(uuid.uuid4())[:7]

        self.name = name if name is not None else "dev_test_view-" + prefix

        if create_on_init:
            self.helper.create_index_fun(self.name, prefix, fn_str)



class SimpleDataSet:
    def __init__(self, helper, num_docs):
        self.helper = helper
        self.num_docs = num_docs
        self.views = self._create_views()
        self.name = "simple_dataset"


    def _create_views(self):
        view_fn = 'function (doc) {if(doc.geometry !== undefined || doc.name !== undefined ) { emit(doc.geometry, doc.name);}}'
        return [View(self.helper, self.num_docs, fn_str = view_fn)]

    def load(self, verify_docs_loaded = True):
        prefix = str(uuid.uuid4())[:7]
        inserted_keys = self.helper.insert_docs(self.num_docs, prefix)

        if verify_docs_loaded:
            all_docs = self.helper.rest.all_docs(self.helper.bucket)
            all_keys = self.helper.get_keys(all_docs)

            # all_keys also contains the design document, but this doesn't
            # matter if we compare like this
            if set(inserted_keys) - set(all_keys):
                self.helper.testcase.fail("not all docs were loaded")

        return inserted_keys

    def add_limit_queries(self):
        for view in self.views:
            view.queries += [
                QueryHelper({"limit": 10}, 10),
                QueryHelper({"limit": 3417}, 3417),
                QueryHelper({"limit": view.index_size}, view.index_size),
                QueryHelper({"limit": 5*view.index_size}, view.index_size)]

    def add_skip_queries(self):
        for view in self.views:
            view.queries += [
                QueryHelper({"skip": 10}, view.index_size-10),
                QueryHelper({"skip": 2985}, view.index_size-2985),
                QueryHelper({"skip": view.index_size}, 0),
                QueryHelper({"skip": 5*view.index_size}, 0),
                QueryHelper({"skip": 2985, "limit": 1539}, 1539),
                QueryHelper({"skip": view.index_size-120, "limit": 1539}, 120),
                QueryCompareHelper([{"skip": 6210, "limit": 1592}],
                                   [{"skip": 6210, "limit": 1086},
                                    {"skip": 7296, "limit": 506}])
                ]

    def add_bbox_queries(self):
        for view in self.views:
            view.queries += [
                QueryHelper({"bbox": "-180,-90,180,90"}, view.index_size),
                QueryHelper({"bbox": "-900,-900,900,900"}, view.index_size),
                QueryHelper({}, view.index_size),
                QueryHelper({"bbox": "-900,-900,900,900"}, view.index_size),
                QueryCompareHelper([{"bbox": "-900,-900,900,900"}],
                                   [{}]),
                QueryCompareHelper([{"bbox": "-117,-76,34,43"}],
                                   [{"bbox": "-117,-76,34,-5"},
                                    {"bbox": "-117,-5,34,43"}]),
                ]

    def add_all_query_sets(self):
        self.add_limit_queries()
        self.add_skip_queries()
        self.add_bbox_queries()



class QueryHelper:
    def __init__(self, params, expected_num_docs):
        self.params = params

        # number of docs this query should return
        self.expected_num_docs = expected_num_docs

# Put in two lists of queries, it will then join the results of the
# individual queries and compare both
class QueryCompareHelper:
    def __init__(self, queries_a, queries_b):
        self.queries_a = queries_a
        self.queries_b = queries_b



class RunQueriesThread(threading.Thread):
    def __init__(self, view, verify_results = False):
        threading.Thread.__init__(self)
        self.view = view
        self.verify_results = verify_results
        # The last retrieved results, useful when an exception happened
        self._last_results = None

        # Store failures in here. So we can make the whole test fail,
        # normally only this thread will fail
        self.test_results = unittest.TestResult()

        self.helper = self.view.helper
        self.log = self.view.log

    def run(self):
        if not len(self.view.queries) > 0 :
            self.log.info("No queries to run for this view")
            return

        try:
            self._run_queries()
        except Exception:
            self.log.error("Last query result:\n\n{0}\n\n"\
                               .format(json.dumps(self._last_results,
                                                  sort_keys=True)))
            self.test_results.addFailure(self.helper.testcase, sys.exc_info())

    def _run_queries(self):
        for query in self.view.queries:
            # Simple query
            if isinstance(query, QueryHelper):
                if self.verify_results:
                    self._last_results = self._run_query(
                        query.params, query.expected_num_docs)
                else:
                    self._last_results = self._run_query(query.params)
            # Compare queries, don't verify the individual queries
            # but only the final result
            elif isinstance(query, QueryCompareHelper):
                result_keys_a = []
                result_keys_b = []
                for params in query.queries_a:
                    self._last_results = self._run_query(params)
                    result_keys_a.extend(
                        self.helper.get_keys(self._last_results))

                for params in query.queries_b:
                    self._last_results = self._run_query(params)
                    result_keys_b.extend(
                        self.helper.get_keys(self._last_results))

                if self.verify_results:
                    diff = set(result_keys_a) - set(result_keys_b)
                    self.helper.testcase.assertEqual(diff, set())
            else:
                self.helper.testcase.fail("no queries specified")

    # If expected_num_docs is given, the results are verified
    def _run_query(self, query_params, expected_num_docs=None):
        params = {"debug": True}
        params.update(query_params)

        if expected_num_docs is not None:
            self.log.info("Quering view {0} with params: {1}".format(
                    self.view.name, params));
            results = self.helper.get_results(self.view.name, None, params)
            num_keys = len(self.helper.get_keys(results))
            self.log.info("{0}: retrieved value {1} expected: {2}"\
                              .format(self.view.name, num_keys,
                                      expected_num_docs));

            if(num_keys != expected_num_docs):
                error = "Query failed: {0} Documents Retrieved, "\
                    "expected {1}".format(num_keys, expected_num_docs)
                try:
                    self.helper.testcase.assertEquals(num_keys,
                                                      expected_num_docs,
                                                      error)
                except Exception:
                    self.log.error(error)
                    raise
            else:
                return results
        else:
            # query without verification
            self.log.info("Quering view {0} with params: {1}"\
                              .format(self.view.name, params));
            return self.helper.get_results(self.view.name, None, params)
