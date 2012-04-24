import uuid
import logger
import time
import unittest
import json
import sys
from threading import Thread
from couchbase.document import View
from membase.api.rest_client import RestConnection
from viewtests import ViewBaseTests
from memcached.helper.data_helper import VBucketAwareMemcached, DocumentGenerator, KVStoreAwareSmartClient
from tasks import task, taskmanager
from memcached.helper.kvstore import ClientKeyValueStore

class ViewQueryTests(unittest.TestCase):

    def setUp(self):
        ViewBaseTests.common_setUp(self)

        self.task_manager = taskmanager.TaskManager()
        self.task_manager.start()

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

        self.task_manager.cancel()

    def test_simple_dataset_stale_queries(self):
        # init dataset for test
        data_set = SimpleDataSet(self._rconn(), self.num_docs)
        data_set.add_stale_queries()
        self._query_test_init(data_set)

    def test_simple_dataset_startkey_endkey_queries(self):
        data_set = SimpleDataSet(self._rconn(), self.num_docs)
        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set)

    def test_simple_dataset_all_queries(self):
        data_set = SimpleDataSet(self._rconn(), self.num_docs)
        data_set.add_all_query_sets()
        self._query_test_init(data_set)

    def test_simple_dataset_include_queries(self):
        data_set = SimpleDataSet(self._rconn(), self.num_docs)
        data_set.add_include_docs_queries([data_set.views[0]])
        self._query_test_init(data_set, tm = self.task_manager)

    def test_employee_dataset_startkey_endkey_queries(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set)

    def test_employee_dataset_alldocs_queries(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        data_set.add_all_docs_queries()
        self._query_test_init(data_set)

    def test_employee_dataset_alldocs_queries_rebalance_in(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        data_set.add_all_docs_queries()
        self._query_test_init(data_set, False)

        # rebalance_in and verify loaded data
        ViewBaseTests._begin_rebalance_in(self)
        self._query_all_views(data_set.views)
        ViewBaseTests._end_rebalance(self)

    def test_employee_dataset_startkey_endkey_docid_queries(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        data_set.add_startkey_endkey_docid_queries()
        self._query_test_init(data_set)

    def test_employee_dataset_group_queries(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        data_set.add_group_count_queries()
        self._query_test_init(data_set)


    def test_employee_dataset_startkey_endkey_queries_rebalance_in(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set, False)

        # rebalance_in and verify loaded data
        ViewBaseTests._begin_rebalance_in(self)
        self._query_all_views(data_set.views)
        ViewBaseTests._end_rebalance(self)

    def test_employee_dataset_startkey_endkey_queries_rebalance_out(self):
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)


        data_set.add_startkey_endkey_queries()
        self._query_test_init(data_set, False)

        # rebalance_out and verify loaded data
        ViewBaseTests._begin_rebalance_out(self)
        self._query_all_views(data_set.views)
        ViewBaseTests._end_rebalance(self)

    def test_employee_dataset_stale_queries(self):
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        data_set.add_stale_queries()
        self._query_test_init(data_set)

    def test_employee_dataset_all_queries(self):
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

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
            self._query_all_views(views, False)
            time.sleep(5)
        if 'result' in dir(load_task):
            load_task.result()
        else:
            load_task.join()

        # results will be verified if verify_results set
        if verify_results:
            self._query_all_views(views, verify_results, data_set.kv_store)
        else:
            self._check_view_intergrity(views)


    ##
    # run all queries for all views in parallel
    ##
    def _query_all_views(self, views, verify_results = True, kv_store = None):

        query_threads = []
        for view in views:
            t = Thread(target=view.run_queries,
               name="query-{0}".format(view.name),
               args=(self, verify_results, kv_store))
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
                 create_on_init = True):

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

        # queries defined for this view
        self.queries = (queries, list())[queries is None]

        if create_on_init:
            rest.create_view(self.name, self.bucket, [View(self.name, self.fn_str, self.reduce_fn)])

    # query this view
    def run_queries(self, tc, verify_results = False, kv_store = None):
        rest = tc._rconn()

        if not len(self.queries) > 0 :
            self.log.info("No queries to run for this view")
            return

        view_name = self.name

        for query in self.queries:
            params = query.params

            params["debug"] = "true"
            expected_num_docs = query.expected_num_docs
            num_keys = -1

            if expected_num_docs is not None and verify_results:
                attempt = 0
                delay = 15
                results = None

                # first verify all doc_names get reported in the view
                # for windows, we need more than 20+ times
                while attempt < 40 and num_keys != expected_num_docs:
                    self.log.info("Quering view {0} with params: {1}".format(view_name, params));
                    results = ViewBaseTests._get_view_results(tc, rest,
                                                              self.bucket,
                                                              view_name,
                                                              limit=None,
                                                              extra_params=params,
                                                              type_ = query.type_)

                    # check if this is a reduced query using _count
                    if self.reduce_fn is '_count':
                        num_keys = self._verify_count_reduce_helper(query, results)
                        self.log.info("{0}: attempt {1} reduced {2} group(s) to value {3} expected: {4}" \
                            .format(view_name, attempt, query.expected_num_groups,
                                    num_keys, expected_num_docs));
                    else:

                        num_keys = len(ViewBaseTests._get_keys(self, results))
                        self.log.info("{0}: attempt {1} retrieved value {2} expected: {3}" \
                            .format(view_name, attempt, num_keys, expected_num_docs));

                    attempt += 1

                    time.sleep(delay)

                if(num_keys != expected_num_docs):
                    msg = "Query failed: {0} Documents Retrieved,  expected {1}"
                    val = msg.format(num_keys, expected_num_docs)
                    try:
                        tc.assertEquals(num_keys, expected_num_docs, val)
                    except Exception as ex:
                        self.log.error(val)
                        self.log.error("Last query result:\n\n%s\n\n" % (json.dumps(results, sort_keys=True, indent=4)))
                        self.results.addFailure(tc, sys.exc_info())

                if 'include_docs' in params:
                    self.view_doc_integrity(tc, results, expected_num_docs, kv_store)

            else:
                # query without verification
                self.log.info("Quering view {0} with params: {1}".format(view_name, params));
                results = ViewBaseTests._get_view_results(tc, rest, self.bucket, view_name,
                                                          limit=None, extra_params=params,
                                                          type_ = query.type_)


    def view_doc_integrity(self, tc, results, expected_num_docs, kv_store):
        rest = tc._rconn()

        try:
            kv_client = KVStoreAwareSmartClient(rest,
                                                self.bucket,
                                                kv_store = kv_store)
            valid_keys = kv_client.get_all_valid_items()
            tc.assertEquals(len(valid_keys), expected_num_docs)

            # retrieve doc from view result and compare with memcached
            for row in results['rows']:

                # retrieve doc from view result
                tc.assertTrue('doc' in row, "malformed view result")
                view_doc = row['doc']
                doc_id = str(view_doc['_id'])

                # retrieve doc from memcached
                mc_item = kv_client.mc_get_full(doc_id)
                tc.assertFalse(mc_item == None, "document missing from memcached")
                mc_doc = json.loads(mc_item["value"])

                # compare
                for key in mc_doc.keys():
                    err_msg = "error verifying document id {0}: retrieved value {1} expected {2}"
                    err_msg_fmt = err_msg.format(doc_id, mc_doc[key], view_doc[key])
                    tc.assertEquals(mc_doc[key], view_doc[key], err_msg_fmt)

        except AssertionError :
            self.log.error("error during data verification")
            self.results.addFailure(tc, sys.exc_info())


    """
        helper function for verifying results when _count reduce is used.
        by default 1 group is expected but when more specified
        a summation is derived

        result is compared with expected_num_docs of the query

        TODO: _sum,_stats? :)
    """
    def _verify_count_reduce_helper(self, query, results):

        num_keys = 0

        for i in range(0,query.expected_num_groups):
            num_keys += results["rows"][i]["value"]

        return num_keys

class EmployeeDataSet:
    def __init__(self, rest, docs_per_day = 200, bucket = "default"):
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

    def calc_total_doc_count(self):
        return self.years * self.months * self.days * self.docs_per_day * len(self.get_data_sets())

    def add_startkey_endkey_queries(self, views = None):
        if views is None:
            views = self.views

        for view in views:
            index_size = view.index_size
            offset = self.docs_per_day

             # offset includes all data types if
             # indexing entire data_set
            if index_size == self.calc_total_doc_count():
                offset = offset * len(self.get_data_sets())

            view.queries += [QueryHelper({"start_key" : "[2008,7,null]"}, index_size/2),
                             QueryHelper({"start_key" : "[2008,0,1]",
                                          "end_key"   : "[2008,7,1]",
                                          "inclusive_end" : "false"}, index_size/2),
                             QueryHelper({"start_key" : "[2008,0,1]",
                                          "end_key"   : "[2008,7,1]",
                                          "inclusive_end" : "true"}, index_size/2 + offset),
                             QueryHelper({"start_key" : "[2008,7,1]",
                                          "end_key"   : "[2008,1,1]",
                                          "descending"   : "true",
                                          "inclusive_end" : "false"}, index_size/2),
                             QueryHelper({"start_key" : "[2008,7,1]",
                                          "end_key"   : "[2008,1,1]",
                                          "descending"   : "true",
                                          "inclusive_end" : "true"}, index_size/2 + offset)]



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
            offset = self.docs_per_day


            section_size = index_size/len(self.get_data_sets())

            view.queries += [QueryHelper({"start_key": '"arch0000-2008_10_1"'}, index_size - section_size),
                             QueryHelper({"start_key" : '"ui0000-2008_10_1"'}, index_size  - section_size*2),
                             QueryHelper({"start_key" : '"arch0000-2008_10_1"',
                                          "end_key"   : '"ui0000-2008_10_1"',
                                          "inclusive_end" : "false"}, section_size),
                             # test design docs are included when start_key not specified
                             QueryHelper({"end_key" : '"ui0000-2008_10_1"',
                                          "inclusive_end" : "false"}, index_size  - section_size + len(self.views))]

            # set all_docs flag
            for query in view.queries:
                query.type_ = "all_docs"


    """
        Create queries for testing docids on duplicate start_key result ids.
        Only pass in view that indexes all employee types as they are
        expected in the query params...i.e (ui,admin,arch)
    """
    def add_startkey_endkey_docid_queries(self, views = None):
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

            view.queries += [QueryHelper({"start_key" : "[2008,7,1]",
                                          "startkey_docid" : "arch0000-2008_7_1"}, index_size/2  - offset),
                            QueryHelper({"start_key" : "[2008,7,1]",
                                          "startkey_docid" : "ui0000-2008_7_1"}, index_size/2  - offset*2),
                                          # +endkey_docid
                            QueryHelper({"start_key" : "[2008,0,1]",
                                         "end_key"   : "[2008,7,1]",
                                         "endkey_docid" : "arch0000-2008_7_1",
                                         "inclusive_end" : "false"}, index_size/2 + offset),
                            QueryHelper({"end_key"   : "[2008,7,1]",
                                         "endkey_docid" : "ui0000-2008_7_1",
                                         "inclusive_end" : "false"}, index_size/2 + offset*2),
                                          # + inclusive_end
                            QueryHelper({"end_key"   : "[2008,7,1]",
                                         "endkey_docid" : "arch0000-2008_7_1",
                                         "inclusive_end" : "true"}, index_size/2 + offset + 1),
                                          # + single bounded and descending
                            QueryHelper({"start_key" : "[2008,7,1]",
                                         "end_key"   : "[2008,2,20]",
                                         "endkey_docid"   : "ui0000-2008_2_20",
                                         "descending"   : "true",
                                         "inclusive_end" : "true"}, complex_query_key_count - offset * 2),
                            QueryHelper({"start_key" : "[2008,7,1]",
                                         "end_key"   : "[2008,2,20]",
                                         "endkey_docid"   : "arch0000-2008_2_20",
                                         "descending"   : "true",
                                         "inclusive_end" : "false"}, complex_query_key_count - offset - 1),
                                          # + double bounded and descending
                            QueryHelper({"start_key" : "[2008,7,1]",
                                         "startkey_docid" : "admin0000-2008_7_1",
                                         "end_key"   : "[2008,2,20]",
                                         "endkey_docid"   : "arch0000-2008_2_20",
                                         "descending"   : "true",
                                         "inclusive_end" : "false"},
                                         complex_query_key_count - offset - all_docs_per_day)]


    def add_stale_queries(self, views = None):
        if views is None:
            views = self.views

        for view in views:
            index_size = view.index_size
            view.queries += [QueryHelper({"stale" : "false"}, index_size),
                             QueryHelper({"stale" : "ok"}, index_size),
                             QueryHelper({"stale" : "update_after"}, index_size)]


    """
        group queries should only be added to views with reduce views.
        in this particular case, _count function is expected.
        if no specific views are passed in, we'll just figure it out.

        verification requries that the expected number of groups generated
        by the query is provided.  each group will generate a value that
        when summed, should add up to the number of indexed docs
    """
    def add_group_count_queries(self, views = None):
        if views is None:
            views = [self.views[-1]]

        for view in views:
            index_size = view.index_size
            view.queries += [QueryHelper({"group" : "true"}, index_size,
                             self.years * self.months * self.days),
                             QueryHelper({"group_level" : "1"}, index_size,
                             self.years),
                             QueryHelper({"group_level" : "2"}, index_size,
                             self.years * self.months),
                             QueryHelper({"group_level" : "3"}, index_size,
                             self.years * self.months * self.days)]

    def add_all_query_sets(self, views = None):
        self.add_stale_queries(views)
        self.add_startkey_endkey_queries(views)
        self.add_startkey_endkey_docid_queries(views)
        self.add_group_count_queries(views)

    # views for this dataset
    def create_views(self, rest):
        vfn1 = 'function (doc) { if(doc.job_title !== undefined) { var myregexp = new RegExp("^UI "); if(doc.job_title.match(myregexp)){ emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}}'
        vfn2 = 'function (doc) { if(doc.job_title !== undefined) { var myregexp = new RegExp("^System "); if(doc.job_title.match(myregexp)){ emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}}'
        vfn3 = 'function (doc) { if(doc.job_title !== undefined) { var myregexp = new RegExp("^Senior "); if(doc.job_title.match(myregexp)){ emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}}'
        vfn4 = 'function (doc) { if(doc.job_title !== undefined) emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] ); }'

        full_index_size = self.calc_total_doc_count()
        partial_index_size = self.calc_total_doc_count()/3

        return [QueryView(rest, full_index_size,    fn_str = vfn4),
                QueryView(rest, partial_index_size, fn_str = vfn1),
                QueryView(rest, partial_index_size, fn_str = vfn2),
                QueryView(rest, partial_index_size, fn_str = vfn3),
                QueryView(rest, full_index_size,    fn_str = vfn4, reduce_fn="_count")]

    def get_data_sets(self):
        return [self.sys_admin_info, self.ui_eng_info, self.senior_arch_info]

    def load(self, tc, view, verify_docs_loaded = True):
        data_threads = []
        for info in self.get_data_sets():
            t = Thread(target=self._iterative_load,
                       name="iterative_load",
                       args=(info, tc, view, self.docs_per_day, verify_docs_loaded))
            data_threads.append(t)
            t.start()

        for t  in data_threads:
            t.join()

    def _iterative_load(self, info, tc, view, loads_per_iteration, verify_docs_loaded):
        loaded_docs = []

        try:
            smart = VBucketAwareMemcached(self.rest, self.bucket)
            for i in range(1,self.years + 1):
                for j in range(1, self.months + 1):
                    doc_sets = []
                    for k in range(1, self.days + 1):
                        kv_template = {"name": "employee-${prefix}-${seed}",
                                       "join_yr" : 2007+i, "join_mo" : j, "join_day" : k,
                                       "email": "${prefix}@couchbase.com",
                                       "job_title" : info["title"].encode("utf-8","ignore"),
                                       "type" : info["type"].encode("utf-8","ignore"),
                                       "desc" : info["desc"].encode("utf-8", "ignore")}
                        options = {"size": 256, "seed":  str(uuid.uuid4())[:7]}
                        docs = DocumentGenerator.make_docs(loads_per_iteration, kv_template, options)
                        doc_sets.append(docs)
                    #load docs
                    self._load_chunk(smart, doc_sets)
        except Exception as ex:
            view.results.addError(tc, sys.exc_info())
            raise ex

    def _load_chunk(self, smart, doc_sets):
        for docs in doc_sets:
            idx = 0
            for value in docs:
                value = value.encode("utf-8", "ignore")
                json_map = json.loads(value, encoding="utf-8")
                doc_id = "{0}{1}-{2}_{3}_{4}".format(json_map["type"],
                                                     str(idx).zfill(4),
                                                     json_map["join_yr"],
                                                     json_map["join_mo"],
                                                     json_map["join_day"])
                del json_map["_id"]
                smart.memcached(doc_id).set(doc_id, 0, 0, json.dumps(json_map))
                idx += 1

class SimpleDataSet:
    def __init__(self, rest, num_docs):
        self.num_docs = num_docs
        self.views = self.create_views(rest)
        self.name = "simple_dataset"
        self.kv_store = ClientKeyValueStore()
        self.kv_template = {"name": "doc-${prefix}-${seed}-", "age": "${prefix}"}

    def create_views(self, rest):
        view_fn = 'function (doc) {if(doc.age !== undefined) { emit(doc.age, doc.name);}}'
        return [QueryView(rest, self.num_docs, fn_str = view_fn)]

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

    def add_include_docs_queries(self, views = None):
        views = views or self.views

        for view in views:
            index_size = view.index_size
            view.queries += [QueryHelper({"include_docs" : "true"}, index_size)]


    def add_startkey_endkey_queries(self, views = None):

        if views is None:
            views = self.views

        for view in views:
            index_size = view.index_size
            start_key = index_size/2
            end_key = index_size - 1000
            view.queries += [QueryHelper({"start_key"     : end_key,
                                          "end_key"       : start_key,
                                          "descending"     : "true"},
                                            end_key - start_key + 1),
                             QueryHelper({"start_key"     : end_key,
                                          "end_key"       : start_key,
                                          "descending"     : "true"},
                                           end_key - start_key + 1),
                             QueryHelper({"end_key"       : end_key},
                                            end_key + 1),
                             QueryHelper({"end_key"       : end_key,
                                          "inclusive_end" : "false"}, end_key),
                             QueryHelper({"start_key"     : start_key},
                                            index_size - start_key)]
    def add_stale_queries(self, views = None):
        if views is None:
            views = self.views

        for view in views:
            index_size = view.index_size

        view.queries += [QueryHelper({"stale" : "false"}, index_size),
                         QueryHelper({"stale" : "ok"}, index_size),
                         QueryHelper({"stale" : "update_after"}, index_size)]


    def add_all_query_sets(self, views = None):
        self.add_startkey_endkey_queries(views)
        self.add_stale_queries(views)

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
                 type_ = "view"):

        self.params = params

        # number of docs this query should return
        self.expected_num_docs = expected_num_docs
        self.expected_num_groups = expected_num_groups
        self.type_ = type_   # "view" or "all_docs"

