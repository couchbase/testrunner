import uuid
import logger
import time
import unittest
from threading import Thread
from membase.api.rest_client import RestConnection
from viewtests import ViewBaseTests
from memcached.helper.data_helper import VBucketAwareMemcached, DocumentGenerator
import json


class ViewQueryTests(unittest.TestCase):

    def setUp(self):
        ViewBaseTests.common_setUp(self)

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

    def test_simple_dataset_stale_queries(self):
        # init dataset for test
        data_set = SimpleDataSet(self._rconn(), self.num_docs)
        queries = data_set.get_stale_queries()
        self._query_test_init(data_set, queries)

    def test_simple_dataset_startkey_endkey_queries(self):
        data_set = SimpleDataSet(self._rconn(), self.num_docs)
        queries = data_set.get_startkey_endkey_queries()
        self._query_test_init(data_set, queries)

    def test_simple_dataset_startkey_endkey_docid_queries(self):
        data_set = SimpleDataSet(self._rconn(), self.num_docs)
        queries = data_set.get_startkey_endkey_docid_queries()
        self._query_test_init(data_set, queries)

    def test_simple_dataset_all_queries(self):
        data_set = SimpleDataSet(self._rconn(), self.num_docs)
        queries = data_set.get_all_query_sets()
        self._query_test_init(data_set, queries)

    def test_employee_dataset_startkey_endkey_queries(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        queries = data_set.get_startkey_endkey_queries()
        self._query_test_init(data_set, queries)

    def test_employee_dataset_startkey_endkey_queries_rebalance_in(self):
        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        queries = data_set.get_startkey_endkey_queries()
        self._query_test_init(data_set, queries, False)

        # rebalance_in and verify loaded data
        ViewBaseTests._begin_rebalance_in(self)
        self._query_all_views(data_set.views)
        ViewBaseTests._end_rebalance(self)

    def test_employee_dataset_startkey_endkey_queries_rebalance_out(self):
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)


        queries = data_set.get_startkey_endkey_queries()
        self._query_test_init(data_set, queries, False)

        # rebalance_out and verify loaded data
        ViewBaseTests._begin_rebalance_out(self)
        self._query_all_views(data_set.views)
        ViewBaseTests._end_rebalance(self)


    def test_employee_dataset_stale_queries(self):
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        queries = data_set.get_stale_queries()
        self._query_test_init(data_set, queries)

    def test_employee_dataset_all_queries(self):
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        docs_per_day = self.input.param('docs-per-day', 200)
        data_set = EmployeeDataSet(self._rconn(), docs_per_day)

        queries = data_set.get_all_query_sets()
        self._query_test_init(data_set, queries)

    def test_all_datasets_all_queries(self):
        ViewBaseTests._begin_rebalance_in(self)
        ViewBaseTests._end_rebalance(self)

        ds1 = EmployeeDataSet(self._rconn())
        ds2 = SimpleDataSet(self._rconn(), self.num_docs)
        data_sets = [ds1, ds2]

        # load and query all views and datasets
        test_threads = []
        for ds in data_sets:
            t = Thread(target=self._query_test_init,
                       name=ds.name,
                       args=(ds, ds.get_all_query_sets(), False))
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
    # verification is optional
    ###
    def _query_test_init(self, data_set, queries, verify_results = True):
        tcObj = self

        # init queries
        views = data_set.views
        for view in views:
            view.queries = queries

        # start loading data
        t = Thread(target=data_set.load,
                   name="load_data_set",
                   args=(tcObj,views[0]))
        t.start()

        # run queries while loading data
        while(t.is_alive()):
            self._query_all_views(views)
            time.sleep(5)
        t.join()

        # results will be verified if verify_results set
        if verify_results:
            self._query_all_views(views, verify_results)


    ##
    # run all queries for all views in parallel
    ##
    def _query_all_views(self, views, verify_results = False):
        tcObj = self
        query_threads = []
        for view in views:
            t = Thread(target=view.run_queries,
               name="query-{0}".format(view.name),
               args=(tcObj, verify_results))
            query_threads.append(t)
            t.start()

        [t.join() for t in query_threads]


    # retrieve default rest connection associated with the master server
    def _rconn(self):
        return RestConnection(self.servers[0])

class View:
    def __init__(self, rest,
                 bucket = "default",
                 prefix=None,
                 name = None,
                 fn_str = None,
                 queries = [],
                 create_on_init = True):

        self.log = logger.Logger.get_logger()
        default_prefix = str(uuid.uuid4())[:7]
        default_fn_str = 'function (doc) {if(doc.name) { emit(doc.name, doc);}}'

        self.bucket = bucket
        self.prefix = (prefix, default_prefix)[prefix is None]
        default_name = "dev_test_view-{0}".format(self.prefix)
        self.name = (name, default_name)[name is None]
        self.fn_str = (fn_str, default_fn_str)[fn_str is None]
        self.fn = self._set_view_fn_from_attrs(rest)

        # queries defined for this view
        self.queries = queries

        if create_on_init:
            self.create(rest)


    def _set_view_fn_from_attrs(self, rest):
        return ViewBaseTests._create_function(self, rest,
                                              self.bucket,
                                              self.name,
                                              self.fn_str)

    # create this view
    def create(self, rest):
        res = rest.create_view(self.bucket,
                               self.name,
                               self.fn)
        return res

    # query this view
    def run_queries(self, tc, verify_results = False):
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

                # first verify all doc_names get reported in the view
                while attempt < 10 and num_keys != expected_num_docs:
                    self.log.info("Quering view {0} with params: {1}".format(view_name, params));
                    results = ViewBaseTests._get_view_results(tc, rest,
                        "default", view_name, limit=None, extra_params=params)
                    num_keys = len(ViewBaseTests._get_keys(self, results))
                    self.log.info("retrieved {0} keys expected: {1}".format(num_keys, expected_num_docs));
                    self.log.info("trying again in {0} seconds".format(delay))
                    time.sleep(delay)
                    attempt += 1
                msg = "Query failed: {0} Documents Retrieved,  expected {1}"
                tc.assertTrue(num_keys == expected_num_docs,
                    msg.format(num_keys, expected_num_docs))

class EmployeeDataSet:
    def __init__(self, rest, docs_per_day = 200, bucket = "default"):
        self.docs_per_day = docs_per_day
        self.years = 1
        self.months = 12
        self.days = 28
        self.sys_admin_info = {"title" : "System Administrator and heliport manager",
                              "desc" : "...Last but not least, as the heliport manager, you will help maintain our growing fleet of remote controlled helicopters, that crash often due to inexperienced pilots.  As an independent thinker, you may be free to replace some of the technologies we currently use with ones you feel are better. If so, you should be prepared to discuss and debate the pros and cons of suggested technologies with other stakeholders"}
        self.ui_eng_info = {"title" : "UI Engineer",
                           "desc" : "Couchbase server UI is one of the crown jewels of our product, which makes the Couchbase NoSQL database easy to use and operate, reports statistics on real time across large clusters, and much more. As a Member of Couchbase Technical Staff, you will design and implement front-end software for cutting-edge distributed, scale-out data infrastructure software systems, which is a pillar for the growing cloud infrastructure."}
        self.senior_arch_info = {"title" : "Senior Architect",
                               "desc" : "As a Member of Technical Staff, Senior Architect, you will design and implement cutting-edge distributed, scale-out data infrastructure software systems, which is a pillar for the growing cloud infrastructure. More specifically, you will bring Unix systems and server tech kung-fu to the team."}
        self.views = self.get_views(rest)
        self.bucket = bucket
        self.rest = rest
        self.name = "employee_dataset"


    def calc_total_doc_count(self):
        return self.years * self.months * self.days * self.docs_per_day * len(self.get_data_sets())

    def get_startkey_endkey_queries(self):
        total_doc_count = self.calc_total_doc_count()
        return [QueryHelper({"start_key" : "[2008,7,null]"}, total_doc_count/6),
                QueryHelper({"start_key" : "[2008,0,1]",
                             "end_key"   : "[2008,7,1]",
                             "inclusive_end" : "false"}, total_doc_count/6),
                QueryHelper({"start_key" : "[2008,0,1]",
                             "end_key"   : "[2008,7,1]",
                             "inclusive_end" : "true"}, total_doc_count/6 + self.docs_per_day),
                QueryHelper({"start_key" : "[2008,7,1]",
                             "end_key"   : "[2008,1,1]",
                             "descending"   : "true",
                             "inclusive_end" : "false"}, total_doc_count/6),
                QueryHelper({"start_key" : "[2008,7,1]",
                             "end_key"   : "[2008,1,1]",
                             "descending"   : "true",
                             "inclusive_end" : "true"}, total_doc_count/6 + self.docs_per_day)]

    def get_stale_queries(self):
        total_doc_count = self.calc_total_doc_count()
        return  [QueryHelper({"stale" : "false"}, total_doc_count/3),
                 QueryHelper({"stale" : "ok"}, total_doc_count/3),
                 QueryHelper({"stale" : "update_after"}, total_doc_count/3)]

    def get_all_query_sets(self):
        return self.get_startkey_endkey_queries() +  \
               self.get_stale_queries()

    # views for this dataset
    def get_views(self, rest):
        vfn1 = 'function (doc) { var myregexp = new RegExp("^UI "); if(doc.job_title.match(myregexp)){ emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}'
        vfn2 = 'function (doc) { var myregexp = new RegExp("^System "); if(doc.job_title.match(myregexp)){ emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}'
        vfn3 = 'function (doc) { var myregexp = new RegExp("^Senior "); if(doc.job_title.match(myregexp)){ emit([doc.join_yr, doc.join_mo, doc.join_day], [doc.name, doc.email] );}}'
        view1 = View(rest, fn_str = vfn1)
        view2 = View(rest, fn_str = vfn2)
        view3 = View(rest, fn_str = vfn3)
        return [view1, view2, view3]


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

        smart = VBucketAwareMemcached(self.rest, self.bucket)
        for i in range(1,self.years + 1):
            for j in range(1, self.months + 1):
                doc_sets = []
                for k in range(1, self.days + 1):
                    kv_template = {"name": "employee-${prefix}-${seed}",
                                   "join_yr" : 2007+i, "join_mo" : j, "join_day" : k,
                                   "email": "${prefix}@couchbase.com",
                                   "job_title" : info["title"].encode("utf-8","ignore"),
                                   "desc" : info["desc"].encode("utf-8", "ignore")}
                    options = {"size": 256, "seed":  str(uuid.uuid4())[:7]}
                    docs = DocumentGenerator.make_docs(loads_per_iteration, kv_template, options)
                    doc_sets.append(docs)
                #load docs
                self._load_chunk(smart, doc_sets)


    def _load_chunk(self, smart, doc_sets):
        for docs in doc_sets:
            for value in docs:
                value = value.encode("utf-8", "ignore")
                json_map = json.loads(value, encoding="utf-8")
                doc_id = json_map["_id"].encode("utf-8", "ignore")
                del json_map["_id"]
                smart.memcached(doc_id).set(doc_id, 0, 0, json.dumps(json_map))

class SimpleDataSet:
    def __init__(self, rest, num_docs):
        self.num_docs = num_docs
        self.views = self.get_views(rest)
        self.name = "simple_dataset"

    def get_views(self, rest):
        view_fn = 'function (doc) {if(doc.age !== undefined) { emit(doc.age, doc.id);}}'
        return [View(rest, fn_str = view_fn)]

    def load(self, tc, view, verify_docs_loaded = True):
        doc_names = ViewBaseTests._load_docs(tc, self.num_docs, view.prefix, verify_docs_loaded)
        return doc_names

    def get_startkey_endkey_queries(self):
        start_key = self.num_docs/2
        end_key = self.num_docs - 1000
        return [QueryHelper({"start_key"   : start_key,
                             "end_key"    : end_key,
                             "decending"  : "true"}, end_key - start_key + 1),
                QueryHelper({"start_key"   : start_key,
                             "end_key"    : end_key,
                             "decending"  : "true"}, end_key - start_key + 1),
                QueryHelper({"end_key"     : end_key}, end_key + 1),
                QueryHelper({"end_key"     : end_key,
                             "inclusive_end" : "false"}, end_key),
                QueryHelper({"start_key"   : start_key}, self.num_docs - start_key)]

    def get_startkey_endkey_docid_queries(self):
        start_key = self.num_docs/2
        start_key_docid_idx = start_key + 100
        end_key = self.num_docs - 1000
        startkey_docid = "\"{0}-{1}\"".format(self.views[0].prefix, start_key_docid_idx)
        return [QueryHelper({"start_key"   : start_key,
                             "end_key"    : end_key,
                             "startkey_docid" : startkey_docid,
                             "decending"  : "true"}, end_key - start_key_docid_idx + 1),
                QueryHelper({"start_key"   : start_key,
                             "end_key"    : end_key,
                             "startkey_docid" : startkey_docid,
                             "decending"  : "true"}, end_key - start_key_docid_idx + 1),
                QueryHelper({"start_key"   : start_key,
                             "startkey_docid"   : startkey_docid}, self.num_docs - start_key_docid_idx)]


    def get_stale_queries(self):
        total_doc_count = self.num_docs
        return  [QueryHelper({"stale" : "false"}, total_doc_count),
                 QueryHelper({"stale" : "ok"}, total_doc_count),
                 QueryHelper({"stale" : "update_after"}, total_doc_count)]


    def get_all_query_sets(self):
        return self.get_startkey_endkey_queries() +  \
               self.get_stale_queries()
               #self.get_startkey_endkey_docid_queries() + \

class QueryHelper:
    def __init__(self, params, expected_num_docs):
        self.params = params

        # number of docs this query should return
        self.expected_num_docs = expected_num_docs
