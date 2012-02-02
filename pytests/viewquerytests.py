import uuid
import time
import unittest
from membase.api.rest_client import RestConnection
from viewtests import ViewBaseTests

class ViewQueryTests(unittest.TestCase):

    def setUp(self):

        self.view_prefix = str(uuid.uuid4())[:7]

        # generic view attributes
        self.view_spec = {'bucket' : "default",
                          'name' : "dev_test_view-{0}".format(self.view_prefix),
                          'fn_str' : 'function (doc) {if(doc.name) { emit(doc.name, doc);}}'}

        ViewBaseTests.common_setUp(self)

    def tearDown(self):
        ViewBaseTests.common_tearDown(self)

    def test_query_range(self):

        # create view with age as key in map function
        self.view_spec['fn_str'] = 'function (doc) {if(doc.age) { emit(doc.age, doc);}}'
        self._create_view_from_spec(self.view_spec)

        # load some docs
        doc_names = ViewBaseTests._load_docs(self, self.num_docs, self.view_prefix, False)

        # append range queries to list
        query_spec = self.add_range_queries([], 1000, 7999)
        # verify results
        self.run_queries(query_spec, self.view_spec['name'], self.num_docs)

    def test_modify_query_during_create(self):
        self._toggle_query_during_ops(query_during_create=True)

    def test_modify_query_during_delete(self):
        self._toggle_query_during_ops(query_during_delete=True)

    def test_modify_query_during_rebalance_in(self):
        self._toggle_query_during_ops(query_during_rebalance_in=True)

    def test_modify_query_during_rebalance_out(self):
        self._toggle_query_during_ops(query_during_rebalance_out=True)

    def test_modify_query_during_all_ops(self):
        self._toggle_query_during_ops(True, True, True, True)

    def _toggle_query_during_ops(self, query_during_create=False,
                                 query_during_delete=False,
                                 query_during_rebalance_in=False,
                                 query_during_rebalance_out=False):
        rest = self._rconn()
        prefix = self.view_prefix
        num_docs = self.num_docs
        num_of_doc_to_delete = ViewBaseTests.parami("docs-to-delete", 9000)
        view_name = "dev_test_view-{0}".format(prefix)
        query_spec = self.add_stale_queries([])

        #  load docs
        doc_names = ViewBaseTests._load_docs(self, num_docs, prefix, False)
        res = self._create_view_from_spec(self.view_spec)
        if query_during_create:
            self.run_queries(query_spec, view_name)
        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)

        # rebalance in
        ViewBaseTests._begin_rebalance_in(self)
        if query_during_rebalance_in == True:
            self.run_queries(query_spec, view_name, num_docs) # verify results
        ViewBaseTests._end_rebalance(self)

        # delete doc
        num_of_deleted_docs = ViewBaseTests._delete_doc_range(self, prefix,
            0, num_of_doc_to_delete)
        remaining_doc_count = num_docs  - num_of_deleted_docs
        if query_during_delete == True:
            self.run_queries(query_spec, view_name)
        doc_names = doc_names[num_of_deleted_docs:]
        ViewBaseTests._verify_docs_doc_name(self, doc_names, prefix)

        # rebalance out
        ViewBaseTests._begin_rebalance_out(self)
        if query_during_rebalance_out == True:
            self.run_queries(query_spec, view_name,
                num_docs - num_of_deleted_docs) # verify results
        ViewBaseTests._end_rebalance(self)

    def run_queries(self, query_spec, view_name, query_doc_count = None):
        rest = self._rconn()
        for params in query_spec:
            expected_doc_count = query_doc_count
            self.log.info("Quering view {0} with params: {1}".format(view_name, params));
            results = ViewBaseTests._get_view_results(self, rest,
                "default", view_name, limit=None, extra_params=params)
            num_keys = len(ViewBaseTests._get_keys(self, results))
            if expected_doc_count != None:
                attempt = 0
                delay = 15
                if "start_key" in params:  # start key with possible end key
                    if "end_key" in params:  # bounded range
                        expected_doc_count = params["end_key"] - params["start_key"] + 1
                    else :  # start key without end key
                        expected_doc_count = expected_doc_count - params["start_key"]
                elif "end_key" in params:  # end key without start key
                    expected_doc_count = params["end_key"] + 1

                # first verify all doc_names get reported in the view
                while attempt < 30 and num_keys != expected_doc_count:
                    msg = "view returned {0} items, expected to return {1} items"
                    self.log.info(msg.format(num_keys, expected_doc_count))
                    self.log.info("trying again in {0} seconds".format(delay))
                    time.sleep(delay)
                    attempt += 1
                    results = ViewBaseTests._get_view_results(self, rest,
                        "default", view_name, limit=None, extra_params=params)
                    num_keys = len(ViewBaseTests._get_keys(self, results))
                msg = "Query failed: {0} Documents Retrieved,  expected {1}"
                self.assertTrue(num_keys == expected_doc_count,
                    msg.format(num_keys, expected_doc_count))

    def add_range_queries(self, query_spec, start_key, end_key):
        query_spec.append({"start_key"   : start_key,
                            "end_key"    : end_key,
                            "decending"  : "true"})
        query_spec.append({"end_key"     : end_key})
        query_spec.append({"start_key"   : start_key})
        return query_spec

    def add_stale_queries(self, query_spec):
        query_spec.append({"stale" : "false"})
        query_spec.append({"stale" : "update_after"})
        query_spec.append({"stale" : "ok"})
        return query_spec

    def add_debug_queries(self, query_spec):
        query_spec.append({"debug" : "true"})
        return query_spec

    def _create_view_from_spec(self, view_spec):
        view_spec['fn'] = self._get_view_fn_from_spec(view_spec)
        res = self._rconn().create_view(view_spec['bucket'],
                                        view_spec['name'], view_spec['fn'])
        return res

    def _get_view_fn_from_spec(self, spec):
        return ViewBaseTests._create_function(self, self._rconn(),
                                              spec['bucket'],
                                              spec['name'],
                                              spec['fn_str'])

    # retrieve default rest connection associated with the master server
    def _rconn(self):
        return RestConnection(self.servers[0])
