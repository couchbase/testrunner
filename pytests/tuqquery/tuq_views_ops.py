import math

from tuqquery.tuq import QueryTests
from remote.remote_util import RemoteMachineShellConnection
from membase.api.rest_client import RestConnection
from couchbase_helper.document import View

class QueriesViewsTests(QueryTests):
    def setUp(self):
        self.ddoc_name = "tuq_ddoc"
        super(QueriesViewsTests, self).setUp()
        self.map_fn = 'function (doc){emit([doc.join_yr, doc.join_mo],doc.name);}'
        self.view_name = "tuq_view"
        self.default_view = View(self.view_name, self.map_fn, None, False)

    def suite_setUp(self):
        super(QueriesViewsTests, self).suite_setUp()

    def tearDown(self):
        try:
            self.cluster.delete_view(self.master, self.ddoc_name, None)
        except:
            self.log.error("Ddoc %s wasn't deleted" % self.ddoc_name)
        super(QueriesViewsTests, self).tearDown()

    def suite_tearDown(self):
        super(QueriesViewsTests, self).suite_tearDown()

    def test_creating_views_query(self):
        self.test_array_agg()
        views = self.make_default_views(self.view_name, 10, different_map=True)
        tasks_view = self.async_create_views(self.master, self.ddoc_name, views)
        self.test_array_agg()
        for task in tasks_view:
            task.result(self.wait_timeout * 2)
        self.test_array_agg()

    def test_view_query(self):
        self.cluster.create_view(self.master, self.ddoc_name, self.default_view)
        self.test_array_agg()
        full_list = self.generate_full_docs_list(self.gens_load)
        task = self.cluster.async_query_view(self.master, self.ddoc_name,
                                             self.default_view.name, {"stale" : "false"},
                                             len(full_list))
        self.test_array_agg()
        task.result(self.wait_timeout)
        self.test_array_agg()

    def test_view_query_simple(self):
        self.cluster.create_view(self.master, self.ddoc_name, self.default_view)
        self.query = 'SELECT join_yr, join_mo, name from default' +\
            ' ORDER BY join_yr, join_mo, name'
        tool_res = self.run_cbq_query()
        view_res = RestConnection(self.master).\
                  query_view(self.ddoc_name, self.default_view.name,
                             "default", {"stale" : "false"})
        self._compare_view_and_tool_result(view_res['rows'], tool_res["results"])

    def test_view_query_limit_offset(self):
        self.cluster.create_view(self.master, self.ddoc_name, self.default_view)
        self.query = 'SELECT join_yr, join_mo, name from default' +\
            ' WHERE join_yr > 2010 ORDER BY join_yr, join_mo, name' +\
            ' LIMIT 10 OFFSET 10'
        tool_res = self.run_cbq_query()
        view_res = RestConnection(self.master).\
                  query_view(self.ddoc_name, self.default_view.name,
                             "default", {"stale" : "false",
                                         "startkey" : "[2011,null]",
                                         "limit" : 10, "skip" : 10})
        self._compare_view_and_tool_result(view_res['rows'], tool_res["results"],
                                           check_values=False)

    def test_view_query_start_end(self):
        self.cluster.create_view(self.master, self.ddoc_name, self.default_view)
        self.query = 'SELECT join_yr, join_mo, name from default' +\
            ' WHERE join_yr == 2011 AND join_mo > 3 AND join_mo < 7' +\
            'ORDER BY join_yr, join_mo, name'
        tool_res = self.run_cbq_query()
        view_res = RestConnection(self.master).\
                  query_view(self.ddoc_name, self.default_view.name,
                             "default", {"stale" : "false",
                                         "startkey" : "[2011,4]",
                                         "endkey" : "[2011,6]"})
        self._compare_view_and_tool_result(view_res['rows'], tool_res["results"])

    def test_view_query_order(self):
        self.cluster.create_view(self.master, self.ddoc_name, self.default_view)
        self.query = 'SELECT join_yr, join_mo, name from default' +\
            ' WHERE join_yr == 2011 ORDER BY join_yr DESC, join_mo DESC, name DESC'
        tool_res = self.run_cbq_query()
        view_res = RestConnection(self.master).\
                  query_view(self.ddoc_name, self.default_view.name,
                             "default", {"stale" : "false",
                                         "endkey" : "[2011,1]",
                                         "descending" : "true"})
        self._compare_view_and_tool_result(view_res['rows'], tool_res["results"])

    def _compare_view_and_tool_result(self, view_result, tool_result, check_values=True):
        self.log.info("Comparing result ...")
        formated_tool_res = [{"key" : [doc["join_yr"], doc["join_mo"]],
                              "value" : doc["name"]} for doc in tool_result]
        formated_view_res = [{"key" : row["key"],
                              "value": row["value"]} for row in view_result]

        self.assertEqual(len(formated_tool_res), len(formated_view_res),
                         "Query results are not equal. Tool %s, view %s" %(
                                        len(formated_tool_res), len(formated_view_res)))
        self.log.info("Length is equal")
        self.assertEqual([row["key"] for row in formated_tool_res],
                         [row["key"] for row in formated_view_res],
                         "Query results sorting are not equal./n Actual %s, Expected %s" %(
                                                formated_tool_res[:100],formated_view_res[:100]))
        self.log.info("Sorting is equal")
        if check_values:
            formated_tool_res = sorted(formated_tool_res, key=lambda doc: (doc['key'],
                                                            doc['value']))
            formated_view_res = sorted(formated_view_res, key=lambda doc: (doc['key'],
                                                            doc['value']))
            self.assertTrue(formated_tool_res == formated_view_res,
                             "Query results sorting are not equal. View but not tool has [%s]" %(
                                [r for r in view_result if r in formated_tool_res]))
            self.log.info("Items are equal")