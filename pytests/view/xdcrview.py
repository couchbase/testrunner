from xdcr.xdcrnewbasetests import XDCRNewBaseTest
from .createdeleteview import CreateDeleteViewTests
from membase.api.exception import QueryViewException
from couchbase_helper.document import View

class XDCRViewTests(XDCRNewBaseTest, CreateDeleteViewTests):
    def setUp(self):
        super(XDCRViewTests, self).setUp()
        self.bucket_ddoc_map = {}
        self.ddoc_ops = self._input.param("ddoc_ops", None)
        self.ddoc_ops_dest = self._input.param("ddoc_ops_dest", None)
        self.stale_param = self._input.param("stale_param", "false")
        self.num_views_per_ddoc = self._input.param("num_views_per_ddoc", 1)
        self.num_ddocs = self._input.param("num_ddocs", 1)
        self.test_with_view = True
        self.updated_map_func = 'function (doc) { emit(null, doc);}'
        self.src_cluster = self.get_cb_cluster_by_name('C1')
        self.src_master = self.src_cluster.get_master_node()
        self.dest_cluster = self.get_cb_cluster_by_name('C2')
        self.dest_master = self.dest_cluster.get_master_node()
        self.defaul_map_func = "function (doc) {\n  emit(doc._id, doc);\n}"
        self.default_view_name = "default_view"
        self.default_view = View(self.default_view_name, self.defaul_map_func, None)
        self.cluster = self.src_cluster.get_cluster()
            
    def tearDown(self):
        super(XDCRViewTests, self).tearDown()

    def _query_view(self, exp_items=None):
        query = {"stale" : "false", "full_set" : "true", "connection_timeout" : 60000}
        for bucket, self.ddoc_view_map in list(self.bucket_ddoc_map.items()):
            for ddoc_name, view_list in list(self.ddoc_view_map.items()):
                try:
                    for view in view_list:
                            self.cluster.query_view(self.master, ddoc_name, view.name, query, exp_items, bucket)
                except QueryViewException:
                    self.fail("Error occurred querying view")

    def _run_ddoc_ops(self, cluster, view_ops):
        tasks = []
        master = cluster.get_master_node();
        buckets = cluster.get_buckets();
        if view_ops in ["update", "delete"]:
            for bucket in buckets:
                tasks.extend(self._async_execute_ddoc_ops(view_ops, self.test_with_view, self.num_ddocs // 2,
                                                        self.num_views_per_ddoc // 2, "dev_test", "v1", bucket=bucket))
        elif view_ops == "query":
            if self.stale_param in ["false", "ok", "update_after"]:
                query = {"stale" : self.stale_param, "full_set" : "true"}
            for bucket, self.ddoc_view_map in list(self.bucket_ddoc_map.items()):
                for ddoc_name, view_list in list(self.ddoc_view_map.items()):                                                                      
                    for view in view_list:
                        tasks.append(self.cluster.async_query_view(master, ddoc_name, view.name, query))
        return tasks

    def _verify_views(self, verify_src):
        srv_list = [self.dest_master]
        if verify_src == True:
            srv_list.append(self.src_master)
        for server in srv_list:
            self.master = server
            self._verify_ddoc_ops_all_buckets()
            self._verify_ddoc_data_all_buckets()

    """CBQE-1867,1868,1869 : Delete,Update and Query Views during Replication"""
    def test_views_with_xdcr(self):
        self.setup_xdcr_and_load()
        for cluster in [self.src_cluster, self.dest_cluster]:
            buckets = cluster.get_buckets()
            self.master = cluster.get_master_node()
            self.wait_timeout = self._wait_timeout;
            for bucket in buckets:
                self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc, "dev_test", "v1", bucket=bucket)
            self._query_view()
            self.sleep(self.wait_timeout // 2)

        tasks = []
        if self.ddoc_ops is not None:
            tasks = self._run_ddoc_ops(self.src_cluster, self.ddoc_ops)
        if self.ddoc_ops_dest is not None:
            tasks += self._run_ddoc_ops(self.dest_cluster, self.ddoc_ops_dest)
        for task in tasks:
            task.result(self._poll_timeout)
            
        self.async_perform_update_delete()

        self.verify_results()
        self.sleep(self.wait_timeout // 2)
        
        self._verify_views(True)
