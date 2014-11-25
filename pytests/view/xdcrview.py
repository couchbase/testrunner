from xdcr.xdcrbasetests import XDCRReplicationBaseTest
from couchbase_helper.documentgenerator import BlobGenerator
from createdeleteview import CreateDeleteViewTests
from membase.api.exception import QueryViewException

class XDCRViewTests(XDCRReplicationBaseTest, CreateDeleteViewTests):
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
        if self._replication_direction_str in "bidirection":
            self.gen_create2 = BlobGenerator('LoadTwo', 'LoadTwo', self._value_size, end=self.num_items)
            self.gen_delete2 = BlobGenerator('LoadTwo', 'LoadTwo-', self._value_size,
                start=int((self.num_items) * (float)(100 - self._percent_delete) / 100), end=self.num_items)
            self.gen_update2 = BlobGenerator('LoadTwo', 'LoadTwo-', self._value_size, start=0,
                end=int(self.num_items * (float)(self._percent_update) / 100))

    def tearDown(self):
        super(XDCRViewTests, self).tearDown()

    def _query_view(self, exp_items=None):
        query = {"stale" : "false", "full_set" : "true", "connection_timeout" : 60000}
        for bucket, self.ddoc_view_map in self.bucket_ddoc_map.items():
            for ddoc_name, view_list in self.ddoc_view_map.items():
                try:
                    for view in view_list:
                            self.cluster.query_view(self.master, ddoc_name, view.name, query, exp_items, bucket)
                except QueryViewException:
                    self.fail("Error occurred querying view")

    def _run_ddoc_ops(self, server, view_ops):
        tasks = []
        self.master = server
        buckets = self._get_cluster_buckets(self.master)
        if view_ops in ["update", "delete"]:
            for bucket in buckets:
                tasks.extend(self._async_execute_ddoc_ops(view_ops, self.test_with_view, self.num_ddocs / 2,
                                                        self.num_views_per_ddoc / 2, "dev_test", "v1", bucket=bucket))
        elif view_ops == "query":
            if self.stale_param in ["false", "ok", "update_after"]:
                query = {"stale" : self.stale_param, "full_set" : "true"}
            for bucket, self.ddoc_view_map in self.bucket_ddoc_map.items():
                for ddoc_name, view_list in self.ddoc_view_map.items():
                    for view in view_list:
                        tasks.append(self.cluster.async_query_view(server, ddoc_name, view.name, query))
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
        verify_src = False
        self._load_all_buckets(self.src_master, self.gen_create, "create", 0)
        if self._replication_direction_str in "bidirection":
            self._load_all_buckets(self.dest_master, self.gen_create2, "create", 0)
        for self.master in [self.src_master, self.dest_master]:
            buckets = self._get_cluster_buckets(self.master)
            for bucket in buckets:
                self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc, "dev_test", "v1", bucket=bucket)
            self._query_view()
            self.sleep(self.wait_timeout / 2)

        tasks = []
        if self.ddoc_ops is not None:
            tasks = self._run_ddoc_ops(self.src_master, self.ddoc_ops)
        if self.ddoc_ops_dest is not None:
            tasks += self._run_ddoc_ops(self.dest_master, self.ddoc_ops_dest)
        tasks_ops = []
        if self._doc_ops is not None:
            if "update" in self._doc_ops:
                tasks_ops.extend(self._async_load_all_buckets(self.src_master, self.gen_update, "update", self._expires))
            if "delete" in self._doc_ops:
                tasks_ops.extend(self._async_load_all_buckets(self.src_master, self.gen_delete, "delete", 0))
            self.sleep(5)
        if self._doc_ops_dest is not None:
            if "update" in self._doc_ops_dest:
                tasks_ops.extend(self._async_load_all_buckets(self.dest_master, self.gen_update2, "update", self._expires))
            if "delete" in self._doc_ops_dest:
                tasks_ops.extend(self._async_load_all_buckets(self.dest_master, self.gen_delete2, "delete", 0))
            self.sleep(5)
        for task in tasks:
                task.result(self._poll_timeout)
        max_retry = 20
        retry = 0
        while retry < max_retry and set([task.state for task in tasks_ops]) != set(["FINISHED"]):
            self._query_view()
            retry += 1

        if self._replication_direction_str in "unidirection":
            self.merge_buckets(self.src_master, self.dest_master, bidirection=False)
        elif self._replication_direction_str in "bidirection":
            self.merge_buckets(self.src_master, self.dest_master, bidirection=True)
            verify_src = True

        self.verify_results()
        self.sleep(self.wait_timeout / 2)
        for bucket in buckets:
            self.num_items = sum([len(kv_store) for kv_store in bucket.kvs.values()])
            break
        self._verify_views(verify_src=verify_src)
