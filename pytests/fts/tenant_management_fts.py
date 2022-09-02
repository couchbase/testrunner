# coding=utf-8

from .fts_base import FTSBaseTest



class TenantManagementFTS(FTSBaseTest):

    def setUp(self):
        super(TenantManagementFTS, self).setUp()

    def tearDown(self):
        super(TenantManagementFTS, self).tearDown()

    def suite_setUp(self):
        pass

    def suite_tearDown(self):
        pass

    def create_simple_default_index(self, data_loader_output=False):
        plan_params = self.construct_plan_params()
        self.load_data(generator=None, data_loader_output=data_loader_output, num_items=self._num_items)
        if not (self.bucket_storage == "magma"):
            self.wait_till_items_in_bucket_equal(self._num_items//2)
        self.create_fts_indexes_all_buckets(plan_params=plan_params)
        if self.restart_couchbase:
            self._cb_cluster.restart_couchbase_on_all_nodes()

        if self._update or self._delete:
            self.wait_for_indexing_complete()
            self.validate_index_count(equal_bucket_doc_count=True,
                                      zero_rows_ok=False)
            self.sleep(20)
            self.async_perform_update_delete(self.upd_del_fields)
            if self._update:
                self.sleep(60, "Waiting for updates to get indexed...")
        self.wait_for_indexing_complete()
        self.validate_index_count(equal_bucket_doc_count=True)