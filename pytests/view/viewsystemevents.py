import copy
from threading import Thread, Event
from couchbase_helper.document import DesignDocument, View
from membase.helper.cluster_helper import ClusterOperationHelper
from pytests.security.ntonencryptionBase import ntonencryptionBase
from pytests.view import createdeleteview
from basetestcase import BaseTestCase
from lib.SystemEventLogLib.view_service_events import ViewsServiceEvents
from lib import global_vars


class ViewsSystemEventLog(createdeleteview.CreateDeleteViewTests,BaseTestCase):
    def setUp(self):
        try:
            super(ViewsSystemEventLog, self).setUp()
            self.bucket_ddoc_map = {}
            self.ddoc_ops = self.input.param("ddoc_ops", None)
            self.boot_op = self.input.param("boot_op", None)
            self.test_with_view = self.input.param("test_with_view", False)
            self.num_views_per_ddoc = self.input.param("num_views_per_ddoc", 1)
            self.num_ddocs = self.input.param("num_ddocs", 1)
            self.gen = None
            self.is_crashed = Event()
            self.default_design_doc_name = "Doc1"
            self.default_map_func = 'function (doc) { emit(doc.age, doc.first_name);}'
            self.updated_map_func = 'function (doc) { emit(null, doc);}'
            self.default_view = View("View", self.default_map_func, None, False)
            self.fragmentation_value = self.input.param("fragmentation_value", 80)

        except Exception as ex:
            self.input.test_params["stop-on-failure"] = True
            self.log.error("SETUP WAS FAILED. ALL TESTS WILL BE SKIPPED")
            self.fail(ex)

    def tearDown(self):
        super(ViewsSystemEventLog, self).tearDown()

    def test_view_system_events_create_doc(self):

        self._load_doc_data_all_buckets()
        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc,
                                   bucket=bucket)
        global_vars.system_event_logs.add_event(
            ViewsServiceEvents.DDoc_Created(self.master))
        self.log.info("============= Validating DDocCreated event ===============")
        self.system_events.validate(server=self.master)
        self.log.info("==========================================================")

        self._wait_for_stats_all_buckets([self.master])

        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

    def test_view_system_events_delete_doc(self):

        self._load_doc_data_all_buckets()
        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc,
                                   bucket=bucket)

        self._wait_for_stats_all_buckets([self.master])

        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

        self._execute_ddoc_ops("delete", self.test_with_view,
                               self.num_ddocs // 2, self.num_views_per_ddoc // 2, bucket=bucket)

        global_vars.system_event_logs.add_event(
            ViewsServiceEvents.DDoc_Deleted(self.master))
        self.system_events.validate(server=self.master)
        self._wait_for_stats_all_buckets([self.master])

        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()


    def test_view_system_events_update_doc(self):

        self._load_doc_data_all_buckets()
        for bucket in self.buckets:
            self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc,
                                   bucket=bucket)

        self._wait_for_stats_all_buckets([self.master])

        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

        self._execute_ddoc_ops("delete", self.test_with_view,
                               self.num_ddocs // 2, self.num_views_per_ddoc // 2, bucket=bucket)
        global_vars.system_event_logs.add_event(
            ViewsServiceEvents.DDoc_Updated(self.master))
        self.system_events.validate(server=self.master)

        self._wait_for_stats_all_buckets([self.master])
        self._verify_ddoc_ops_all_buckets()
        self._verify_ddoc_data_all_buckets()

    def test_view_system_events_settings_change(self):

            self._load_doc_data_all_buckets()
            for bucket in self.buckets:
                self._execute_ddoc_ops("create", self.test_with_view, self.num_ddocs, self.num_views_per_ddoc,
                                       bucket=bucket)

            self._wait_for_stats_all_buckets([self.master])

            self._verify_ddoc_ops_all_buckets()
            self._verify_ddoc_data_all_buckets()

            ntonencryptionBase().disable_nton_cluster([self.master])
            ntonencryptionBase().setup_nton_cluster([self.master])
            ntonencryptionBase().disable_nton_cluster([self.master])

            global_vars.system_event_logs.add_event(
                ViewsServiceEvents.DDoc_Settings_change(self.master))
            self.system_events.validate(server=self.master)
