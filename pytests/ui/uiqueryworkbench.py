import re
from os.path import expanduser
from unittest import TestCase

from membase.api.rest_client import RestConnection
from uibasetest import *
from uisampletests import NavigationHelper


class QueryWorkbenchTests(BaseUITestCase):
    """
    For the first node the following services should be turn on: kv,n1ql,index
    """
    def setUp(self):
        super(QueryWorkbenchTests, self).setUp()
        self.bucketname = 'beer'
        self.baseHelper = BaseHelper(self)
        self.queryHelper = QueryWorkbenchHelper(self)
        self.baseHelper.login()
        self.baseHelper.loadSampleBucket(self.servers[0], self.bucketname)
        self.rest = RestConnection(self.servers[0])
        self.rest.set_indexer_storage_mode()
        self.rest.query_tool("DROP INDEX `beer`.`beer_index_sec` USING GSI;")

    def tearDown(self):
        super(QueryWorkbenchTests, self).tearDown()

    def test_create_indexes(self):
        expected_results = self.input.param('expected_result', None)
        if expected_results is not None:
            expected_results = expected_results.replace('_STAR_', '*').replace('_SEM_', ';').decode('unicode_escape')\
                .split('|')  # 4.7 vs 4.6 versions
        summary_result = self.input.param('summary_result', '')
        summary_result = summary_result.replace('_STAR_', '*').replace('_SEM_', ';').decode('unicode_escape')
        result_mode = self.input.param('mode', 'JSON')
        if self.rest.get_nodes()[0].version <= '4.7' and result_mode in ['Plan Text', 'Plan']:
            self.log.info("skipp 'Plan Text', 'Plan' modes in version < 4.7")
            return
        init_query = self.input.param('init_query', '').replace('_STAR_', '*').replace('_SEM_', ';').decode(
            'unicode_escape')
        check_query = self.input.param('check_query', '').replace('_STAR_', '*').replace('_SEM_', ';').decode(
            'unicode_escape')

        NavigationHelper(self).navigate('Query')
        if init_query:
            self.queryHelper.execute_query(init_query)
            self.queryHelper.controls.query_top_screen().view_next.click()
            self.assertEqual('{"no_data_yet": "hit execute to run query"}',
                             self.queryHelper.controls.query_results_box().result_json_mode.get_text())
        self.queryHelper.execute_query(check_query)
        if expected_results is not None:
            self.queryHelper.check_result(expected_results, mode=result_mode)
        if summary_result:
            self.queryHelper.check_summary_result(summary_result, mode=result_mode)
        if init_query:
            self.queryHelper.controls.query_top_screen().view_previous.click()
            self.assertEqual(init_query, self.queryHelper.controls.query_top_screen().query_editor_value.get_text())

    def test_bucket_analysis(self):
        init_analysis = self.input.param('init_analysis', None)
        expected_analysis = self.input.param('expected_analysis', None)
        init_analysis = init_analysis.replace('_STAR_', '*').replace('_SEM_', ';').decode(
            'unicode_escape').split('|')  # 4.7 vs 4.6
        expected_analysis = expected_analysis.replace('_STAR_', '*').replace('_SEM_', ';').decode(
            'unicode_escape')
        check_query = self.input.param('check_query', '').replace('_STAR_', '*').replace('_SEM_', ';').decode(
            'unicode_escape')

        NavigationHelper(self).navigate('Query')
        self.queryHelper.check_bucket_analysis_result(init_analysis)
        self.queryHelper.execute_query(check_query)
        self.queryHelper.controls.query_bucket_analysis().refresh_button.click(highlight=False)
        time.sleep(6)
        self.queryHelper.check_bucket_analysis_result(expected_analysis)

    def test_save_query(self):
        path = self.input.param('path', "n1ql_query.txt")
        check_query = self.input.param('check_query', '').replace('_STAR_', '*').replace('_SEM_', ';').decode(
            'unicode_escape')
        home = expanduser("~")
        home += "\Downloads"
        saved_file = "%s\%s" % (home, path)
        try:
            if path:
                os.remove(saved_file)
            else:
                os.remove("%s\download.txt" % home)
        except OSError:
            pass
        NavigationHelper(self).navigate('Query')
        self.queryHelper.execute_query(check_query)
        self.queryHelper.save_query(path)
        if not path:
            saved_file = "%s\download.txt" % home
        f = open(saved_file, 'r')
        content = f.read()
        self.assertEqual(content, check_query, msg='Incorrect saved query file: %s' % content)

    def test_save_result(self):
        path = self.input.param('path', "data.json")
        check_query = self.input.param('check_query', '').replace('_STAR_', '*').replace('_SEM_', ';').decode(
            'unicode_escape')
        expected_result = self.input.param('expected_result', '{"no_data_yet": "hit execute to run query"}')
        expected_result = expected_result.replace('_STAR_', '*').replace('_SEM_', ';').decode('unicode_escape')
        home = expanduser("~")
        home += "\Downloads"
        saved_file = "%s\%s" % (home, path)
        try:
            if path:
                os.remove(saved_file)
            else:
                os.remove("%s\download" % home)
        except OSError:
            pass
        NavigationHelper(self).navigate('Query')
        if check_query:
            self.queryHelper.execute_query(check_query)
        self.queryHelper.save_result(path)
        if not path:
            saved_file = "%s\download" % home
        f = open(saved_file, 'r')
        content = f.read()
        search_obj = re.search(expected_result, content, re.M | re.I)
        self.assertTrue(search_obj, msg='Incorrect saved query result file: %s' % content)


class QueryWorkbenchHelper(TestCase):
    def __init__(self, tc):
        self.tc = tc
        self.wait = WebDriverWait(tc.driver, timeout=250)
        self.controls = QueryControls(tc.driver)

    def execute_query(self, query):
        self.wait.until_not(lambda fn:
                            self.controls.query_top_screen().query_editor.is_displayed(),
                            "Query Editor not displayed")
        self.controls.query_top_screen().query_editor.type(query)
        self.controls.query_top_screen().execute_button.click()
        self.controls.query_results_box()
        self.controls.query_results_box().result_json_mode.get_text()
        self.wait.until(lambda fn:
                        self.controls.query_top_screen().execute_button.is_displayed(),
                        "Query is still running?")

    def check_result(self, expected_results, mode='JSON'):
        self.select_result_mode(mode)
        if mode == 'JSON':
            result = self.controls.query_results_box().result_json_mode.get_text()
        elif mode == 'Table':
            result = self.controls.query_results_box().result_table_mode.get_text()
        elif mode == 'Tree':
            result = self.controls.query_results_box().result_tree_mode.get_text()
        elif mode == 'Plan':
            result = self.controls.query_results_box().result_plan_mode.get_text()
        elif mode == 'Plan Text':
            result = self.controls.query_results_box().result_plan_text_mode.get_text()
        search_obj = None
        for expected_result in expected_results:
            search_obj = re.search(expected_result, result, re.M | re.I)
            if search_obj:
                break
        self.assertTrue(search_obj, msg='Incorrect query result: %s' % result)

    def check_summary_result(self, expected_result, mode='JSON'):
        self.select_result_mode(mode)
        result = self.controls.query_results_box().result_summary.get_text()
        search_obj = re.search(expected_result, result, re.M | re.I)
        self.assertTrue(search_obj, msg='Incorrect query summary result: %s' % result)

    def select_result_mode(self, mode='JSON'):
        selected = self.controls.query_results_box().result_selected_mode.get_text()
        if selected != mode:
            if mode == 'JSON':
                self.controls.query_results_box().result_select_json_mode.click()
            elif mode == 'Table':
                self.controls.query_results_box().result_select_table_mode.click()
            elif mode == 'Tree':
                self.controls.query_results_box().result_select_tree_mode.click()
            elif mode == 'Plan':
                self.controls.query_results_box().result_select_plan_mode.click()
            elif mode == 'Plan Text':
                self.controls.query_results_box().result_select_plan_text_mode.click()

    def check_bucket_analysis_result(self, expected_results):
        result = self.controls.query_bucket_analysis().sidebar_body.get_text()
        search_obj = None
        for expected_result in expected_results:
            search_obj = re.search(expected_result, result, re.M | re.I)
            if search_obj:
                break
        self.assertTrue(search_obj, msg='Incorrect bucket analysis result: %s' % result)

    def save_query(self, path):
        self.wait.until(lambda fn:
                        self.controls.query_top_screen().save_query.is_displayed(),
                        "Save button is not available")
        time.sleep(5)
        self.controls.query_top_screen().save_query.click()
        self.wait.until(lambda fn:
                        self.controls.save_dialog().path.is_displayed(),
                        "Save dialog is not displayed")
        self.controls.save_dialog().path.type_native(path)
        self.controls.save_dialog().ok_button.click()
        time.sleep(3)

    def save_result(self, path):
        self.wait.until(lambda fn:
                        self.controls.query_results_box().result_save_button.is_displayed(),
                        "Save button is not available")
        time.sleep(3)
        self.controls.query_results_box().result_save_button.click()
        self.wait.until(lambda fn:
                        self.controls.save_dialog().path.is_displayed(),
                        "Save dialog is not displayed")
        self.controls.save_dialog().path.type_native(path)
        self.controls.save_dialog().ok_button.click()
        time.sleep(3)


class QueryControls:
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)

    def query_top_screen(self):
        self.execute_button = self.helper.find_control('query_top_screen', 'execute_button')
        self.view_previous = self.helper.find_control('query_top_screen', 'view_previous')
        self.view_next = self.helper.find_control('query_top_screen', 'view_next')
        self.history_link = self.helper.find_control('query_top_screen', 'history_link')
        self.page_count_label = self.helper.find_control('query_top_screen', 'page_count_label')
        self.query_editor = self.helper.find_control('query_top_screen', 'query_editor')
        self.query_editor_value = self.helper.find_control('query_top_screen', 'query_editor_value')
        self.save_query = self.helper.find_control('query_top_screen', 'save_query')
        return self

    def query_bucket_analysis(self):
        self.refresh_button = self.helper.find_control('query_bucket_analysis', 'refresh_button')
        self.resize_button = self.helper.find_control('query_bucket_analysis', 'resize_button')
        self.sidebar_body = self.helper.find_control('query_bucket_analysis', 'sidebar_body')
        return self

    def query_results_box(self):
        self.result_box = self.helper.find_control('query_results_box', 'result_box')
        self.result_select_json_mode = self.helper.find_control('query_results_box', 'result_select_mode', text="JSON")
        self.result_select_table_mode = self.helper.find_control('query_results_box', 'result_select_mode',
                                                                 text="Table")
        self.result_select_tree_mode = self.helper.find_control('query_results_box', 'result_select_mode', text="Tree")
        self.result_select_plan_mode = self.helper.find_control('query_results_box', 'result_select_mode', text="Plan")
        self.result_select_plan_text_mode = self.helper.find_control('query_results_box', 'result_select_mode',
                                                                     text="Plan Text")
        self.result_selected_mode = self.helper.find_control('query_results_box', 'result_selected_mode')
        self.result_summary = self.helper.find_control('query_results_box', 'result_summary')
        self.result_json_mode = self.helper.find_control('query_results_box', 'result_json_mode')
        self.result_table_mode = self.helper.find_control('query_results_box', 'result_table_mode')
        self.result_tree_mode = self.helper.find_control('query_results_box', 'result_tree_mode')
        self.result_plan_mode = self.helper.find_control('query_results_box', 'result_plan_mode')
        self.result_plan_text_mode = self.helper.find_control('query_results_box', 'result_plan_text_mode')
        self.result_box = self.helper.find_control('query_results_box', 'result_box')
        self.result_box = self.helper.find_control('query_results_box', 'result_box')
        self.result_save_button = self.helper.find_control('query_results_box', 'result_save_button')
        return self

    def save_dialog(self):
        self.path = self.helper.find_control('query_save_screen', 'path')
        self.cancel_button = self.helper.find_control('query_save_screen', 'cancel_button')
        self.ok_button = self.helper.find_control('query_save_screen', 'ok_button')
        return self
