import logger
import time
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from couchbase_helper.cluster import Cluster
from membase.api.rest_client import RestConnection
from uibasetest import *
from uisampletests import Bucket, NavigationHelper, BucketHelper
from selenium.common.exceptions import StaleElementReferenceException
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper


class FTSTests(BaseUITestCase):
    def setUp(self):
        super(FTSTests, self).setUp()
        self.bucketname = 'beer'
        self.baseHelper = BaseHelper(self)
        self.ftsHelper = FTSHelper(self)
        self.baseHelper.login()
        self.baseHelper.loadSampleBucket(self.servers[0], self.bucketname)

    def tearDown(self):
        super(FTSTests, self).tearDown()

    def test_create_simple_fts_index(self):
        indexName = "FTSIndex1"
        NavigationHelper(self).navigate('Indexes')
        self.ftsHelper.click_fts_tab(text="Full Text")
        self.assertTrue(self.ftsHelper.create_new_fts_index(indexName=indexName, bucketName=self.bucketname),
                        "Failed to create FTS Index")
        NavigationHelper(self).navigate('Indexes')
        self.ftsHelper.click_fts_tab(text="Full Text")
        self.assertTrue(self.ftsHelper.is_fts_index_displayed_in_list(indexName=indexName),
                        "Index not listed in the FTS Index list")


class FTSHelper():
    def __init__(self, tc):
        self.tc = tc
        self.wait = WebDriverWait(tc.driver, timeout=250)
        self.controls = FTSControls(tc.driver)

    def click_fts_tab(self, text=''):
        self.controls.fts_screen(text=text).fts_tab.click()
        self.tc.log.info("tab '%s' is selected" % text)
        self.wait.until(lambda fn:
                        self.controls.fts_index_btn().create_new_fts_index_btn.is_displayed(),
                        "Create FTS Index button not displayed")

    def create_new_fts_index(self, indexName, bucketName):
        self.wait.until(lambda fn:
                        self.controls.fts_index_btn().create_new_fts_index_btn.is_displayed(),
                        "Create FTS Index button not displayed")
        self.tc.log.info('Trying create a new fts index')
        self.controls.fts_index_btn().create_new_fts_index_btn.click()
        self.wait.until(lambda fn:
                        self.controls.create_fts_index_screen().index_name.is_displayed(),
                        "Create FTS Index screen is not opened")
        self.controls.create_fts_index_screen().index_name.type(indexName)
        self.controls.create_fts_index_screen().bucket_list.select(bucketName)
        self.controls.create_fts_index_screen().create_index_btn.click()
        time.sleep(10)
        self.wait.until(lambda fn:
                        self.controls.fts_index_details_screen(indexname=indexName).index_name_header.is_displayed(),
                        "FTS Index Details screen is not opened")
        return self.controls.fts_index_details_screen(indexname=indexName).index_name_header.is_displayed()

    def is_fts_index_displayed_in_list(self, indexName):
        self.wait.until(lambda fn:
                        self.controls.fts_screen_index_row(indexName).fts_index_name_in_row.is_displayed(),
                        indexName + " is not listed in list of FTS indexes")
        return self.controls.fts_screen_index_row(indexName).fts_index_name_in_row.is_displayed()


class FTSControls():
    def __init__(self, driver):
        self.helper = ControlsHelper(driver)

    def fts_screen(self, text=''):
        self.fts_tab = self.helper.find_control('fts_screen', 'fts_tab', text=text)
        return self

    def fts_index_btn(self):
        self.create_new_fts_index_btn = self.helper.find_control('fts_screen', 'create_new_fts_index_btn')
        return self

    def create_fts_index_screen(self):
        self.index_name = self.helper.find_control('fts_create_new_index', 'index_name')
        self.bucket_list = self.helper.find_control('fts_create_new_index', 'bucket_list')
        self.create_index_btn = self.helper.find_control('fts_create_new_index', 'create_index_btn')
        self.cancel_create_index_btn = self.helper.find_control('fts_create_new_index', 'cancel_create_index_btn')
        return self

    def fts_index_details_screen(self, indexname=''):
        self.index_name_header = self.helper.find_control('fts_index_details_screen', 'index_name_header',
                                                          text=indexname)
        self.show_index_defn_json_checkbox = self.helper.find_control('fts_index_details_screen',
                                                                      'show_index_defn_json_checkbox')
        return self

    def fts_screen_index_row(self, indexname=''):
        self.fts_index_name_in_row = self.helper.find_control('fts_screen_index_row', 'fts_index_name_in_row',
                                                              text=indexname)
        return self
