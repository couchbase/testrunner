import ConfigParser
import commands
import datetime
import os
import time
import types
import unittest
from threading import Thread

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select

import logger
from TestInput import TestInputSingleton
from membase.api.rest_client import RestConnection
from membase.helper.bucket_helper import BucketOperationHelper
from membase.helper.cluster_helper import ClusterOperationHelper
from remote.remote_util import RemoteMachineShellConnection

"""
*** IMPORTANT! NEED TO READ BEFORE RUN UI TEST ***
- Server that is used as host UI slave must be in uiconf session (ask IT to get it)
ini file format must follow format below [uiconf]
- Jenkins slave must install python selenium as we import selenium above

#### ini file start here
[global]
username:xxxx
password:xxxx
#ssh_key=/home/xxxx
port:8091

[servers]
1:xxx.xxx.xxx.xxx
2:xxx.xx.xxx.xx

[membase]
rest_username:Administrator
rest_password:xxxxxxx

[uiconf]
browser:chrome
chrome_path:path_to_chrome_driver
selenium_path:path_to_selenium_standalone_server
selenium_ip:UI_slave_IP
selenium_port:4444
selenium_user:username_used_to_login_to_UI_slave
selenium_password:password_used_to_login_to_UI_slave
screenshots:logs/screens

### ini file end here

"""

class BaseUITestCase(unittest.TestCase):
    # selenium thread
    def _start_selenium(self):
        host = self.machine.ip
        if host in ['localhost', '127.0.0.1']:
             os.system("java -jar %sselenium-server-standalone*.jar "
                       "-Dwebdriver.chrome.driver=%s > selenium.log 2>&1"
                                 % (self.input.ui_conf['selenium_path'],\
                                      self.input.ui_conf['chrome_path']))
        else:
            """ go to remote server with better video driver to display browser """
            self.shell.execute_command('{0}start-selenium.bat > {0}selenium.log 2>&1 &'\
                                           .format(self.input.ui_conf['selenium_path']))

    def _kill_old_drivers(self):
        if self.shell.extract_remote_info().type.lower() == 'windows':
            self.shell.execute_command('taskkill /F /IM chromedriver.exe')
            self.shell.execute_command('taskkill /F /IM chrome.exe')

    def _wait_for_selenium_is_started(self, timeout=10):
        if self.machine.ip in ['localhost', '127.0.0.1']:
            start_time = time.time()
            while (time.time() - start_time) < timeout:
                log = open("/tmp/selenium.log")
                if log.read().find('Started org.openqa.jetty.jetty.Server') > -1:
                    log.close()
                    if self._is_selenium_running():
                        time.sleep(1)
                        return
                time.sleep(1)
        else:
            time.sleep(timeout)

    def _start_selenium_thread(self):
        self.t = Thread(target=self._start_selenium,
                       name="selenium",
                       args=())
        self.t.start()

    def _is_selenium_running(self):
        self.log.info("check if selenium is running")
        host = self.machine.ip
        if host in ['localhost', '127.0.0.1']:
            cmd = 'ps -ef|grep selenium-server'
            output = commands.getstatusoutput(cmd)
            if str(output).find('selenium-server-standalone') > -1:
                self.log.info("selenium is running")
                return True
        else:
            """need to add code to go either windows or linux """
            #cmd = "ssh {0}@{1} 'bash -s' < 'tasklist |grep selenium-server'"
            #.format(self.input.servers[0].ssh_username,
            #                                                           host)
            cmd = "tasklist | grep java"
            o, r = self.shell.execute_command(cmd)
            #cmd = "ssh {0}@{1} 'bash -s' < 'ps -ef|grep selenium-server'"
            if str(o).find('java') > -1:
                self.log.info("selenium is running")
                return True
        return False

    def setUp(self):
        try:
            self.log = logger.Logger.get_logger()
            self.input = TestInputSingleton.input
            self.servers = self.input.servers
            self.browser = self.input.ui_conf['browser']
            self.replica  = self.input.param("replica", 1)
            self.case_number = self.input.param("case_number", 0)
            self.machine = self.input.ui_conf['server']
            self.driver = None
            self.shell = RemoteMachineShellConnection(self.machine)
            #avoid clean up if the previous test has been tear down
            if not self.input.param("skip_cleanup", True) \
                                            or self.case_number == 1:
                self.tearDown()
            self._log_start(self)
            self._kill_old_drivers()
            #thread for selenium server
            if not self._is_selenium_running():
                self.log.info('start selenium')
                self._start_selenium_thread()
                self._wait_for_selenium_is_started()
            self.log.info('start selenium session')
            if self.browser == 'firefox':
                self.log.info("Test Couchbase Server UI in Firefox")
                self.driver = webdriver.Remote(command_executor='http://{0}:{1}/wd/hub'
                                                               .format(self.machine.ip,
                                                                    self.machine.port),
                                     desired_capabilities=DesiredCapabilities.FIREFOX)
            elif self.browser == 'chrome':
                self.log.info("Test Couchbase Server UI in Chrome")
                self.driver = webdriver.Remote(command_executor='http://{0}:{1}/wd/hub'
                                                               .format(self.machine.ip,
                                                                    self.machine.port),
                                      desired_capabilities=DesiredCapabilities.CHROME)
            """ need to add support test on Internet Explorer """

            self.log.info('*** selenium started ***')
            self.driver.get("http://" + self.servers[0].ip + ":8091")
            self.username = self.input.membase_settings.rest_username
            self.password = self.input.membase_settings.rest_password
            self.driver.maximize_window()
        except Exception as ex:
            self.input.test_params["stop-on-failure"] = True
            self.log.error("SETUP WAS FAILED. ALL TESTS WILL BE SKIPPED")
            self.fail(ex)

    @staticmethod
    def _log_start(self):
        try:
            msg = "{0} : {1} started ".format(datetime.datetime.now(),
                                              self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    @staticmethod
    def _log_finish(self):
        try:
            msg = "{0} : {1} finished ".format(datetime.datetime.now(),
                                               self._testMethodName)
            RestConnection(self.servers[0]).log_client_error(msg)
        except:
            pass

    def tearDown(self):
        try:
            if self.driver:
                path_screen = self.input.ui_conf['screenshots'] or 'logs/screens'
                full_path = '{1}/screen_{0}.png'.format(time.time(), path_screen)
                self.log.info('screenshot is available: %s' % full_path)
                if not os.path.exists(path_screen):
                    os.mkdir(path_screen)
                self.driver.get_screenshot_as_file(os.path.abspath(full_path))
            rest = RestConnection(self.servers[0])
            if rest._rebalance_progress_status() == 'running':
                stopped = rest.stop_rebalance()
                self.assertTrue(stopped, msg="unable to stop rebalance")
            BucketOperationHelper.delete_all_buckets_or_assert(self.servers, self)
            for server in self.servers:
                ClusterOperationHelper.cleanup_cluster([server])
            ClusterOperationHelper.wait_for_ns_servers_or_assert(self.servers, self)
            if self.driver:
                self.driver.close()
        except Exception as e:
            raise e
        finally:
            if self.driver:
                self.shell.disconnect()

    def sleep(self, timeout=1, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

class Control():
    def __init__(self, selenium, by=None, web_element=None):
        self.selenium = selenium
        self.by = by
        if by:
            try:
                self.web_element = self.selenium.find_element_by_xpath(by)
                self.present = True
                if self.web_element is None:
                    self.present = False
            except NoSuchElementException as ex:
                self.present = False
        else:
            self.web_element = web_element
            self.present = True

    def highlightElement(self):
        if self.by:
            self.selenium.execute_script("document.evaluate(\"{0}\",document,null,\
                           XPathResult.ANY_TYPE, null).iterateNext().setAttribute(\
                                              'style','background-color:yellow');"\
                                                                  .format(self.by))

    def type_native(self, text):
        ActionChains(self.selenium).click(self.web_element).perform()
        ActionChains(self.selenium).key_down(Keys.CONTROL).perform()
        ActionChains(self.selenium).send_keys('a').perform()
        ActionChains(self.selenium).key_up(Keys.CONTROL).perform()
        ActionChains(self.selenium).send_keys(Keys.DELETE).perform()
        ActionChains(self.selenium).send_keys(text).perform()

    def click(self, highlight=True):
        if highlight:
            self.highlightElement()
        self.web_element.click()

    def click_native(self):
        ActionChains(self.selenium).move_to_element(self.web_element).perform()
        ActionChains(self.selenium).click(self.web_element).perform()

    def click_with_mouse_over(self):
        ActionChains(self.selenium).move_to_element(self.web_element).perform()
        ActionChains(self.selenium).click(self.web_element).perform()
        ActionChains(self.selenium).key_down(Keys.ENTER).perform()
        ActionChains(self.selenium).key_up(Keys.ENTER).perform()

    def type(self, message):
        if message:
            self.highlightElement()
            self.web_element.clear()
            if type(message) == types.StringType and message.find('\\') > -1:
                for symb in list(message):
                    if symb == '\\':
                        self.web_element.send_keys(Keys.DIVIDE)
                    else:
                        self.web_element.send_keys(symb)
            else:
                self.web_element.send_keys(message)

    def check(self, setTrue=True):
        if setTrue:
            if not self.is_checked():
                self.click()
        else:
            if self.is_checked():
                self.click()

    def is_present(self):
        return self.present

    def is_displayed(self):
        return self.present and self.web_element.is_displayed()

    def is_checked(self):
        checked = self.web_element.get_attribute("checked")
        return checked is not None

    def get_text(self):
        self.highlightElement()
        return self.web_element.text

    def get_attribute(self, atr):
        return self.web_element.get_attribute(atr)

    def select(self, label):
        element = Select(self.web_element)
        element.select_by_visible_text(label)

    def mouse_over(self):
        ActionChains(self.selenium).move_to_element(self.web_element).perform()

    def get_inner_html(self):
        return self.web_element.get_attribute("outerHTML")

class ControlsHelper():
    def __init__(self, driver):
        self.driver = driver
        file = "pytests/ui/uilocators-watson.conf"
        config = ConfigParser.ConfigParser()
        config.read(file)
        self.locators = config

    def find_control(self, section, locator, parent_locator=None, text=None):
        by = self._find_by(section, locator, parent_locator)
        if text:
           by = by.format(text)
        return Control(self.driver, by=by)

    def find_controls(self, section, locator, parent_locator=None):
        by = self._find_by(section, locator, parent_locator)
        controls = []
        elements = self.driver.find_elements_by_xpath(by)
        for element in elements:
            controls.append(Control(self.driver, web_element=element))
        return controls

    def find_first_visible(self, section, locator, parent_locator=None, text=None):
        by = self._find_by(section, locator, parent_locator)
        if text:
            by = by.format(text)
        controls = []
        elements = self.driver.find_elements_by_xpath(by)
        for element in elements:
            try:
                if element.is_displayed():
                    return Control(self.driver, web_element=element)
            except StaleElementReferenceException:
                pass
        return None

    def _find_by(self, section, locator, parent_locator=None):
        if parent_locator:
            return self.locators.get(section, parent_locator) + \
                    self.locators.get(section, locator)
        else:
            return self.locators.get(section, locator)

class BaseHelperControls():
     def __init__(self, driver):
        helper = ControlsHelper(driver)
        self._user_field = helper.find_control('login', 'user_field')
        self._user_password = helper.find_control('login', 'password_field')
        self._login_btn = helper.find_control('login', 'login_btn')
        self._logout_btn = helper.find_control('login', 'logout_btn')
        self.error = helper.find_control('login', 'error')

class BaseHelper():
    def __init__(self, tc):
        self.tc = tc
        self.controls = BaseHelperControls(tc.driver)

    def login(self, user=None, password=None):
        self.tc.log.info("Try to login to Couchbase Server in browser")
        if not user:
            user =  self.tc.input.membase_settings.rest_username
        if not password:
            password = self.tc.input.membase_settings.rest_password
        self.controls._user_field.type(user)
        self.controls._user_password.type(password)
        self.controls._login_btn.click()
        self.tc.log.info("user %s is logged in" % user)

    def logout(self):
        self.tc.log.info("Try to logout")
        self.controls._logout_btn.click()
        time.sleep(3)
        self.tc.log.info("You are logged out")
