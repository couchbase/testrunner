#!/usr/bin/env python3

import base64
import gzip
from http.client import BadStatusLine
import os
import urllib.request, urllib.error, urllib.parse
import sys
import threading
from os.path import basename, splitext
from multiprocessing import Process
from pprint import pprint
sys.path = ["lib", "pytests", "pysystests"] + sys.path

if sys.hexversion < 0x30706f0:
    sys.exit("Testrunner requires version 3.7.6+ of python (found: " + sys.version + ")")

import re
import time
import unittest
import logging.config
from threading import Thread, Event
from xunit import XUnitTestResult
from TestInput import TestInputParser, TestInputSingleton
from optparse import OptionParser, OptionGroup
from scripts.collect_server_info import cbcollectRunner, couch_dbinfo_Runner
from scripts.measure_sched_delays import SchedDelays
from scripts.getcoredumps import Getcoredumps, Clearcoredumps
import signal
import shutil
import glob
import xml.dom.minidom
import logging
from remote.remote_util import RemoteMachineShellConnection



log = logging.getLogger(__name__)
logging.info(__name__)
print("*** TestRunner ***")

def usage(err=None):
    print("""\
Syntax: testrunner [options]

Examples:
  ./testrunner -i tmp/local.ini -t performance.perf.DiskDrainRate
  ./testrunner -i tmp/local.ini -t performance.perf.DiskDrainRate.test_9M
""")
    sys.exit(0)


def parse_args(argv):

    parser = OptionParser()

    parser.add_option("-q", action="store_false", dest="verbose")

    tgroup = OptionGroup(parser, "TestCase/Runlist Options")
    tgroup.add_option("-i", "--ini", dest="ini",
                      help="Path to .ini file containing server information,e.g -i tmp/local.ini")
    tgroup.add_option("-c", "--config", dest="conf",
                      help="Config file name (located in the conf subdirectory), "
                           "e.g -c py-view.conf")
    tgroup.add_option("-t", "--test", dest="testcase",
                      help="Test name (multiple -t options add more tests) e.g -t "
                           "performance.perf.DiskDrainRate")
    tgroup.add_option("-d", "--include_tests", dest="include_tests",
                      help="Value can be 'failed' (or) 'passed' (or) 'failed=<junit_xml_path (or) "
                           "jenkins_build_url>' (or) 'passed=<junit_xml_path or "
                           "jenkins_build_url>' (or) 'file=<filename>' (or) '<regular "
                           "expression>' to include tests in the run. Use -g option to search "
                           "entire conf files. e.g. -d 'failed' or -d 'failed=report.xml' or -d "
                           "'^gsi.*nodes_init=2.*'")
    tgroup.add_option("-e", "--exclude_tests", dest="exclude_tests",
                      help="Value can be 'failed' (or) 'passed' (or) 'failed=<junit_xml_path (or) "
                           "jenkins_build_url>' (or) 'passed=<junit_xml_path (or) "
                           "jenkins_build_url>' or 'file=<filename>' (or) '<regular expression>' "
                           "to exclude tests in the run. Use -g option to search entire conf "
                           "files. e.g. -e 'passed'")
    tgroup.add_option("-r", "--rerun", dest="rerun",
                      help="Rerun fail or pass tests with given =count number of times maximum. "
                           "\ne.g. -r 'fail=3'")
    tgroup.add_option("-g", "--globalsearch", dest="globalsearch",
                      help="Option to get tests from given conf file path pattern, "
                           "like conf/**/*.conf. Useful for include or exclude conf files to "
                           "filter tests. e.g. -g 'conf/**/.conf'",
                      default="")
    tgroup.add_option("-m", "--merge", dest="merge",
                      help="Merge the report files path pattern, like logs/**/.xml. e.g.  -m '["
                           "logs/**/*.xml]'",
                      default="")
    parser.add_option_group(tgroup)

    parser.add_option("-p", "--params", dest="params",
                      help="Optional key=value parameters, comma-separated -p k=v,k2=v2,...",
                      default="")
    parser.add_option("-n", "--noop", action="store_true",
                      help="NO-OP - emit test names, but don't actually run them e.g -n true")
    parser.add_option("-l", "--log-level", dest="loglevel", default="INFO",
                      help="e.g -l info,warning,error")
    options, args = parser.parse_args()

    tests = []
    test_params = {}

    setLogLevel(options.loglevel)
    log.info("Checking arguments...")
    if not options.ini:
        parser.error("Please specify an .ini file (-i) option.")
        parser.print_help()
    else:
        test_params['ini'] = options.ini
        if not os.path.exists(options.ini):
            sys.exit("ini file {0} was not found".format(options.ini))

    test_params['cluster_name'] = splitext(os.path.basename(options.ini))[0]

    if not options.testcase and not options.conf and not options.globalsearch and not options.include_tests and not options.exclude_tests:
        parser.error("Please specify a configuration file (-c) or a test case (-t) or a globalsearch (-g) option.")
        parser.print_help()
    if options.conf and not options.globalsearch:
        parse_conf_file(options.conf, tests, test_params)
    if options.globalsearch:
        parse_global_conf_file(options.globalsearch, tests, test_params)
    try:
        if options.include_tests:
            tests = process_include_or_filter_exclude_tests("include", options.include_tests, tests,
                                                            options)
        if options.exclude_tests:
            tests = process_include_or_filter_exclude_tests("exclude", options.exclude_tests, tests, options)
    except Exception as e:
        log.error("Failed to get the test xml to include or exclude the tests. Running all the tests instead.")
        log.error(e)
    if options.testcase:
        tests.append(options.testcase)
    if options.noop:
        print(("---\n"+"\n".join(tests)+"\n---\nTotal="+str(len(tests))))
        sys.exit(0)

    return tests, test_params, options.ini, options.params, options

def setLogLevel(log_level):
    if log_level and log_level.lower() == 'info':
        log.setLevel(logging.INFO)
    elif log_level and log_level.lower() == 'warning':
        log.setLevel(logging.WARNING)
    elif log_level and log_level.lower() == 'debug':
        log.setLevel(logging.DEBUG)
    elif log_level and log_level.lower() == 'critical':
        log.setLevel(logging.CRITICAL)
    elif log_level and log_level.lower() == 'fatal':
        log.setLevel(logging.FATAL)
    else:
        log.setLevel(logging.NOTSET)

def process_include_or_filter_exclude_tests(filtertype, option, tests, options):
    if filtertype == 'include' or filtertype == 'exclude':

        if option.startswith('failed') or option.startswith('passed') or option.startswith("http://") or option.startswith("https://"):
            passfail = option.split("=")
            tests_list = []
            if len(passfail) == 2:
                if passfail[1].startswith("http://") or passfail[1].startswith("https://"):
                    tp, tf = parse_testreport_result_xml(passfail[1])
                else:
                    tp, tf = parse_junit_result_xml(passfail[1])
                    if not tp and not tf:
                        tp, tf = parse_testreport_result_xml(passfail[1])
            elif option.startswith("http://") or option.startswith("https://"):
                tp, tf = parse_testreport_result_xml(option)
                tests_list=tp+tf
            else:
                tp, tf = parse_junit_result_xml()
                if not tp and not tf:
                    tp, tf = parse_testreport_result_xml()
            if tp is None and tf is None:
                return tests
            if option.startswith('failed') and tf:
                tests_list = tf
            elif option.startswith('passed') and tp:
                tests_list = tp
            if filtertype == 'include':
                tests = tests_list
            else:
                for line in tests_list:
                    isexisted, t = check_if_exists_with_params(tests, line, options.params)
                    if isexisted:
                        tests.remove(t)
        elif option.startswith("file="):
            filterfile = locate_conf_file(option.split("=")[1])
            if filtertype == 'include':
                tests_list = []
                if filterfile:
                    for line in filterfile:
                        tests_list.append(line.strip())
                tests = tests_list
            else:
                for line in filterfile:
                    isexisted, t = check_if_exists_with_params(tests, line.strip(), options.params)
                    if isexisted:
                        tests.remove(t)
        else:  # pattern
            if filtertype == 'include':
                tests = [i for i in tests if re.search(option, i)]
            else:
                tests = [i for i in tests if not re.search(option, i)]

    else:
        log.warning("Warning: unknown filtertype given (only include/exclude supported)!")

    return tests

def create_log_file(log_config_file_name, log_file_name, level):
    tmpl_log_file = open("logging.conf.sample")
    log_file = open(log_config_file_name, "w")
    log_file.truncate()
    for line in tmpl_log_file:
        newline = line.replace("@@LEVEL@@", level)
        newline = newline.replace("@@FILENAME@@", log_file_name.replace('\\', '/'))
        log_file.write(newline)
    log_file.close()
    tmpl_log_file.close()


def append_test(tests, name):
    prefix = ".".join(name.split(".")[0:-1])
    """
        Some tests carry special chars, need to skip it
    """
    if "test_restore_with_filter_regex" not in name and \
        "test_restore_with_rbac" not in name and \
        "test_backup_with_rbac" not in name and \
        "test_add_node_with_cert_diff_services" not in name and \
        "test_add_nodes_x509_rebalance" not in name and \
        "test_init_nodes_x509" not in name and \
         name.find('*') > 0:
        for t in unittest.TestLoader().loadTestsFromName(name.rstrip('.*')):
            tests.append(prefix + '.' + t._testMethodName)
    else:
        tests.append(name)


def locate_conf_file(filename):
    log.info("Conf filename: %s" % filename)
    if filename:
        if os.path.exists(filename):
            return open(filename)
        if os.path.exists("conf{0}{1}".format(os.sep, filename)):
            return open("conf{0}{1}".format(os.sep, filename))
        return None


def parse_conf_file(filename, tests, params):
    """Parse a configuration file.
    Configuration files contain information and parameters about test execution.
    Should follow the following order:

    Part1: Tests to execute.
    Part2: Parameters to override the defaults.

    @e.x:
        TestModuleName1:
            TestName1
            TestName2
            ....
        TestModuleName2.TestName3
        TestModuleName2.TestName4
        ...

        params:
            items=4000000
            num_creates=400000
            ....
    """

    f = locate_conf_file(filename)
    if not f:
        usage("unable to locate configuration file: " + filename)
    prefix = None
    for line in f:
        stripped = line.strip()
        if stripped.startswith("#") or len(stripped) <= 0:
            continue
        if stripped.endswith(":"):
            prefix = stripped.split(":")[0]
            log.info("Test prefix: {0}".format(prefix))
            continue
        name = stripped
        if prefix and prefix.lower() == "params":
            args = stripped.split("=", 1)
            if len(args) == 2:
                params[args[0]] = args[1]
            continue
        elif line.startswith(" ") and prefix:
            name = prefix + "." + name
        prefix = ".".join(name.split(",")[0].split('.')[0:-1])
        append_test(tests, name)

    # If spec parameter isn't defined, testrunner uses the *.conf filename for
    # the spec value
    if 'spec' not in params:
        params['spec'] = splitext(basename(filename))[0]

    params['conf_file'] = filename

def parse_global_conf_file(dirpath, tests, params):
    log.info("dirpath="+dirpath)
    if os.path.isdir(dirpath):
        dirpath=dirpath+os.sep+"**"+os.sep+"*.conf"
        log.info("Global filespath=" + dirpath)

    conf_files = glob.glob(dirpath)
    for file in conf_files:
        parse_conf_file(file, tests, params)

def check_if_exists(test_list, test_line):
    new_test_line = ''.join(sorted(test_line))
    for t in test_list:
        t1 = ''.join(sorted(t))
        if t1 == new_test_line:
            return True, t
    return False, ""

def check_if_exists_with_params(test_list, test_line, test_params):
    new_test_line = ''.join(sorted(test_line))
    for t in test_list:
        if test_params:
            t1 = ''.join(sorted(t+","+test_params.strip()))
        else:
            t1 = ''.join(sorted(t))

        if t1 == new_test_line:
            return True, t
    return False, ""

def transform_and_write_to_file(tests_list, filename):
    new_test_list = []
    for test in tests_list:
        line = filter_fields(test)
        line = line.rstrip(",")
        isexisted, _ = check_if_exists(new_test_list, line)
        if not isexisted:
            new_test_list.append(line)

    file = open(filename, "w+")
    for line in new_test_list:
        file.writelines((line) + "\n")
    file.close()
    return new_test_list

def getNodeText(nodelist):
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)

def parse_testreport_result_xml(filepath=""):
    if filepath.startswith("http://") or filepath.startswith("https://"):
        if filepath.endswith(".xml"):
            url_path = filepath
        else:
            url_path = filepath+"/testReport/api/xml?pretty=true"
        jobnamebuild = filepath.split('/')
        if not os.path.exists('logs'):
            os.mkdir('logs')
        newfilepath = 'logs'+''.join(os.sep)+'_'.join(jobnamebuild[-3:])+"_testresult.xml"
        log.info("Downloading " + url_path +" to "+newfilepath)
        try:
            filedata = urllib.request.urlopen(url_path)
            datatowrite = filedata.read()
            filepath = newfilepath
            with open(filepath, 'wb') as f:
                f.write(datatowrite)
        except Exception as ex:
            log.error("Error:: "+str(ex)+"! Please check if " +
                      url_path + " URL is accessible!!")
            log.info("Running all the tests instead for now.")
            return None, None
    if filepath == "":
        filepath = "logs/**/*.xml"
    log.info("Loading result data from "+filepath)
    xml_files = glob.glob(filepath)
    passed_tests=[]
    failed_tests=[]
    for xml_file in xml_files:
        log.info("-- "+xml_file+" --")
        doc = xml.dom.minidom.parse(xml_file)
        testresultelem = doc.getElementsByTagName("testResult")
        testsuitelem = testresultelem[0].getElementsByTagName("suite")
        for ts in testsuitelem:
            testcaseelem = ts.getElementsByTagName("case")
            for tc in testcaseelem:
                tcname = getNodeText((tc.getElementsByTagName("name")[0]).childNodes)
                tcstatus = getNodeText((tc.getElementsByTagName("status")[0]).childNodes)
                if tcstatus == 'PASSED':
                    failed=False
                    passed_tests.append(tcname)
                else:
                    failed=True
                    failed_tests.append(tcname)

    if failed_tests:
        failed_tests = transform_and_write_to_file(failed_tests,"failed_tests.conf")

    if passed_tests:
        passed_tests = transform_and_write_to_file(passed_tests, "passed_tests.conf")

    return passed_tests, failed_tests

def parse_junit_result_xml(filepath=""):
    if filepath.startswith("http://") or filepath.startswith("https://"):
        return parse_testreport_result_xml(filepath)
    if filepath == "":
        filepath = "logs/**/*.xml"
    log.info("Loading result data from "+filepath)
    xml_files = glob.glob(filepath)
    passed_tests=[]
    failed_tests=[]
    for xml_file in xml_files:
        log.info("-- "+xml_file+" --")
        doc = xml.dom.minidom.parse(xml_file)
        testsuitelem = doc.getElementsByTagName("testsuite")
        for ts in testsuitelem:
            tsname = ts.getAttribute("name")
            testcaseelem = ts.getElementsByTagName("testcase")
            failed=False
            for tc in testcaseelem:
                tcname = tc.getAttribute("name")
                tcerror = tc.getElementsByTagName("error")
                for tce in tcerror:
                    failed_tests.append(tcname)
                    failed = True
                if not failed:
                    passed_tests.append(tcname)

    if failed_tests:
        failed_tests = transform_and_write_to_file(failed_tests,"failed_tests.conf")

    if passed_tests:
        passed_tests = transform_and_write_to_file(passed_tests, "passed_tests.conf")

    return passed_tests, failed_tests

def create_headers(username, password):
    #authorization = base64.encodebytes('%s:%s' % (username, password))
    authorization = base64.encodebytes(('%s:%s' % (username, password)).encode()).decode()
    authorization=authorization.rstrip('\n')
    return {'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic %s' % authorization,
            'Accept': '*/*'}


def get_server_logs(input, path):
    for server in input.servers:
        log.info("grabbing diags from ".format(server.ip))
        diag_url = "http://{0}:{1}/diag".format(server.ip, server.port)
        log.info(diag_url)

        try:
            req = urllib.request.Request(diag_url)
            req.headers = create_headers(input.membase_settings.rest_username,
                                         input.membase_settings.rest_password)
            filename = "{0}/{1}-diag.txt".format(path, server.ip)
            page = urllib.request.urlopen(req, timeout=60)
            with open(filename, 'wb') as output:
                os.write(1, "downloading {0} ...".format(str(server.ip)).encode())
                while True:
                    buffer = page.read(65536)
                    if not buffer:
                        break
                    output.write(buffer)
                    os.write(1, ".".encode())
            file_input = open('{0}'.format(filename), 'rb')
            zipped = gzip.open("{0}.gz".format(filename), 'wb')
            zipped.writelines(file_input)
            file_input.close()
            zipped.close()

            os.remove(filename)
            log.info("downloaded and zipped diags @ : {0}".format("{0}.gz".format(filename)))
        except urllib.error.URLError:
            log.error("unable to obtain diags from %s" % diag_url)
        except BadStatusLine:
            log.error("unable to obtain diags from %s" % diag_url)
        except Exception as e:
            log.error("unable to obtain diags from %s %s" % (diag_url, e))

def get_logs_cluster_run(input, path, ns_server_path):
    print("grabbing logs (cluster-run)")
    path = path or "."
    logs_path = ns_server_path + os.sep + "logs"
    try:
        shutil.make_archive(path + os.sep + "logs", 'zip', logs_path)
    except Exception as e:
        log.error("NOT POSSIBLE TO GRAB LOGS (CLUSTER_RUN)")

def get_cbcollect_info(input, path):
    runner = cbcollectRunner(input.servers, path)
    runner.run()
    for (server, e) in runner.fail:
        log.error("NOT POSSIBLE TO GRAB CBCOLLECT FROM {0}: {1}".format(server.ip, e))

def get_couch_dbinfo(input, path):
    for server in input.servers:
        print(("grabbing dbinfo from {0}".format(server.ip)))
        path = path or "."
        try:
            couch_dbinfo_Runner(server, path).run()
        except Exception as e:
            log.error("NOT POSSIBLE TO GRAB dbinfo FROM {0}: {1}".format(server.ip, e))

def clear_old_core_dumps(_input, path):
    for server in _input.servers:
        path = path or "."
        try:
            Clearcoredumps(server, path).run()
        except Exception as e:
            log.error("Unable to clear core dumps on {0} : {1}".format(server.ip, e))

def get_core_dumps(_input, path):
    ret = False
    for server in _input.servers:
        print(("grabbing core dumps files from {0}".format(server.ip)))
        path = path or "."
        try:
            if Getcoredumps(server, path).run():
                ret = True
        except Exception as e:
            log.error("NOT POSSIBLE TO GRAB CORE DUMPS FROM {0} : {1}".\
                format(server.ip, e))
    return ret


class StoppableThreadWithResult(Thread):
    """Thread class with a stop() method. The thread itself has to check
    regularly for the stopped() condition."""

    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super(StoppableThreadWithResult, self).__init__(group=group, target=target,
                        name=name, args=args, kwargs=kwargs)
        self._stopper = Event()

    def stopit(self):
        self._stopper.set()
        self._Thread__stop()

    def stopped(self):
        return self._stopper.isSet()

    def run(self):
        if self._target is not None:
            self._return = self._target(*self._args,
                                                **self._kwargs)

    def join(self, timeout=None):
        Thread.join(self, timeout=None)
        return self._return


def runtests(names, options, arg_i, arg_p, runtime_test_params):
    log.info("\nNumber of tests initially selected before GROUP filters: " + str(len(names)))
    BEFORE_SUITE = "suite_setUp"
    AFTER_SUITE = "suite_tearDown"
    xunit = XUnitTestResult()
    # Create root logs directory
    abs_path = os.path.dirname(os.path.abspath(sys.argv[0]))
    # Create testrunner logs subdirectory
    str_time = time.strftime("%y-%b-%d_%H-%M-%S", time.localtime())
    root_log_dir = os.path.join(abs_path, "logs{0}testrunner-{1}".format(os.sep, str_time))
    if not os.path.exists(root_log_dir):
        os.makedirs(root_log_dir)

    results = []
    case_number = 1
    last_case_fail = False
    base_tear_down_run = TestInputSingleton.input.param('teardown_run', False)

    if "GROUP" in runtime_test_params:
        print(("Only cases in GROUPs '{0}' will be executed".format(runtime_test_params["GROUP"])))
    if "EXCLUDE_GROUP" in runtime_test_params:
        print(("Cases from GROUPs '{0}' will be excluded".format(runtime_test_params["EXCLUDE_GROUP"])))

    if TestInputSingleton.input.param("get-delays", False):
        # start measure_sched_delays on all servers
        sd = SchedDelays(TestInputSingleton.input.servers)
        sd.start_measure_sched_delays()

    if TestInputSingleton.input.param("hanging_threads", False):
       print("--> hanging_threads: start monitoring...")
       from hanging_threads import start_monitoring
       hanging_threads_frozen_time = int(TestInputSingleton.input.param("hanging_threads", 120))
       hanging_threads_test_interval = int(TestInputSingleton.input.param("test_interval", 1000))
       monitoring_thread = start_monitoring(seconds_frozen=hanging_threads_frozen_time, test_interval=hanging_threads_test_interval) 

    logs_folder="."
    test_exec_count=0
    for name in names:
        start_time = time.time()
        argument_split = [a.strip() for a in re.split("[,]?([^,=]+)=", name)[1:]]
        params = dict(list(zip(argument_split[::2], argument_split[1::2])))

        # Note that if ALL is specified at runtime then tests which have no groups are still run - just being
        # explicit on this

        if "GROUP" in runtime_test_params and "ALL" not in runtime_test_params["GROUP"].split(";"):
            if 'GROUP' not in params:         # params is the .conf file parameters.
                # this test is not in any groups so we do not run it
                print(("test '{0}' skipped, a group was requested and this is not any groups".format(name)))
                continue

            # there is a group for this test case, if that group is not specified at run time then do not run it
            elif not set(runtime_test_params["GROUP"].split(";")).issubset(set(params["GROUP"].split(";"))):
                print(("test '{0}' skipped, is not in the requested group".format(name)))
                continue
            else:
                pass # the test was in requested group, will run it

        elif "EXCLUDE_GROUP" in runtime_test_params:
            if 'GROUP' in params and \
                set(runtime_test_params["EXCLUDE_GROUP"].split(";")).issubset(set(params["GROUP"].split(";"))):
                    print(("test '{0}' skipped, is in an excluded group".format(name)))
                    continue
        log.info("--> Running test: {}".format(name))
        test_exec_count += 1
        # Create Log Directory
        logs_folder = os.path.join(root_log_dir, "test_%s" % case_number)
        log.info("Logs folder: {}".format(logs_folder))
        os.mkdir(logs_folder)
        test_log_file = os.path.join(logs_folder, "test.log")
        log_config_filename = r'{0}'.format(os.path.join(logs_folder, "test.logging.conf"))
        create_log_file(log_config_filename, test_log_file, options.loglevel)
        logging.config.fileConfig(log_config_filename)
        print(("Logs will be stored at {0}".format(logs_folder)))
        print(("\n.{3}testrunner -i {0} -p {1} -t {2}\n"\
              .format(arg_i or "", arg_p or "", name, os.sep)))
        name = name.split(",")[0]

        # Update the test params for each test
        TestInputSingleton.input.test_params = params
        TestInputSingleton.input.test_params.update(runtime_test_params)
        TestInputSingleton.input.test_params["case_number"] = case_number
        TestInputSingleton.input.test_params["last_case_fail"] = \
            str(last_case_fail)
        TestInputSingleton.input.test_params["teardown_run"] = \
            str(base_tear_down_run)
        TestInputSingleton.input.test_params["logs_folder"] = logs_folder
        print("Test Input params:")
        print((TestInputSingleton.input.test_params))
        if "get-coredumps" in TestInputSingleton.input.test_params:
            if TestInputSingleton.input.param("get-coredumps", True):
                clear_old_core_dumps(TestInputSingleton.input, logs_folder)
        if case_number == 1 and "upgrade" not in name and "xdcr" not in name:
            before_suite_name = "%s.%s" % (name[:name.rfind('.')], BEFORE_SUITE)
            try:
                print(("Run before suite setup for %s" % name))
                suite = unittest.TestLoader().loadTestsFromName(before_suite_name)
                print(("-->before_suite_name:{},suite: {}".format(before_suite_name,suite)))
                result = unittest.TextTestRunner(verbosity=2).run(suite)
                print(("-->result: {}".format(result)))
                if "get-coredumps" in TestInputSingleton.input.test_params:
                    if TestInputSingleton.input.param("get-coredumps", True):
                        if get_core_dumps(TestInputSingleton.input, logs_folder):
                            result = unittest.TextTestRunner(verbosity=2)._makeResult()
                            result.errors = [(name, "Failing test : new core dump(s) "
                                             "were found and collected."
                                             " Check testrunner logs folder.")]
                            log.info("FAIL: New core dump(s) was found and collected")
            except AttributeError as ex:
                traceback.print_exc()
                pass
        try:
            suite = unittest.TestLoader().loadTestsFromName(name)
        except AttributeError as e:
            print(("Test {0} was not found: {1}".format(name, e)))
            result = unittest.TextTestRunner(verbosity=2)._makeResult()
            result.errors = [(name, str(e))]
        except SyntaxError as e:
            print(("SyntaxError in {0}: {1}".format(name, e)))
            result = unittest.TextTestRunner(verbosity=2)._makeResult()
            result.errors = [(name, str(e))]
        else:
            test_timeout = TestInputSingleton.input.param("test_timeout", None)
            t = StoppableThreadWithResult(target=unittest.TextTestRunner(verbosity=2).run,
               name="test_thread",
               args=(suite))
            t.start()
            result = t.join(timeout=test_timeout)
            if "get-coredumps" in TestInputSingleton.input.test_params:
                if TestInputSingleton.input.param("get-coredumps", True):
                    if get_core_dumps(TestInputSingleton.input, logs_folder):
                        result = unittest.TextTestRunner(verbosity=2)._makeResult()
                        result.errors = [(name, "Failing test : new core dump(s) "
                                             "were found and collected."
                                             " Check testrunner logs folder.")]
                        log.info("FAIL: New core dump(s) was found and collected")
            if not result:
                for t in threading.enumerate():
                    if t != threading.current_thread():
                        t._Thread__stop()
                result = unittest.TextTestRunner(verbosity=2)._makeResult()
                case_number += 1000
                print ("========TEST WAS STOPPED DUE TO  TIMEOUT=========")
                result.errors = [(name, "Test was stopped due to timeout")]
        time_taken = time.time() - start_time

        # Concat params to test name
        # To make tests more readable
        params = ''
        if TestInputSingleton.input.test_params:
            for key, value in list(TestInputSingleton.input.test_params.items()):
                if key and value:
                    params += "," + str(key) + "=" + str(value)

        base_tear_down_run = TestInputSingleton.input.param(
            'teardown_run', False)
        if result.failures or result.errors:
            # Immediately get the server logs, if
            # the test has failed or has errors
            last_case_fail = True
            if "get-logs" in TestInputSingleton.input.test_params:
                get_server_logs(TestInputSingleton.input, logs_folder)

            if "get-logs-cluster-run" in TestInputSingleton.input.test_params:
                if TestInputSingleton.input.param("get-logs-cluster-run", True):
                    # Generate path to ns_server directory
                    ns_server_path = os.path.normpath(abs_path + os.sep + os.pardir + os.sep + "ns_server")
                    get_logs_cluster_run(TestInputSingleton.input, logs_folder, ns_server_path)

            if "get-cbcollect-info" in TestInputSingleton.input.test_params:
                if TestInputSingleton.input.param("get-cbcollect-info", True):
                    get_cbcollect_info(TestInputSingleton.input, logs_folder)

            if "get-couch-dbinfo" in TestInputSingleton.input.test_params and \
                TestInputSingleton.input.param("get-couch-dbinfo", True):
                    get_couch_dbinfo(TestInputSingleton.input, logs_folder)

            errors = []
            for failure in result.failures:
                test_case, failure_string = failure
                errors.append(failure_string)
                break
            for error in result.errors:
                test_case, error_string = error
                errors.append(error_string)
                break
            xunit.add_test(name=name, status='fail', time=time_taken,
                           errorType='membase.error', errorMessage=str(errors),
                           params=params)
            results.append({"result": "fail", "name": name})
        else:
            last_case_fail = False
            xunit.add_test(name=name, time=time_taken, params=params)
            results.append({"result": "pass", "name": name, "time": time_taken})
        xunit.write("{0}{2}report-{1}".format(os.path.dirname(logs_folder), str_time, os.sep))
        xunit.print_summary()
        print(("testrunner logs, diags and results are available under {0}".format(logs_folder)))
        case_number += 1
        if (result.failures or result.errors) and \
                TestInputSingleton.input.param("stop-on-failure", False):
            print("test fails, all of the following tests will be skipped!!!")
            break

    print("\n*** Tests executed count: {}\n".format(test_exec_count))
    if test_exec_count > 0 and "upgrade" not  in name:
        after_suite_name = "%s.%s" % (name[:name.rfind('.')], AFTER_SUITE)
        try:
            print(("Run after suite setup for %s" % name))
            suite = unittest.TestLoader().loadTestsFromName(after_suite_name)
            result = unittest.TextTestRunner(verbosity=2).run(suite)
        except AttributeError as ex:
            pass
    if "makefile" not in TestInputSingleton.input.test_params:
        print("During the test, Remote Connections: %s, Disconnections: %s" %
                (RemoteMachineShellConnection.connections,
                RemoteMachineShellConnection.disconnections))

        if TestInputSingleton.input.param("get-delays", False):
            sd.stop_measure_sched_delay()
            sd.fetch_logs()

    # terminate any non main thread - these were causing hangs
    for t in threading.enumerate():
        if t.name != 'MainThread' and t.is_alive():
            print(('Thread', t, 'was not properly terminated, will be terminated now.'))
            if hasattr(t, 'shutdown'):
                print("Shutting down the thread...")
                t.shutdown(True)
            else:
                print("Stopping the thread...")
                try:
                    t._stop()
                except Exception as e:
                    print("Unable to stop hung thread, killing python process")
                    os.kill(os.getpid(), signal.SIGKILL)
    
    if "makefile" in TestInputSingleton.input.test_params:
        # print out fail for those tests which failed and do sys.exit() error code
        fail_count = 0
        for result in results:
            if result["result"] == "fail":
                print((result["name"], " fail "))
                fail_count += 1
            else:
                print((result["name"], " pass"))
        if fail_count > 0:
            sys.exit(1)

    return results, xunit, "{0}{2}report-{1}".format(os.path.dirname(logs_folder), str_time, os.sep)


def filter_fields(testname):
    # TODO: Fix for old xml style
    if "logs_folder:" in testname:
        testwords = testname.split(",")
        line = ""
        for fw in testwords:
            if not fw.startswith("logs_folder") and not fw.startswith("conf_file") \
                    and not fw.startswith("cluster_name:") \
                    and not fw.startswith("ini:") \
                    and not fw.startswith("case_number:") \
                    and not fw.startswith("num_nodes:") \
                    and not fw.startswith("spec:") \
                    and not fw.startswith("last_case_fail:") \
                    and not fw.startswith("teardown_run:") \
                    and not fw.startswith("get-cbcollect-info"):
                if not "\":" in fw or "query:" in fw:
                    #log.info("Replacing : with ={}".format(fw))
                    line = line + fw.replace(":", "=", 1)
                else:
                    line = line + fw
                if fw != testwords[-1]:
                    line = line + ","

        return line    
    else:
        testwords = testname.split(",")
        line = []
        for fw in testwords:
            if not fw.startswith("logs_folder=") and not fw.startswith("conf_file=") \
                    and not fw.startswith("cluster_name=") \
                    and not fw.startswith("ini=") \
                    and not fw.startswith("case_number=") \
                    and not fw.startswith("num_nodes=") \
                    and not fw.startswith("spec=") \
                    and not fw.startswith("last_case_fail=") \
                    and not fw.startswith("teardown_run=") \
                    and not fw.startswith("get-cbcollect-info="):
                line.append(fw)
        return ",".join(line)

def compare_with_sort(dict, key):
    for k in list(dict.keys()):
        if "".join(sorted(k)) == "".join(sorted(key)):
            return True

    return False


def merge_reports(filespath):
    log.info("Merging of report files from "+str(filespath))

    testsuites = {}
    if not isinstance(filespath, list):
        filespaths = filespath.split(",")
    else:
        filespaths = filespath
    for filepath in filespaths:
        xml_files = glob.glob(filepath)
        if not isinstance(filespath, list) and filespath.find("*"):
            xml_files.sort(key=os.path.getmtime)
        for xml_file in xml_files:
            log.info("-- " + xml_file + " --")
            doc = xml.dom.minidom.parse(xml_file)
            testsuitelem = doc.getElementsByTagName("testsuite")
            for ts in testsuitelem:
                tsname = ts.getAttribute("name")
                tserros = ts.getAttribute("errors")
                tsfailures = ts.getAttribute("failures")
                tsskips = ts.getAttribute("skips")
                tstime = ts.getAttribute("time")
                tstests = ts.getAttribute("tests")
                issuite_existed = False
                tests = {}
                testsuite = {}
                # fill testsuite details
                if tsname in list(testsuites.keys()):
                    testsuite = testsuites[tsname]
                    tests = testsuite['tests']
                else:
                    testsuite['name'] = tsname
                testsuite['errors'] = tserros
                testsuite['failures'] = tsfailures
                testsuite['skips'] = tsskips
                testsuite['time'] = tstime
                testsuite['testcount'] = tstests
                issuite_existed = False
                testcaseelem = ts.getElementsByTagName("testcase")
                # fill test case details
                for tc in testcaseelem:
                    testcase = {}
                    tcname = tc.getAttribute("name")
                    tctime = tc.getAttribute("time")
                    tcerror = tc.getElementsByTagName("error")

                    tcname_filtered = filter_fields(tcname)
                    if compare_with_sort(tests, tcname_filtered):
                        testcase = tests[tcname_filtered]
                        testcase['name'] = tcname
                    else:
                        testcase['name'] = tcname
                    testcase['time'] = tctime
                    testcase['error'] = ""
                    if tcerror:
                        testcase['error']  = str(tcerror[0].firstChild.nodeValue)

                    tests[tcname_filtered] = testcase
                testsuite['tests'] = tests
                testsuites[tsname] = testsuite

    log.info("\nNumber of TestSuites="+str(len(testsuites)))
    tsindex = 0
    for tskey in list(testsuites.keys()):
        tsindex = tsindex+1
        log.info("\nTestSuite#"+str(tsindex)+") "+str(tskey)+", Number of Tests="+str(len(testsuites[tskey]['tests'])))
        pass_count = 0
        fail_count = 0
        tests = testsuites[tskey]['tests']
        xunit = XUnitTestResult()
        for testname in list(tests.keys()):
            testcase = tests[testname]
            tname = testcase['name']
            ttime = testcase['time']
            inttime = float(ttime)
            terrors = testcase['error']
            tparams = ""
            if "," in tname:
                tparams = tname[tname.find(","):]
                tname = tname[:tname.find(",")]

            if terrors:
                failed = True
                fail_count = fail_count + 1
                xunit.add_test(name=tname, status='fail', time=inttime,
                               errorType='membase.error', errorMessage=str(terrors), params=tparams
                               )
            else:
                passed = True
                pass_count = pass_count + 1
                xunit.add_test(name=tname, time=inttime, params=tparams
                    )

        str_time = time.strftime("%y-%b-%d_%H-%M-%S", time.localtime())
        abs_path = os.path.dirname(os.path.abspath(sys.argv[0]))
        root_log_dir = os.path.join(abs_path, "logs{0}testrunner-{1}".format(os.sep, str_time))
        if not os.path.exists(root_log_dir):
            os.makedirs(root_log_dir)
        logs_folder = os.path.join(root_log_dir, "merged_summary")
        try:
            os.mkdir(logs_folder)
        except:
            pass
        output_filepath="{0}{2}mergedreport-{1}".format(logs_folder, str_time, os.sep).strip()

        xunit.write(output_filepath)
        xunit.print_summary()
        log.info("Summary file is at " + output_filepath+"-"+tsname+".xml")
    return testsuites



def reruntests(rerun, names, options, arg_i, arg_p,runtime_test_params):
    if "=" in rerun:
        reruns = rerun.split("=")
        rerun_type = reruns[0]
        rerun_count = int(reruns[1])
        all_results = {}
        log.info("NOTE: Running " + rerun_type + " tests for " + str(rerun_count) + " times maximum.")

        report_files = []
        for testc in range(rerun_count+1):
            if testc == 0:
                log.info("\n*** FIRST run of the tests ***")
            else:
                log.info("\n*** "+rerun_type.upper()+" Tests Rerun#" + str(testc) + "/" + str(rerun_count) + " ***")
            results, xunit, report_file = runtests(names, options, arg_i, arg_p, runtime_test_params)
            all_results[(testc + 1)] = results
            all_results[str(testc+1)+"_report"] = report_file+"*.xml"
            report_files.append(report_file+"*.xml")
            tobe_rerun = False
            for result in results:
                if result["result"] == rerun_type:
                    tobe_rerun = True
            if not tobe_rerun:
                break
            tp, tf = parse_junit_result_xml(report_file+"*.xml")
            if "fail" == rerun_type:
                names = tf
            elif "pass" == rerun_type:
                names = tp

        log.info("\nSummary:\n" + str(all_results))
        log.info("Final result: merging...")
        merge_reports(report_files)
        return all_results

def main():
    log.info("TestRunner: parsing args...")
    names, runtime_test_params, arg_i, arg_p, options = parse_args(sys.argv)
    log.info("TestRunner: start...")
    # get params from command line
    TestInputSingleton.input = TestInputParser.get_test_input(sys.argv)
    # ensure command line params get higher priority
    runtime_test_params.update(TestInputSingleton.input.test_params)
    TestInputSingleton.input.test_params = runtime_test_params
    log.info("Global Test input params:")
    pprint(TestInputSingleton.input.test_params)
    if names:
        if options.merge:
            merge_reports(options.merge)
        elif options.rerun:
            results = reruntests(options.rerun, names, options, arg_i, arg_p, runtime_test_params)
        else:
            results, _, _ = runtests(names, options, arg_i, arg_p,runtime_test_params)
    else:
        log.warning("Warning: No tests got selected. Please double check the .conf file and other "
              "options!")


    log.info("TestRunner: end...")


def watcher():
    """This little code snippet is from
    http://greenteapress.com/semaphores/threading_cleanup.py (2012-07-31)
    It's now possible to interrupt the testrunner via ctrl-c at any time
    in a platform neutral way."""
    if sys.platform == 'win32':
        p = Process(target=main, name="MainProcess")
        p.start()
        try:
            p.join()
            rc = p.exitcode
            if rc > 0:
                sys.exit(rc)
        except KeyboardInterrupt:
            log.error('KeyBoardInterrupt')
            p.terminate()
    else:
        child = os.fork()
        if child == 0:
            main() # child runs test
        try:
            rc = os.waitpid(child, 0)[1] //256 # exit status is the high order byte of second member of the tuple
            if rc > 0:
                sys.exit( rc )
        except KeyboardInterrupt:
            log.error('KeyBoardInterrupt')
            try:
                os.kill(child, signal.SIGKILL)
            except OSError:
                pass
        except OSError:
            pass

    sys.exit()

if __name__ == "__main__":
    watcher()

