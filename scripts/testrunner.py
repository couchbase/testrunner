#!/usr/bin/env python

import base64
import gzip
from http.client import BadStatusLine
import os
import urllib.request, urllib.error, urllib.parse
import uuid
import sys
from os.path import basename, splitext
from pprint import pprint
sys.path = ["lib", "pytests"] + sys.path

if sys.hexversion < 0x02060000:
    print("Testrunner requires version 2.6+ of python")
    sys.exit()

import re
import time
import unittest
import logger
import logging.config
from xunit import XUnitTestResult
from TestInput import TestInputParser, TestInputSingleton
from optparse import OptionParser, OptionGroup
from scripts.collect_server_info import cbcollectRunner
import signal


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
    tgroup.add_option("-i", "--ini",
                      dest="ini", help="Path to .ini file containing server information,e.g -i tmp/local.ini")
    tgroup.add_option("-c", "--config", dest="conf",
                      help="Config file name (located in the conf subdirectory), e.g -c py-view.conf")
    tgroup.add_option("-t", "--test",
                      dest="testcase", help="Test name (multiple -t options add more tests) e.g -t performance.perf.DiskDrainRate")
    parser.add_option_group(tgroup)

    parser.add_option("-p", "--params",
                      dest="params", help="Optional key=value parameters, comma-separated -p k=v,k2=v2,...",
                      default="")
    parser.add_option("-n", "--noop", action="store_true",
                      help="NO-OP - emit test names, but don't actually run them e.g -n true")
    parser.add_option("-l", "--log-level",
                      dest="loglevel", default="INFO", help="e.g -l info,warning,error")
    options, args = parser.parse_args()

    tests = []
    test_params = {}

    if not options.ini:
        parser.error("please specify an .ini file (-i)")
        parser.print_help()
    else:
        test_params['ini'] = options.ini

    test_params['cluster_name'] = splitext(os.path.basename(options.ini))[0]

    if not options.testcase and not options.conf:
        parser.error("please specify a configuration file (-c) or a test case (-t)")
        parser.print_help()
    if options.conf:
        parse_conf_file(options.conf, tests, test_params)
    if options.testcase:
        tests.append(options.testcase)
    if options.noop:
        print(("\n".join(tests)))
        sys.exit(0)

    return tests, test_params, options.ini, options.params, options


def create_log_file(log_config_file_name, log_file_name, level):
    tmpl_log_file = open("logging.conf.sample")
    log_file = open(log_config_file_name, "w")
    log_file.truncate()
    for line in tmpl_log_file:
        newline = line.replace("@@LEVEL@@", level)
        newline = newline.replace("@@FILENAME@@", log_file_name)
        log_file.write(newline)
    log_file.close()
    tmpl_log_file.close()


def append_test(tests, name):
    prefix = ".".join(name.split(".")[0:-1])
    if name.find('*') > 0:
        for t in unittest.TestLoader().loadTestsFromName(name.rstrip('.*')):
            tests.append(prefix + '.' + t._testMethodName)
    else:
        tests.append(name)


def locate_conf_file(filename):
    print("filename: %s" % filename)
    if filename:
        if os.path.exists(filename):
            return file(filename)
        if os.path.exists("conf/" + filename):
            return file("conf/" + filename)
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
            print("prefix: {0}".format(prefix))
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


def create_headers(username, password):
    authorization = base64.encodestring('%s:%s' % (username, password))
    return {'Content-Type': 'application/x-www-form-urlencoded',
            'Authorization': 'Basic %s' % authorization,
            'Accept': '*/*'}


def get_server_logs(input, path):

    diag_name = uuid.uuid4()
    for server in input.servers:
        print("grabbing diags from ".format(server.ip))
        diag_url = "http://{0}:{1}/diag".format(server.ip, server.port)
        print(diag_url)

        try:
            req = urllib.request.Request(diag_url)
            req.headers = create_headers(input.membase_settings.rest_username,
                                         input.membase_settings.rest_password)
            filename = "{0}/{1}-{2}-diag.txt".format(path, diag_name, server.ip)
            page = urllib.request.urlopen(req)
            with open(filename, 'wb') as output:
                os.write(1, "downloading {0} ...".format(server.ip))
                while True:
                    buffer = page.read(65536)
                    if not buffer:
                        break
                    output.write(buffer)
                    os.write(1, ".")
            file_input = open('{0}'.format(filename), 'rb')
            zipped = gzip.open("{0}.gz".format(filename), 'wb')
            zipped.writelines(file_input)
            file_input.close()
            zipped.close()

            os.remove(filename)
            print("downloaded and zipped diags @ : {0}".format("{0}.gz".format(filename)))
        except urllib.error.URLError:
            print("unable to obtain diags from %s" % diag_url)
        except BadStatusLine:
            print("unable to obtain diags from %s" % diag_url)
        except Exception as e:
            print("unable to obtain diags from %s %s" % (diag_url, e))


def get_cbcollect_info(input, path):
    for server in input.servers:
        print("grabbing cbcollect from {0}".format(server.ip))
        path = path or "."
        try:
            cbcollectRunner(server, path).run()
        except:
            print("IMPOSSIBLE TO GRAB CBCOLLECT FROM {0}".format(server.ip))


def watcher():
    """This little code snippet is from
    http://greenteapress.com/semaphores/threading_cleanup.py (2012-07-31)
    It's now possible to interrupt the testrunner via ctrl-c at any time"""
    child = os.fork()
    if child == 0: return
    try:
        os.wait()
    except KeyboardInterrupt:
        print('KeyBoardInterrupt')
        try:
            os.kill(child, signal.SIGKILL)
        except OSError:
            pass
    sys.exit()

def main():
    watcher()

    names, test_params, arg_i, arg_p, options = parse_args(sys.argv)
    # get params from command line
    TestInputSingleton.input = TestInputParser.get_test_input(sys.argv)
    # ensure command line params get higher priority
    test_params.update(TestInputSingleton.input.test_params)
    TestInputSingleton.input.test_params = test_params
    print("Global Test input params:")
    pprint(TestInputSingleton.input.test_params)

    xunit = XUnitTestResult()

    # Create root logs directory
    if not os.path.exists("logs"):
        os.makedirs("logs")

    # Create testrunner logs subdirectory
    str_time = time.strftime("%y-%b-%d_%H-%M-%S", time.localtime())
    logs_folder = "logs/testrunner-" + str_time
    logs_folder_abspath = os.path.abspath(logs_folder)
    if not os.path.exists(logs_folder):
        os.makedirs(logs_folder)

    results = []
    case_number = 1
    if "GROUP" in test_params:
        print("Only cases in GROUPs '{0}' will be executed".format(test_params["GROUP"]))
    if "EXCLUDE_GROUP" in test_params:
        print("Cases from GROUPs '{0}' will be excluded".format(test_params["EXCLUDE_GROUP"]))

    for name in names:
        start_time = time.time()
        argument_split = [a.strip() for a in re.split("[,]?([^,=]+)=", name)[1:]]
        params = dict(list(zip(argument_split[::2], argument_split[1::2])))

        if ("GROUP" or "EXCLUDE_GROUP") in test_params:
            #determine if the test relates to the specified group(can be separated by ';')
            if not ("GROUP" in params and len(set(test_params["GROUP"].split(";")) & set(params["GROUP"].split(";")))) or \
                    "EXCLUDE_GROUP" in params and len(set(test_params["EXCLUDE_GROUP"].split(";")) & set(params["EXCLUDE_GROUP"].split(";"))):
                print("test '{0}' was skipped".format(name))
                continue

        log_config_filename = ""
        #reduce the number of chars in the file name, if there are many(255 bytes filename limit)
        if len(name) > 240:
            name = os.path.join(name[:220] + time.strftime("%y-%b-%d_%H-%M-%S", time.localtime()))
        if params:
            log_name = os.path.join(logs_folder_abspath, name + ".log")
            log_config_filename = os.path.join(logs_folder_abspath, name + ".logging.conf")
        else:
            dotnames = name.split('.')
            log_name = os.path.join(logs_folder_abspath, dotnames[-1] + ".log")
            log_config_filename = os.path.join(logs_folder_abspath, dotnames[-1] + ".logging.conf")
        create_log_file(log_config_filename, log_name, options.loglevel)
        logging.config.fileConfig(log_config_filename)
        print("\n./testrunner -i {0} {1} -t {2}\n"\
              .format(arg_i or "", arg_p or "", name))
        name = name.split(",")[0]

        # Update the test params for each test
        TestInputSingleton.input.test_params = params
        TestInputSingleton.input.test_params.update(test_params)
        TestInputSingleton.input.test_params["case_number"] = case_number
        print("Test Input params:")
        pprint(TestInputSingleton.input.test_params)
        suite = unittest.TestLoader().loadTestsFromName(name)
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        time_taken = time.time() - start_time

        # Concat params to test name
        # To make tests more readable
        params = ''
        if TestInputSingleton.input.test_params:
            for key, value in list(TestInputSingleton.input.test_params.items()):
                if key and value:
                    params += "," + str(key) + ":" + str(value)

        if result.failures or result.errors:
            # Immediately get the server logs, if
            # the test has failed or has errors
            if "get-logs" in TestInputSingleton.input.test_params:
                get_server_logs(TestInputSingleton.input, logs_folder)

            if "get-cbcollect-info" in TestInputSingleton.input.test_params:
                if TestInputSingleton.input.param("get-cbcollect-info", True):
                    get_cbcollect_info(TestInputSingleton.input, logs_folder)

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
            xunit.add_test(name=name, time=time_taken, params=params)
            results.append({"result": "pass", "name": name, "time": time_taken})
        xunit.write("{0}/report-{1}".format(logs_folder, str_time))
        xunit.print_summary()
        print("testrunner logs, diags and results are available under {0}".format(logs_folder))
        case_number += 1
        if (result.failures or result.errors) and \
                TestInputSingleton.input.param("stop-on-failure", False):
            print("test fails, all of the following tests will be skipped!!!")
            break

    if "makefile" in TestInputSingleton.input.test_params:
        # print out fail for those tests which failed and do sys.exit() error code
        fail_count = 0
        for result in results:
            if result["result"] == "fail":
                print(result["name"], " fail ")
                fail_count += 1
            else:
                print(result["name"], " pass")
        if fail_count > 0:
            sys.exit(1)

if __name__ == "__main__":
    main()
