#!/usr/bin/env python

import os
import sys
sys.path = ["lib", "pytests", "pysystests"] + sys.path
import time
from xunit import XUnitTestResult
import glob
import xml.dom.minidom
import logging
log = logging.getLogger(__name__)
logging.info(__name__)
logging.getLogger().setLevel(logging.INFO)
import argparse


def filter_fields(testname, run_params=""):
    if "logs_folder:" in testname:
        testwords = testname.split(",")
        line = ""
        filter_test_params = ['logs_folder', 'conf_file',
                              'cluster_name:', 'ini:', 'case_number:',
                              'num_nodes:', 'spec:', 'is_container:',
                              'last_case_fail:', 'teardown_run:', 'get-cbcollect-info']
        filter_test_params.extend([param.split("=")[0] for param in
                            run_params.split(',')])
        for fw in testwords:
            filter_word = False
            for filter_words in filter_test_params:
                if filter_words and fw.startswith(filter_words):
                    filter_word = True
            if not filter_word:
                line = line + fw.replace(":", "=", 1)
                if fw != testwords[-1]:
                    line = line + ","
        return line.rstrip(',')
    else:
        testwords = testname.split(",")
        line = []
        filter_test_params = ['logs_folder=', 'conf_file=',
                              'cluster_name=', 'ini=', 'case_number=',
                              'num_nodes=', 'spec=', 'is_container=',
                              'last_case_fail=', 'teardown_run=', 'get-cbcollect-info=']
        filter_test_params.extend([param.split("=")[0] for param in
                            run_params.split(',')])
        for fw in testwords:
            filter_word = False
            for filter_words in filter_test_params:
                if filter_words and fw.startswith(filter_words):
                    filter_word = True
            if not filter_word:
                line.append(fw)
        return ",".join(line)

def compare_with_sort(dict, key):
    key_split = key.split(',')
    key = '%s,%s' % (key_split[0], ','.join(sorted(key_split[1:])))
    for k in dict.keys():
        test_case_split = k.split(',')
        test_case = "%s,%s" % (test_case_split[0],
                               ",".join(sorted(test_case_split[1:])))
        if key == test_case:
            return True, k

    return False, None


def merge_reports(filespath, run_params=""):
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
                if tsname in testsuites.keys():
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

                    tcname_filtered = filter_fields(tcname, run_params)
                    test_case_present, key = compare_with_sort(tests,
                                                               tcname_filtered)
                    if test_case_present:
                        testcase = tests[key]
                        testcase['name'] = tcname
                    else:
                        testcase['name'] = tcname
                    testcase['time'] = tctime
                    testcase['error'] = ""
                    if tcerror:
                        testcase['error']  = str(tcerror[0].firstChild.nodeValue)
                    if test_case_present:
                        tests[key] = testcase
                    else:
                        tests[tcname_filtered] = testcase
                testsuite['tests'] = tests
                testsuites[tsname] = testsuite
    try:
        abs_path = os.path.dirname(os.path.abspath(sys.argv[0]))
        abs_path = abs_path.rstrip("scripts")
        logs_directory = os.path.join(abs_path, "logs")
        move_logs_directory = os.path.join(abs_path, "job_logs")
        os.rename(logs_directory, move_logs_directory)
        os.mkdir(logs_directory)
    except Exception as e:
        log.info(e)
        return {}
    log.info("\nNumber of TestSuites="+str(len(testsuites)))
    tsindex = 0
    for tskey in testsuites.keys():
        tsindex = tsindex+1
        log.info("\nTestSuite#"+str(tsindex)+") "+str(tskey)+", Number of Tests="+str(len(testsuites[tskey]['tests'])))
        pass_count = 0
        fail_count = 0
        tests = testsuites[tskey]['tests']
        xunit = XUnitTestResult()
        for testname in tests.keys():
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
        root_log_dir = os.path.join(logs_directory, "testrunner-{0}".format(
            str_time))
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('run_params', type=str, default="")
    parser.add_argument('files', metavar='<xml file1> <xml file2> ...', type=str, nargs='+',
                        help='Accept all input xml files')
    args = parser.parse_args()

    print(args.files)
    merge_reports(args.files, args.run_params)