""" Common module contains resueable methods for tools
"""

import os
import re
import time

class Generics(object):
    """ Generic static methods for use in tools"""

    @staticmethod
    def locate_conf_file(filename):
        """
        gets a filename object based on filename
        arguments:
            filename  -- conf file
        """

        filename = "../%s" % filename
        if filename:
            if os.path.exists(filename):
                return file(filename)
            if os.path.exists("conf{0}{1}".format(os.sep, filename)):
                return file("conf{0}{1}".format(os.sep, filename))
            return None


    @staticmethod
    def parse_conf_file(filename):
        """
        get list of tests in conf file as array
        arguments:
            filename  -- conf file
        """

        tests = []
        file_handle = Generics.locate_conf_file(filename)
        if not file_handle:
            # TODO: logger with WARN level
            # print "WARN: unable to locate configuration file: " + filename
            return []

        prefix = None
        for line in file_handle:
            stripped = line.strip()
            if stripped.startswith("#") or len(stripped) <= 0:
                continue
            if stripped.endswith(":"):
                prefix = stripped.split(":")[0]
                continue
            name = stripped
            if line.startswith(" ") and prefix:
                name = prefix + "." + name
            prefix = ".".join(name.split(",")[0].split('.')[0:-1])
            tests.append(name)

        return tests

    @staticmethod
    def parse_params(test):
        """
        parses out the param portion of test definition
        arguments:
            test -- test definition
        """

        params = None
        parts = test.split(',')
        if len(parts) > 1:
            params = ','.join(parts[1:])
        return params


    @staticmethod
    def split_test_params(params):
        """
        takes a param string like k1=v1,k2=v2, and returns [k1=v1, k2=v2]
        arguments:
            params -- param string of a test
        """

        if params is None:
            return []

        return [p if p is not None else ""
                for p in params.split(",")]

    @staticmethod
    def split_test_base(test_list):
        """
        takes a list of test strings and returns
        the base test name without params.
        the tests are indexed to establish uniquness
        arguments:
            test_list -- array of test names
        """

        base_tests = []
        history = []
        for test_name in test_list:
            test_base = test_name.split(',')[0]
            test = test_base
            if test_base in history:
                # is duplicate
                count = history.count(test_base)
                test = "%s.%d" % (test, count)
            history.append(test_base)
            base_tests.append(test)

        return base_tests

    @staticmethod
    def split_test_param_keys(params):
        """
        takes a param string like k1=v1,k2=v2, and returns [k1, k2]
        arguments:
            params -- param string of a test
        """

        if params is None:
            return []

        return [p.split("=")[0]
                if p is not None else ""
                for p in params.split(',')]

    @staticmethod
    def rm_leading_slash(filepath):
        """ SUBJ """

        filepath = re.sub(r'^/', '', filepath)
        filepath = re.sub(r'^conf/', '', filepath)
        return filepath

    @staticmethod
    def timestamp():
        """
            current time formated as YYYY-MM-DD
        """
        ts_now = time.time()
        ts_date = time.gmtime(ts_now)
        ts_year = ts_date.tm_year
        ts_month = str(ts_date.tm_mon).zfill(2)
        ts_day = str(ts_date.tm_mday).zfill(2)
        return "%d-%s-%s" % (ts_year, ts_month, ts_day)


    @staticmethod
    def timestamp_sec():
        """
            current time in unix format seconds
        """
        ts_now = time.time()
        return "%d" % ts_now
