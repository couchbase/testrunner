"""Test repo reporting tool

This script will query the qe test suite couchbase cluster for conf files.
It then reads the tests within each conf file from the local filesystem which
may contain any changes made by a recent git commit.  Changes are detected by
comparing the tests from local git repo with tests stored in remote couchbase
bucket.  Changes are recorded and stored into a history bucket and then the
remote couchbase server is updated.
"""

import os
import re
import argparse
from common import Generics as CG
from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
from couchbase.exceptions import NotFoundError, CouchbaseNetworkError

FUNCTIONAL_TEST_TYPE = "functional"
PERFORMANCE_TEST_TYPE = "performance"

def main():
    """
    parse args and start repo manager
    """
    parser = argparse.ArgumentParser(description='Testrunner reporting tool')
    parser.add_argument(
        '-qe_cluster',
        type=str,
        help='couchbase node with qe data')
    parser.add_argument(
        '-qe_bucket',
        type=str,
        default='QE-Test-Suites',
        help='name of bucket on qe_cluster with suite data')
    parser.add_argument(
        '-repo_cluster',
        type=str,
        help='couchbae node for test repo data')
    parser.add_argument(
        '-perf_repo_dir',
        type=str,
        default="perfrunner",
        help='path to perfrunner repo')

    args = parser.parse_args()

    repo_manager = TestRepoManager(
        args.repo_cluster,
        args.qe_cluster,
        args.qe_bucket,
        args.perf_repo_dir)

    # run repo manager against test types
    test_types = [FUNCTIONAL_TEST_TYPE, PERFORMANCE_TEST_TYPE]
    for test_type in test_types:

        print "===== %s =====" % test_type

        # get test config files for all tests
        conf_info = repo_manager.get_conf_files(test_type)

        # create history docs for today
        repo_manager.update_history_bucket(conf_info)

        # push update conf file with latest changes
        repo_manager.update_test_bucket(conf_info)


class TestRepoManager(object):
    """
    TestRepoManager performs test comparisions, pushes updates to history bucket
    and updates source repo bucket
    """

    def __init__(self, repo_cluster, qe_cluster, qe_bucket, perf_repo_dir):
        self.repo_cluster = repo_cluster
        self.qe_cluster = qe_cluster
        self.qe_bucket = qe_bucket
        self.perf_repo_dir = perf_repo_dir
        self.conf_history = []

    def get_conf_files(self, test_type):
        """
        retrievs list of conf files based on test type
        arguments:
            test_type -- type of test functional, peformance
        """

        if test_type == FUNCTIONAL_TEST_TYPE:
            return self.get_functional_tests()
        if test_type == PERFORMANCE_TEST_TYPE:
            return self.get_perfomance_tests()

    def get_functional_tests(self):
        """
        query the qe bucket for conf files that are used for testing
        """

        rows = []
        url = 'couchbase://%s/%s' % (self.qe_cluster, self.qe_bucket)
        bucket = Bucket(url)
        statement = 'SELECT component, \
                    subcomponent, \
                    confFile FROM `%s`' % self.qe_bucket
        query = N1QLQuery(statement)

        # add each row as test
        for row in bucket.n1ql_query(query):
            row['type'] = 'functional'
            rows.append(row)
        return rows

    def get_perfomance_tests(self):
        """
        walk the perfrunner directory and retrieve tests files
        """
        rows = []
        tests = {}
        root_dir = "%s/tests" % self.perf_repo_dir
        for _, _, files in os.walk(root_dir):
            for conf in files:
                match_str = re.search('.*.test$', conf)
                if match_str is not None:
                    parts = conf.split("_")
                    component = parts[0]
                    sub_component = component
                    if len(parts) > 1:
                        if not parts[1][0].isdigit():
                            match_str = re.split('[0-9]', parts[1])
                            sub_component = match_str[0]
                    if component in tests:
                        if sub_component in tests[component]:
                            tests[component][sub_component].append(conf)
                        else:
                            tests[component][sub_component] = [conf]
                    else:
                        tests[component] = {}
                        tests[component][sub_component] = [conf]

        # combine tests by components[subcomponent]
        for component in tests:
            for sub_component in tests[component]:
                val = tests[component][sub_component]
                conf = "%s_%s.conf" % (component, sub_component)
                rows.append({
                    'confFile': conf,
                    'component': component,
                    'subcomponent': sub_component,
                    'tests': val,
                    'type': 'performance'})
        return rows

    def update_test_bucket(self, conf_info):
        """
        update the conf bucket with most test info
        """
        bucket = Bucket('couchbase://%s/conf' % self.repo_cluster)

        for info in conf_info:
            conf = info.get('confFile', None)
            component = info.get('component', "")
            subcomponent = info.get('subcomponent', "")
            test_type = info.get('type', "")
            if conf is None:
                continue
            conf = CG.rm_leading_slash(conf)
            tests = info.get('tests', None)
            if tests is None:
                tests = CG.parse_conf_file(conf)
            bucket.upsert(
                conf,
                {'tests': tests,
                 'component': component,
                 'subcomponent': subcomponent,
                 'type': test_type})


    def update_history_bucket(self, conf_info):
        """
        update the history bucket with changes between git repo and conf bucket
        """

        url = 'couchbase://%s' % self.repo_cluster
        history_bucket = Bucket('%s/history' % url)
        test_bucket = Bucket('%s/conf' % url)
        timestamp = CG.timestamp()
        total_new = total_removed = total_changed = 0

        for info in conf_info:
            conf = info.get('confFile', None)
            component = info.get('component', "")
            subcomponent = info.get('subcomponent', "")
            test_type = info.get('type', "")
            if conf is None or conf in self.conf_history:
                # no conf or duplicate
                continue
            else:
                # update conf history
                self.conf_history.append(conf)

            conf = CG.rm_leading_slash(conf)
            doc = self.safe_get_doc(test_bucket, conf)

            # get last known status of tests for this conf
            if doc is not None and doc.rc == 0:
                cb_tests = doc.value.get('tests')
                if cb_tests is not None:
                    repo_tests = info.get('tests', None)
                    if repo_tests is None:
                        repo_tests = [str(t) for t in CG.parse_conf_file(conf)]

                    # array comparison
                    if cb_tests == repo_tests:
                        continue # same same

                    curr_new = curr_removed = curr_changed = 0
                    change_doc = {'ts': timestamp,
                                  'new': [],
                                  'removed': [],
                                  'changed': [],
                                  'component': component,
                                  'subcomponent': subcomponent,
                                  'type': test_type,
                                  'conf': conf}

                    # detect new/removed tests
                    base_repo_tests = CG.split_test_base(repo_tests)
                    base_cb_tests = CG.split_test_base(cb_tests)
                    new_tests = set(base_repo_tests) - set(base_cb_tests)
                    removed_tests = set(base_cb_tests) - set(base_repo_tests)

                    # record which test changed
                    for test in new_tests:
                        # get params
                        params_index = base_repo_tests.index(test)
                        param_parts = repo_tests[params_index].split(",")
                        params = ""
                        if len(param_parts) > 1:
                            params = ",%s" % param_parts[1]
                        test = "%s%s" % (test, params)
                        print "[new] %s" % test
                        change_doc['new'].append(test)
                        curr_new += 1

                    for test in removed_tests:
                        # get params
                        params_index = base_cb_tests.index(test)
                        param_parts = cb_tests[params_index].split(",")
                        params = ""
                        if len(param_parts) > 1:
                            params = ",%s" % param_parts[1]
                        test = "%s%s" % (test, params)
                        print "[removed] %s" % test
                        change_doc['removed'].append(test)
                        curr_removed += 1

                    # detect param changes
                    base_repo_params = [CG.parse_params(t) for t in repo_tests]
                    base_cb_params = [CG.parse_params(t) for t in cb_tests]
                    changed_params = set(base_repo_params) - set(base_cb_params)

                    # record which params changed
                    #     as determined by params in which
                    #     no params have been added or removed
                    for params in changed_params:

                        # given the set comparison
                        # the change will be in base_repo_params
                        change_index = base_repo_params.index(params)
                        changed_test = base_repo_tests[change_index]

                        # check if test already processed
                        if  changed_test in list(new_tests):
                            continue # already processed as new
                        if  changed_test in list(removed_tests):
                            continue # already processed as removed

                        # get original test info and params
                        original_test_index = base_cb_tests.index(changed_test)
                        original_test = cb_tests[original_test_index]
                        original_params = CG.parse_params(original_test)

                        # determine if real change or is just an add
                        original_params_keys = set(
                            CG.split_test_param_keys(original_params))
                        changed_params_keys = set(
                            CG.split_test_param_keys(params))

                        if original_params_keys != changed_params_keys:
                            test_new = "%s,%s" % (changed_test, params)
                            print "[new] %s" % test_new
                            change_doc['new'].append(test_new)
                            curr_new += 1
                            continue

                        # detect which value param actually changed
                        original_params_kv = CG.split_test_params(original_params)
                        changed_params_kv = CG.split_test_params(params)

                        to_diff_params = set(original_params_kv) - set(changed_params_kv)
                        from_diff_params = set(changed_params_kv) - set(original_params_kv)

                        # eval diff
                        param_to_str = ",".join(list(to_diff_params))
                        param_from_str = ",".join(list(from_diff_params))

                        print "[change] %s:  (%s) -> (%s)" % (
                            changed_test,
                            param_to_str,
                            param_from_str)
                        change_doc['changed'].append(
                            {'test': changed_test,
                             'to': param_to_str,
                             'from': param_from_str})
                        curr_changed += 1

                    curr_total = curr_new + curr_removed + curr_changed
                    if  curr_total > 0:
                        # push change
                        timestamp_sec = CG.timestamp_sec()
                        key = "%s:%s_%s" % (component, subcomponent, timestamp_sec)
                        history_bucket.upsert(key, change_doc)
                        total_new += curr_new
                        total_removed += curr_removed
                        total_changed += curr_changed
            else:
                # handle brand new conf
                return

        print "New: %d, Removed: %d, Changed: %d" % (
            total_new,
            total_removed,
            total_changed)

    def safe_get_doc(self, bucket, key):
        """
        get doc and return None when errors occurs

        arguments:
            key -- doc key
        """

        doc = None
        try:
            doc = bucket.get(key)
        except NotFoundError:
            print "ERROR: doc not found: %s" % key
        except CouchbaseNetworkError:
            print "unable to connect to host %s" % self.repo_cluster
        return doc

if __name__ == "__main__":
    main()
