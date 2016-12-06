"""Test repo reporting tool

This script will query the qe test suite couchbase cluster for conf files.
It then reads the tests within each conf file from the local filesystem which
may contain any changes made by a recent git commit.  Changes are detected by
comparing the tests from local git repo with tests stored in remote couchbase
bucket.  Changes are recorded and stored into a history bucket and then the
remote couchbase server is updated.
"""

import argparse
from common import Generics as CG
from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
from couchbase.exceptions import NotFoundError, CouchbaseNetworkError


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
    args = parser.parse_args()

    repo_manager = TestRepoManager(
        args.repo_cluster,
        args.qe_cluster,
        args.qe_bucket)

    # create history docs for today
    repo_manager.update_history_bucket()

    # push update conf file with latest changes
    repo_manager.update_conf_bucket()


class TestRepoManager(object):
    """
    TestRepoManager performs test comparisions, pushes updates to history bucket
    and updates source repo bucket
    """

    def __init__(self, repo_cluster, qe_cluster, qe_bucket):
        self.repo_cluster = repo_cluster
        self.qe_cluster = qe_cluster
        self.qe_bucket = qe_bucket


    def get_conf_files(self):
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
            rows.append(row)
        return rows

    def update_conf_bucket(self):
        """
        update the conf bucket with most test info
        """

        bucket = Bucket('couchbase://%s/conf' % self.repo_cluster)
        conf_info = self.get_conf_files()

        for info in conf_info:
            conf = info.get('confFile', None)
            component = info.get('component', "")
            subcomponent = info.get('subcomponent', "")
            if conf is None:
                continue
            conf = CG.rm_leading_slash(conf)
            print conf
            tests = CG.parse_conf_file(conf)
            bucket.upsert(
                conf,
                {'tests': tests,
                 'component': component,
                 'subcomponent': subcomponent})


    def update_history_bucket(self):
        """
        update the history bucket with changes between git repo and conf bucket
        """

        url = 'couchbase://%s' % self.repo_cluster
        history_bucket = Bucket('%s/history' % url)
        conf_bucket = Bucket('%s/conf' % url)
        timestamp = CG.timestamp()

        # get all config files
        conf_info = self.get_conf_files()
        for info in conf_info:
            conf = info.get('confFile', None)
            component = info.get('component', "")
            subcomponent = info.get('subcomponent', "")
            if conf is None:
                continue
            conf = CG.rm_leading_slash(conf)

            doc = self.safe_get_doc(conf_bucket, conf)

            # get last known status of tests for this conf
            if doc is not None and doc.rc == 0:
                cb_tests = doc.value.get('tests')
                if cb_tests is not None:
                    repo_tests = [str(t) for t in CG.parse_conf_file(conf)]

                    # array comparison
                    if cb_tests == repo_tests:
                        continue # same same

                    change_doc = {'ts': timestamp,
                                  'new': [],
                                  'removed': [],
                                  'changed': [],
                                  'component': component,
                                  'subcomponent': subcomponent,
                                  'conf': conf}

                    # detect new/removed tests
                    base_repo_tests = [t.split(',')[0] for t in repo_tests]
                    base_cb_tests = [t.split(',')[0] for t in cb_tests]
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

                    # push change
                    key = "%s_%s:%s" % (timestamp, component, subcomponent)
                    history_bucket.upsert(key, change_doc)
            else:
                # handle brand new conf
                return

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
