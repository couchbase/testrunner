import os
import sys
from optparse import OptionParser
sys.path = [".", "lib"] + sys.path
import testcfg as cfg
from membase.api.rest_client import RestConnection
import plotter
import store_report
import compare_stats

CBFS_HOST = 'http://cbfs.hq.couchbase.com:8484'

def parse_args():
    parser = OptionParser()
    parser.add_option("-c", "--cluster", dest="cluster_name", help="cluster name; not required, it's taken from testcfg(CB_CLUSTER_TAG), if not specified")
    parser.add_option("-b", "--buckets", dest="buckets", help="list of buckets; not required, it's taken all buckets from the cluster if it is alive")
    parser.add_option("-v", "--version", dest="version", help="build version;  not required, it's taken from seriesly DB by tag 'build'")
    parser.add_option("-V", "--version_to_compare", dest="version_to_compare", help="build version to compare; not required, comparative part will be omitted in this case")
    parser.add_option("-r", "--release_number", dest="release_number", help="release number; required")
    parser.add_option("-R", "--release_to_compare", dest="release_to_compare", help="release number for comparison; not required, comparative part will be omitted in this case")
    parser.add_option("-p", "--platform", dest="platform", help="platform version; required")
    parser.add_option("-t", "--test_name", dest="test_name", help="test name; not required, it's taken from seriesly DB by tag 'name'")

    options, args = parser.parse_args()

    if None in [options.release_number, options.platform]:
        parser.print_help()
        sys.exit()

    return options, args


def main():
    options, args = parse_args()

    cluster_name = options.cluster_name
    if options.cluster_name is None:
        cluster_name = cfg.CB_CLUSTER_TAG

    buckets = options.buckets
    if buckets is None:
        for ip in cfg.CLUSTER_IPS:
            if ":" in ip:
                port = ip[ip.find(":") + 1:]
                ip = ip[0:ip.find(":")]
            else:
                port = '8091'
            try:
                serverInfo = {'ip':ip, 'username':cfg.COUCHBASE_USER, 'password':cfg.COUCHBASE_PWD, 'port':port}
                rest = RestConnection(serverInfo)
                buckets = [bucket.name for bucket in rest.get_buckets()]
                break
            except Exception, e:
                print e

    release_number = options.release_number
    platform = options.platform

    test_name = options.test_name
    if test_name == None:
        test_name = store_report.get_run_info('name')
    test_build = options.version
    if test_build == None:
        test_build = store_report.get_run_info('build')

    storage_folder = plotter.plot_all_phases(cluster_name, buckets)

    if None in [options.release_to_compare, options.version_to_compare]:
        if options.release_to_compare is None:
            print "release_to_compare was not set"
        if options.version_to_compare is None:
            print "version_to_compare was not set"
        print 'Comparison of versions to be skipped'
    else:
        release_to_compare = options.release_to_compare
        version_to_compare = options.version_to_compare
        folders = os.walk(storage_folder).next()[1]
        for folder in folders:
            sub_folder = os.path.join(storage_folder, folder)
            url_folder = "system-test-results/%s/%s/%s/%s/%s" % (release_to_compare, platform, test_name, version_to_compare, folder)
            if not os.path.exists(url_folder):
                os.makedirs(url_folder)
            files = [ f for f in os.listdir(sub_folder) if os.path.isfile(os.path.join(sub_folder, f))]
            for f in files:
                os.system("wget %s/%s/%s -O %s/%s" % (CBFS_HOST, url_folder, f, url_folder, f))

        compare_stats.compare_by_folders(storage_folder, "system-test-results/%s/%s/%s/%s" % (release_to_compare, platform, test_name, version_to_compare))

    store_report.store_report_cbfs(release_number, platform)

if __name__ == "__main__":
    main()
