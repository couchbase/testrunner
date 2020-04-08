import os
import sys
from optparse import OptionParser
sys.path = [".", "lib"] + sys.path
import testcfg as cfg
from membase.api.rest_client import RestConnection
import plotter
import store_report
import compare_stats
import json
from seriesly import Seriesly

CBFS_HOST = 'http://cbfs.hq.couchbase.com:8484'

index_html = """<html xmlns="http://www.w3.org/1999/xhtml">
    <head>
      <meta http-equiv="content-type" content="text/html; charset=utf-8" />
      <title>index.html file</title>
      </head>
    <body style="font-family: Arial;border: 0 none;">
      %s
      </body>
    </html>"""



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
    parser.add_option("-f", "--json_test_file", dest="json_test_file", help="json test file; required")

    options, args = parser.parse_args()

    if None in [options.release_number, options.platform, options.json_test_file]:
        parser.print_help()
        sys.exit()

    if not os.path.exists(options.json_test_file):
            sys.exit("json file {0} was not found".format(options.json_test_file))

    return options, args


def generate_index_file(storage_folder, test_file):
    db_event = Seriesly(cfg.SERIESLY_IP, 3133)['event']
    all_event_docs = db_event.get_all()
    phases_info = {}
    for doc in all_event_docs.values():
        phases_info[int(list(doc.keys())[0])] = list(doc.values())[0]
    list(phases_info.keys()).sort()
    num_phases = len(list(phases_info.keys()))
    run_id = phases_info[1]['desc']
    run_id = run_id.replace(" ", "_")
    run_id = run_id.replace(",", "_")
    content = ""

    json_data = open(test_file)
    tests = json.load(json_data)

    for i in range(num_phases)[1:]:
        sub_folder = storage_folder + "phase" + str(i) + "/"
        content += "<a style=\"font-family:arial;color:black;font-size:20px;\" href=\"%s\">%s</a><p>" % ("phase" + str(i), "phase" + str(i))
        if str(i) in tests["phases"]:
            content += json.dumps(tests["phases"][str(i)], indent=10, sort_keys=True) + "<p>"
        files = [ f for f in os.listdir(sub_folder) if os.path.isfile(os.path.join(sub_folder, f))]
        for f in files:
            content += "<a href=\"%s\">&nbsp;&nbsp;&nbsp;&nbsp;%s</a><p>" % ("phase" + str(i) + "/" + f, f)

    html_path = storage_folder + "index.html"
    file1 = open(html_path, 'w')
    file1.write(index_html % content)




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
            except Exception as e:
                print(e)

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
            print("release_to_compare was not set")
        if options.version_to_compare is None:
            print("version_to_compare was not set")
        print('Comparison of versions to be skipped')
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
        generate_index_file(storage_folder, options.json_test_file)
    store_report.store_report_cbfs(release_number, platform)



if __name__ == "__main__":
    main()
