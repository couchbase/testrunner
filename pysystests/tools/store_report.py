import sys
import os
from optparse import OptionParser
sys.path.append(".")
import testcfg as cfg
from seriesly import Seriesly

CBFS_HOST = 'http://cbfs.hq.couchbase.com:8484'

def parse_args():
    """Parse CLI arguments"""
    usage = """
         %prog release_number platform test_name test_build

Example: python tools/store_report.py 2.1.1 linux
         python tools/store_report.py 2.1.1 linux kv_only build_171_run_1

Note:    test_name test_desc are optional if we can retrieve it from event database"""

    parser = OptionParser(usage)
    options, args = parser.parse_args()

    if len(args) < 2:
        parser.print_help()
        sys.exit()

    return options, args

def get_run_info(desc):
    db_event = Seriesly(cfg.SERIESLY_IP, 3133)['event']

    all_event_docs = db_event.get_all()
    phases_info = {}
    for doc in all_event_docs.itervalues():
        phases_info[int(doc.keys()[0])] = doc.values()[0]
    phases_info.keys().sort()

    run_info = ''
    if desc == 'name':
        run_info = phases_info[1]['name']
    if desc == 'build':
        run_info = phases_info[1]['desc']

    run_info = run_info.replace(" ", "_")
    run_info = run_info.replace(",", "_")

    return run_info


def store_report_cbfs(release_number, platform, test_name='', test_build=''):
    if test_name == '':
        test_name = get_run_info('name')
    if test_build == '':
        test_build = get_run_info('build')

    if not os.path.exists("system-test-results/%s/%s/%s" % (release_number, platform, test_name)):
        os.makedirs("system-test-results/%s/%s/%s" % (release_number, platform, test_name))

    os.system('cp -rf %s system-test-results/%s/%s/%s/' % (test_build, release_number, platform, test_name))

    print "Upload test report to system-test-results/%s/%s/%s/%s folder on CBFS" % (release_number, platform, test_name, test_build)

    os.system('find system-test-results/%s/%s/%s/%s -name "*.txt" or -name "*.html" or -name "*.json" -print0 | xargs -0 -I file curl -X PUT -H \"Content-Type:text/plain\" -v --data-binary @file %s/file' % (release_number, platform, test_name, test_build, CBFS_HOST))

    os.system('find system-test-results/ -name "*.pdf" -print0 | xargs -0 -I file curl -X PUT -H \"Content-Type:application/pdf\" -v --data-binary @file %s/file' % CBFS_HOST)


def main():
    options, args = parse_args()
    release_number = args[0]
    platform = args[1]
    test_name = ''
    test_build = ''
    if len(args) > 2:
        test_name = args[2]
        test_build = args[3]

    store_report_cbfs(release_number, platform, test_name, test_build)

if __name__ == "__main__":
    main()
