from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
from couchbase.n1ql import N1QLQuery
import urllib
import json
import argparse
import sys


def usage(err=None):
    print """\
Syntax: triage.py [options]

Examples:
  python pytests/green_board/triage.py --os CENTOS --build 6.5.0-1351 --component QUERY --user username --password password
"""
    sys.exit(0)

def triage(os, build, component, username, password):

    connection_error = ['ServerUnavailableException', 'unable to reach the host']

    cluster = Cluster('couchbase://172.23.99.54')
    authenticator = PasswordAuthenticator(username, password)
    cluster.authenticate(authenticator)
    cb = cluster.open_bucket('server')
    query = N1QLQuery("select * from server where os='"+os.upper()+"' and `build`='"+build+"' and result='UNSTABLE' and component='"+component.upper()+"'")

    for row in cb.n1ql_query(query):
        build_id = row['server']['build_id']
        job_name = row['server']['name']

        logs_url = "http://qa.sc.couchbase.com/job/test_suite_executor/"+str(build_id)+"/testReport/api/json"
        response = urllib.urlopen(logs_url)
        logs_data = json.loads(response.read())
        if len(logs_data['suites']) == 0:
            continue
        test_cases = logs_data['suites'][0]['cases']
        job_printed = False
        case_counter = -1
        for case in test_cases:
            case_counter+=1
            if case['status'] == 'FAILED':
                stack_trace = case['errorStackTrace']
                for err in connection_error:
                    if err in stack_trace:
                        case_name = str(case['name'])
                        cname = str(case_name.split(',')[0])

                        console_page_url = "http://qa.sc.couchbase.com/job/test_suite_executor/"+str(build_id)+"/consoleText"
                        console_response = urllib.urlopen(console_page_url)
                        if not job_printed:
                            print("JOB NAME " + job_name)
                            job_printed = True
                        print("CASE NAME " + str(case_name.split(',')[0]))

                        case_log = ''
                        case_log_started = False
                        cmd = ''
                        for line in console_response.readlines():
                            if case_log_started:
                                case_log = case_log+line

                            if line.find(cname)>-1 and line.startswith("./testrunner"):
                                case_log_started = True
                                cmd = line
                            elif line.startswith("./testrunner"):
                                case_log_started = False
                                for error in connection_error:
                                    if error in case_log:
                                        print("CMD :: "+cmd)
                                        break
                                case_log = ''

                        if len(case_log) > 0:
                            for error in connection_error:
                                if error in case_log:
                                    print("CMD :: " + cmd)
                                    break
                        break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--os', '-o', help="Operating system version", type=str)
    parser.add_argument('--build', '-b', help="Server build version", type=str)
    parser.add_argument('--component', '-c', help='Component name', type=str)
    parser.add_argument('--user', '-u', help='Username', type=str)
    parser.add_argument('--password', '-p', help='Password', type=str)

    args = parser.parse_args()
    os = args.os
    build = args.build
    component = args.component
    username = args.user
    password = args.password

    if os == None or os == '' or build == None or build == '' or component == None or component == '' \
            or username == None or username == '' or password == None or password == '':
        usage()

    triage(os, build, component, username, password)
