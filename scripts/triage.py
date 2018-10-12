from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
from couchbase.n1ql import N1QLQuery
import urllib
import json
import argparse
import sys
from string import maketrans
import re
import pdb


def usage(err=None):
    print """\
Syntax: triage.py [options]

Options
    --os, -o Operating system version, for example CENTOS 
    --build, -b Server build version, for example 6.5.0-1351
    --component, -c Component name, for example QUERY
    --user, -u Username for coachbase instance
    --password, -p Password for couchbase instance
    --job, -j Job name (optional), for example centos-query_covering-2_moi
    --format, -f, Output format (optional)\n \tc - config file\n \ts - stack trace\n \td - cmd line (pretty long operation)
    --help Show this help

Examples:
  python scripts/triage.py --os CENTOS --build 6.5.0-1351 --component QUERY --user username --password password
"""
    sys.exit(0)


def triage(os, build, component, username, password, job, format):
    fmt = format
    if format == '':
        fmt = 'csd'

    connection_error = ['ServerUnavailableException', 'unable to reach the host']
    cbq_error = ['No such prepared statement', 'Create index cannot proceed', 'No index available', 'RebalanceFailedException']
    assertion_error = ['AssertionError']
    dictionary_error = ["TypeError:", "KeyError:", "IndexError"]
    general_cbq_error = ['CBQError']
    json_error = ['No JSON object could be decoded']
    job_name = ''
    if job is not None and job != '':
        job_name = " and lower(name)='"+job.lower()+"'"


    cluster = Cluster('couchbase://172.23.99.54')
    authenticator = PasswordAuthenticator(username, password)
    cluster.authenticate(authenticator)
    cb = cluster.open_bucket('server')
    query = N1QLQuery("select * from server where os='"+os.upper()+"' and `build`='"+build+"' and result='UNSTABLE' and component='"+component.upper()+"' "+job_name)
    # centos-query_covering_tokens centos-query_monitoring_moi centos-query_vset-ro-moi centos-query_clusterops_moi centos-query_tokens

    '''
        main data structure to store all jobs fails info. Format is:
        failers[
                {
                 'name': job name
                 'fail_reasons': {
                                    'name': 'connection',
                                    'cases':[
                                             {
                                                name: case name,
                                                conf_file: config file name,
                                                stack_trace: stack trace,
                                                cmd: run command 
                                             }        
                                            ]
                                    },
                                    {
                                     'name': 'no_logs',
                                     'cases': []
                                    },
                                     'name': 'all_the_rest',
                                     'cases': []               
                 },
                 {
                 }
                ]
    '''
    failers = []

    for row in cb.n1ql_query(query):
        build_id = row['server']['build_id']
        job_name = row['server']['name']

        '''Constructing skeleton of main data structure'''
        job_dict = {}
        job_dict['name'] = job_name
        job_dict['fail_reasons'] = {}

        fail_reason_connection = dict()
        fail_reason_connection['name'] = 'connection'
        fail_reason_connection['cases'] = []
        job_dict['fail_reasons']['connection'] = fail_reason_connection

        fail_reason_no_logs = dict()
        fail_reason_no_logs['name'] = 'no_logs'
        fail_reason_no_logs['cases'] = []
        job_dict['fail_reasons']['no_logs'] = fail_reason_no_logs

        fail_reason_all_the_rest = dict()
        fail_reason_all_the_rest['name'] = 'all_the_rest'
        fail_reason_all_the_rest['cases'] = []
        job_dict['fail_reasons']['all_the_rest'] = fail_reason_all_the_rest

        '''Load json logs for last job call'''
        logs_url = "http://qa.sc.couchbase.com/job/test_suite_executor/"+str(build_id)+"/testReport/api/json"
        response = urllib.urlopen(logs_url)

        logs_data = {}
        try:
            logs_data = json.loads(response.read())
        except ValueError, ve:
            print("No logs for job - "+str(job_name))

        if (str(logs_data)) == '{}':
            job_dict['fail_reasons']['no_logs']['cases'].append('no_logs_case')
            failers.append(job_dict)
            continue
        if (len(logs_data['suites'])) == 0:
            job_dict['fail_reasons']['no_logs']['cases'].append('no_logs_case')
            failers.append(job_dict)
            continue
        suites = logs_data['suites']

        '''Load full text logs for last job call'''
        job_logs = []
        if 'd' in fmt:
            job_logs = load_job_logs(build_id)

        for suite in suites:
            test_cases = suite['cases']

            '''Case run results parser'''
            for case in test_cases:
                if case['status'] == 'FAILED':
                    case_data = dict()

                    case_attrs = str(case['name']).split(",")
                    case_name = str(case_attrs[0])

                    case_conf_file = ''
                    for s in case_attrs:
                        if "conf_file:" in s:
                            case_conf_file = s.split(":")[1]
                    raw_stack_trace = str(case['errorStackTrace'])
                    new_stack_trace = format_stack_trace(raw_stack_trace)

                    '''Collecting case data'''
                    if 'd' in fmt:
                        case_cmd = extract_cmd(case_name, job_logs)
                        case_data['cmd_line'] = case_cmd

                    case_data['name'] = case_name
                    case_data['conf_file'] = case_conf_file
                    case_data['stack_trace'] = new_stack_trace

                    '''Identifying fail reason'''
                    problem_not_identified = True
                    for err in connection_error:
                        if problem_not_identified and err in raw_stack_trace:
                            job_dict['fail_reasons']['connection']['cases'].append(case_data)
                            problem_not_identified = False
                            break

                    if problem_not_identified:
                        job_dict['fail_reasons']['all_the_rest']['cases'].append(case_data)

        failers.append(job_dict)

    print_report(failers, os, build, component, fmt)


def load_job_logs(build_id):
    lines = []
    console_page_url = "http://qa.sc.couchbase.com/job/test_suite_executor/" + str(build_id) + "/consoleText"
    console_response = urllib.urlopen(console_page_url)
    for line in console_response.readlines():
        if line.startswith("./testrunner"):
            lines.append(line)
    return lines


def extract_cmd(case_name, lines):
    for line in lines:
        if line.find(case_name) > -1:
            cmd = line
            return cmd[:len(cmd)-1]


def print_report(failers, os, build, component, fmt):
    print("########################## GREEN BOARD REPORT FOR "+os+"   BUILD "+build+"   COMPONENT - "+component+" #################################")
    connection_failers = []
    no_logs_failers = []
    all_the_rest_failers = []
    n=1

    for job in failers:
        if len(job['fail_reasons']['connection']['cases']) > 0:
            connection_failers.append(job['name'])
        if len(job['fail_reasons']['no_logs']['cases']) > 0:
            no_logs_failers.append(job['name'])
        if len(job['fail_reasons']['all_the_rest']['cases']) > 0:
            job_data = {}
            job_data['name'] = job['name']
            job_data['cases'] = []
            for case in job['fail_reasons']['all_the_rest']['cases']:
                job_data['cases'].append(case)

            all_the_rest_failers.append(job_data)

    if len(connection_failers) > 0:
        print str(n)+". There are some connection issues. "
        connection_jobs_container = ""
        for job in connection_failers:
            connection_jobs_container = connection_jobs_container + job[job.find('_')+1:] + ','
        print("Jobs list to be restarted:")
        print(connection_jobs_container[:len(connection_jobs_container)-1])
        n = n+1
        print

    if len(no_logs_failers) > 0:
        print str(n)+". There are failed jobs without logs."
        no_logs_jobs_container = ""
        for job in no_logs_failers:
            no_logs_jobs_container = no_logs_jobs_container + job[job.find('_')+1:] + ','
        print("Jobs list to be restarted:")
        print(no_logs_jobs_container[:len(no_logs_jobs_container)-1])
        n = n+1
        print

    if len(all_the_rest_failers) > 0:
        print str(n)+". There are failed jobs because of different errors."
        for job in all_the_rest_failers:
            print("="*100)
            print "JOB - "+job['name']
            print
            for case in job['cases']:
                print("     CASE NAME - "+case['name'])
                print
                if 'c' in fmt:
                    print("         config file - "+case['conf_file'])
                    print
                if 's' in fmt:
                    print("         stack trace - ")
                    print(""+case['stack_trace'])
                    print("-"*100)
                if 'd' in fmt:
                    print
                    print("         cmd line - "+case['cmd_line'])
                print
            print("=" * 100)
        n = n+1
        print


def format_stack_trace(raw_stack_trace):
    new_stack_trace = ''
    for i in range(len(raw_stack_trace) - 1):
        if raw_stack_trace[i] == '\\':
            if raw_stack_trace[i + 1] == 'n':
                new_stack_trace = new_stack_trace + '\n'
                i = i + 1
            else:
                new_stack_trace = new_stack_trace + raw_stack_trace[i]
        else:
            if not (raw_stack_trace[i - 1] == '\\' and raw_stack_trace[i] == 'n'):
                new_stack_trace = new_stack_trace + raw_stack_trace[i]

    return new_stack_trace


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--os', '-o', help="Operating system version", type=str)
    parser.add_argument('--build', '-b', help="Server build version", type=str)
    parser.add_argument('--component', '-c', help='Component name', type=str)
    parser.add_argument('--user', '-u', help='Username', type=str)
    parser.add_argument('--password', '-p', help='Password', type=str)
    parser.add_argument('--job', '-j', help='Job name (optional)', type=str)
    parser.add_argument('--format', '-f', help='Output format (optional)\n c - config file\n s - stack trace\n d - cmd line (pretty long operation)', type=str)

    args = parser.parse_args()
    os = args.os
    build = args.build
    component = args.component
    username = args.user
    password = args.password
    job = args.job
    format = args.format

    if os is None or os == '' or build is None or build == '' or component is None or component == '' \
            or username is None or username == '' or password is None or password == '':
        usage()

    triage(os, build, component, username, password, job, format)
