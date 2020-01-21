from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
from couchbase.n1ql import N1QLQuery
import urllib.request, urllib.parse, urllib.error
import json
import argparse
import sys
from string import maketrans
import re
import pdb
import copy
import os
import paramiko


def usage(err=None):
    print("""\
Syntax: triage.py [options]

Options
    --os, -o Operating system version, for example CENTOS 
    --build, -b Server build version, for example 6.5.0-1351
    --component, -c Component name, for example QUERY
    --user, -u Username for coachbase instance
    --password, -p Password for couchbase instance
    --job, -j Job name (optional), for example centos-query_covering-2_moi
    --format, -f, Output format (optional)\n \tc - config file\n \ts - stack trace\n \td - cmd line, ini file, core panic dump file (pretty long operation)
    --ignore, -i, Igrore jobs, comma-separated list (optional)\n
    --restart, -r, Restart specified jobs list\n 

    --help Show this help

Examples:
  python scripts/triage.py --os CENTOS --build 6.5.0-1351 --component QUERY --user username --password password
""")
    sys.exit(0)


def triage(os, build, component, username, password, job_list, format, ignore_list, restart_list, dispatcher_token, server_pool_id, add_pool_id):
    fmt = format
    if format == '' or format is None:
        fmt = 'csd'
    job_name = ''
    ignore_jobs = ''

    connection_error = ['ServerUnavailableException',
                        'unable to reach the host',
                        'This test requires buckets',
                        'ValueError: No JSON object could be decoded']

    if restart_list is not None and restart_list != '':
        execute_restart(restart_list, dispatcher_token, os, build, component, server_pool_id, add_pool_id)
        exit(0)

    if job_list is not None and job_list != '':
        job_arr = job_list.split(",")
        jobs_list = ''
        for j in job_arr:
            jobs_list += ("'"+j.lower()+"'"+',')
        job_name = ' and lower(name) in ['+jobs_list[:len(jobs_list)-1]+'] '
    if ignore_list is not None and ignore_list !='':
        ignore_arr = ignore_list.split(",")
        ignore_jobs_list = ''
        for j in ignore_arr:
            ignore_jobs_list += ("'"+j.lower()+"'"+',')

        ignore_jobs = ' and lower(name) not in ['+ignore_jobs_list[:len(ignore_jobs_list)-1]+'] '


    cluster = Cluster('couchbase://172.23.99.54')
    authenticator = PasswordAuthenticator(username, password)
    cluster.authenticate(authenticator)
    cb = cluster.open_bucket('server')
    query = N1QLQuery("select * from server where os='"+os.upper()+"' and `build`='"+build+"' and result='UNSTABLE' and component='"+component.upper()+"' "+job_name+ignore_jobs+" order by name")
    # centos-query_covering_tokens centos-query_monitoring_moi centos-query_vset-ro-moi centos-query_clusterops_moi centos-query_tokens

    '''
        main data structure to store all jobs fails info. Format is:
        failers[
                {
                 'name': job name,
                 'ini_file': ini file name,
                 'fail_reasons': {
                                    'name': 'connection',
                                    'cases':[
                                             {
                                                name: case name,
                                                conf_file: config file name,
                                                stack_trace: stack trace,
                                                cmd: run command 
                                                dump_file: core panic downloaded file name
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
        response = urllib.request.urlopen(logs_url)

        logs_data = {}
        try:
            logs_data = json.loads(response.read())
        except ValueError as ve:
            a=1

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
        full_job_logs = []
        if 'd' in fmt:
            full_job_logs = download_job_logs(build_id)

            job_logs = extract_cmd_lines(full_job_logs)
            ini_filename = extract_ini_filename(full_job_logs)
            job_dict['ini_file'] = ini_filename

        for suite in suites:
            test_cases = suite['cases']

            '''Case run results parser'''
            for case in test_cases:
                if case['status'] == 'FAILED':
                    case_data = dict()
                    case_attrs = str(case['name'].encode('utf-8')).split(",")
                    case_name = str(case_attrs[0])

                    case_conf_file = ''
                    for s in case_attrs:
                        if "conf_file:" in s:
                            case_conf_file = s.split(":")[1]
                    raw_stack_trace = str(case['errorStackTrace'])
                    new_stack_trace = format_stack_trace(raw_stack_trace)



                    '''Collecting case data'''
                    if 'd' in fmt:
                        json_params = collect_json_params(case_attrs)
                        case_cmd = extract_cmd(case_name, json_params, job_logs)
                        case_data['cmd_line'] = case_cmd
                        if 'new core dump(s) were found and collected. Check testrunner logs folder.' in str(case['errorStackTrace']):
                            core_dump_filename = find_and_download_core_dump(case_cmd, full_job_logs)
                            case_data['dump_file'] = core_dump_filename

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

def execute_restart(restart_list, dispatcher_token, os, build, component, server_pool_id, add_pool_id):
    if restart_list is None or restart_list == '':
        print("Restart list cannot be empty! Please specify comma separated jobs list.")
        exit(1)
    if server_pool_id is None or server_pool_id == '':
        server_pool_id = 'regression'
    if add_pool_id is None or add_pool_id == '':
        add_pool_id = 'None'

    restart_url = "http://qa.sc.couchbase.com/job/test_suite_dispatcher/buildWithParameters?token="+dispatcher_token+"" \
                  "&OS="+os.lower()+"&version_number="+build+"&suite=12hour&component="+component.lower()+"" \
                  "&subcomponent="+restart_list+"&serverPoolId="+server_pool_id+"&addPoolId="+add_pool_id+"&branch=master"
    print(("#### URL ::"+restart_url+"::"))

    response = urllib.request.urlopen(restart_url)
    print(("Restart response - "+str(response)))


def collect_json_params(case_attrs):
    result = {}
    for attr in case_attrs:
        if ':' in attr:
            key = attr[:attr.find(':')]
            val = attr[attr.find(':')+1:]
            result[key] = val
    return result

def find_and_download_core_dump(case_cmd, job_logs):
    #print "CASE NAME ::"+case_cmd+"::"
    case_log_started = False
    crashes_line = ''

    for line in job_logs:
        if './testrunner' in line and case_cmd in line:
            case_log_started = True
        if './testrunner' in line and not case_cmd in line:
            if case_log_started:
                break
        if case_log_started:
            if 'put all crashes on' in line:
                crashes_line = line
                break
    vm_ip = crashes_line[len('put all crashes on '):]
    vm_ip = vm_ip[:vm_ip.find(' ')]
    logs_folder = crashes_line[crashes_line.find('in backup folder: '):].replace('in backup folder: ', '').replace('\n', '')
    local_file_path = download_core_dump(vm_ip, logs_folder)
    return local_file_path

def download_core_dump(vm_ip, logs_folder):
    transport = paramiko.Transport((vm_ip, 22))
    transport.connect(username='root', password='couchbase')
    sftp = paramiko.SFTPClient.from_transport(transport)
    files = sftp.listdir(path=logs_folder)
    if files and len(files)>0:
        sftp.get(logs_folder+'/'+files[0], files[0])
    return files[0]



def download_job_logs(build_id):
    logs = []
    console_page_url = "http://qa.sc.couchbase.com/job/test_suite_executor/" + str(build_id) + "/consoleText"
    console_response = urllib.request.urlopen(console_page_url)
    for line in console_response.readlines():
        logs.append(line)
    return logs

def extract_ini_filename(logs):
    ini_filename = ''
    for line in logs:
        if line.startswith("the ini file is"):
            ini_filename = line[len("the ini file is "):]
            break
    return ini_filename

def extract_cmd_lines(logs):
    lines = []
    for line in logs:
        if line.startswith("./testrunner"):
            lines.append(line)
    return lines

def extract_cmd(case_name, json_params, lines):
    for line in lines:
        if line.find(case_name) > -1:
            rest_of_line = line[line.find(case_name)+len(case_name):]
            if len(rest_of_line) == 0 or rest_of_line[:1] in [' ', ',']:
                params_line = copy.copy(line)
                params_line = params_line[params_line.find(' -p '):]
                params_line = params_line.replace(' -p ', '').replace(' -t ', ',').replace('\n', '')
                params = params_line.split(',')
                params_dict = {}
                for param in params:
                    if '=' in param:
                        key = param[:param.find('=')]
                        val = param[param.find('=')+1:]
                        params_dict[key] = val
                if compare_json_and_cmd_params(json_params, params_dict):
                    cmd = line
                    return cmd[:len(cmd)-1]

def compare_json_and_cmd_params(json_params={}, cmd_params={}):
    #print("JSON ::"+str(json_params)+"::")
    #print("CMD ::"+str(cmd_params)+"::")
    for cmd_key in list(cmd_params.keys()):
        if cmd_key in list(json_params.keys()):
            if cmd_params[cmd_key] != json_params[cmd_key]:
                if cmd_key!='doc-per-day':
                    return False
    return True


def print_report(failers, os, build, component, fmt):
    print(("########################## GREEN BOARD REPORT FOR "+os+"   BUILD "+build+"   COMPONENT - "+component+" #################################"))
    connection_failers = []
    no_logs_failers = []
    all_the_rest_failers = []
    n=1
    connection_jobs_container = ""

    print(str(n) + ". Total list of failed jobs. ")
    for job in failers:
        print((job['name']))

        if len(job['fail_reasons']['connection']['cases']) > 0:
            connection_failers.append(job['name'])
        if len(job['fail_reasons']['no_logs']['cases']) > 0:
            no_logs_failers.append(job['name'])
        if len(job['fail_reasons']['all_the_rest']['cases']) > 0:
            job_data = {}
            if 'ini_file' in list(job.keys()):
                job_data['ini_file'] = job['ini_file']

            job_data['name'] = job['name']
            job_data['cases'] = []
            for case in job['fail_reasons']['all_the_rest']['cases']:
                job_data['cases'].append(case)

            all_the_rest_failers.append(job_data)

    n+=1
    print()

    if len(connection_failers) > 0:
        print(str(n)+". There are some connection issues. ")
        connection_jobs_container = ""
        for job in connection_failers:
            connection_jobs_container = connection_jobs_container + job[job.find('_')+1:] + ','
        print("Jobs list to be restarted:")
        print((connection_jobs_container[:len(connection_jobs_container)-1]))
        n = n+1
        print()

    if len(no_logs_failers) > 0:
        print(str(n)+". There are failed jobs without logs.")
        no_logs_jobs_container = ""
        for job in no_logs_failers:
            no_logs_jobs_container = no_logs_jobs_container + job[job.find('_')+1:] + ','
        print("Jobs list to be restarted:")
        print((no_logs_jobs_container[:len(no_logs_jobs_container)-1]))
        n = n+1
        print()

    if len(all_the_rest_failers) > 0:
        print(str(n)+". There are failed jobs because of different errors.")
        for job in all_the_rest_failers:
            print(("="*100))
            print("JOB - "+str(job['name']))
            if 'ini_file' in list(job.keys()):
                print((".ini file - "+str(job['ini_file'])))
            print()
            for case in job['cases']:
                print(("     CASE NAME - "+str(case['name'])))
                print()
                if 'c' in fmt:
                    print(("         config file - "+str(case['conf_file'])))
                    print()
                if 's' in fmt:
                    print("         stack trace - ")
                    print((""+str(case['stack_trace'])))
                    print(("-"*100))
                if 'd' in fmt:
                    print()
                    if str(case['cmd_line']) == 'None':
                        print('         cmd line - None. If you see this, I could not identify cmd line for this test case.')
                        print('         Please send email to evgeny.makarenko@couchbase.com with subject \'Triage script - cmd error\',')
                        print('         containing component name, job name, build version, and test case name. I\'ll fix it immediately.')
                    else:
                        print(("         cmd line - "+str(case['cmd_line'])))
                    if 'dump_file' in list(case.keys()):
                        print(("         dump file - "+str(case['dump_file'])))
                print()
            print(("=" * 100))
        n = n+1
        print()


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
    parser.add_argument('--job', '-j', help='Job names list (optional)', type=str)
    parser.add_argument('--format', '-f', help='Output format (optional)\n c - config file\n s - stack trace\n d - cmd line, ini file, core panic dump file (pretty long operation)', type=str)
    parser.add_argument('--ignore', '-i', help='Igrore jobs, comma-separated list (optional)\n ', type=str)
    parser.add_argument('--restart', '-r', help='Restart jobs automatically, comma-separated list (optional).', type=str)
    parser.add_argument('--dispatcher_token', '-t', help='Test suite dispatcher security token.', type=str)
    parser.add_argument('--server_pool_id', '-l', help='Test suite dispatcher server pool id (optional). Default value is \'regression\'', type=str)
    parser.add_argument('--add_pool_id', '-a', help='Test suite dispatcher additional pool id (optional). Default value is \'None\'', type=str)

    args = parser.parse_args()
    os = args.os
    build = args.build
    component = args.component
    username = args.user
    password = args.password
    job_list = args.job
    format = args.format
    ignore_list = args.ignore
    restart_list = args.restart
    dispatcher_token = args.dispatcher_token
    server_pool_id = args.server_pool_id
    add_pool_id=args.add_pool_id

    if restart_list is None:
        if os is None or os == '' or build is None or build == '' or component is None or component == '' \
            or username is None or username == '' or password is None or password == '':
            usage()
    else:
        if os is None or os == '' or build is None or build == '' or component is None or component == '' \
            or dispatcher_token is None or dispatcher_token == '' or restart_list is None or restart_list == '':
            usage()

    triage(os, build, component, username, password, job_list, format, ignore_list, restart_list, dispatcher_token, server_pool_id, add_pool_id)



