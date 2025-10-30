import base64
import configparser
import ipaddress
import json
import logging
import os as OS
import subprocess
import time
import traceback
import urllib.error
import urllib.parse
import urllib.request
from copy import deepcopy
from optparse import OptionParser
from uuid import uuid4

import paramiko
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions

import capella
import cloud_provision
import find_rerun_job
import httplib2
from server_manager import ServerManager
import get_jenkins_params
from table_view import TableView

# takes an ini template as input, standard out is populated with the server pool
# need a descriptor as a parameter
# need a timeout param

POLL_INTERVAL = 60

TEST_SUITE_DB = '172.23.105.178'
TEST_SUITE_DB_USER_NAME = 'Administrator'
TEST_SUITE_DB_PASSWORD = "esabhcuoc"
TIMEOUT = 120
SSH_NUM_RETRIES = 3
SSH_POLL_INTERVAL = 20
QE_SERVER_POOL_BUCKET = 'QE-server-pool'

DOCKER = "DOCKER"
AWS = "AWS"
AZURE = "AZURE"
GCP = "GCP"
KUBERNETES = "KUBERNETES"
VM = "VM"
CAPELLA_LOCAL = "CAPELLA_LOCAL"
SERVERLESS_ONCLOUD = "SERVERLESS_ONCLOUD"
PROVISIONED_ONCLOUD = "PROVISIONED_ONCLOUD"
ELIXIR_ONPREM = "ELIXIR_ONPREM"
ON_PREM_PROVISIONED = "ON_PREM_PROVISIONED"
SERVERLESS_COLUMNAR = "SERVERLESS_COLUMNAR"

CLOUD_SERVER_TYPES = [AWS, AZURE, GCP, SERVERLESS_ONCLOUD,
                      PROVISIONED_ONCLOUD, SERVERLESS_COLUMNAR]

DEFAULT_ARCHITECTURE = "x86_64"
DEFAULT_SERVER_TYPE = VM
log = logging.getLogger()
Server_Manager_Obj = None


def sleep_function(seconds, message=None):
    log.info(f"Sleeping for {seconds} seconds. Reason: {message}")
    time.sleep(seconds)


# Configure logging with dynamic log level
def setup_logging(log_level):
    """Setup logging with configurable log level."""
    log_format = '%(asctime)s: %(funcName)s:L%(lineno)d: %(levelname)s: %(message)s'
    log_level = getattr(logging, str(log_level).upper())

    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt='%Y-%m-%d %H:%M:%S')


def getNumberOfServers(iniFile):
    f = open(iniFile)
    contents = f.read()
    f.close()
    return contents.count('dynamic')


def getNumberOfAddpoolServers(iniFile, addPoolId):
    f = open(iniFile)
    contents = f.read()
    f.close()
    try:
        return contents.count(addPoolId)
    except Exception as e:
        log.error(f"Error in getNumberOfAddpoolServers: {str(e)}")
        return 0


def get_ssh_username_password(ini_file_name):
    u_name, p_word = None, None
    for line in open(ini_file_name).readlines():
        if u_name is None and "username:" in line:
            arr = line.split(":")[1:]
            u_name = arr[0].rstrip()
        if p_word is None and "password:" in line:
            arr = line.split(":")[1:]
            p_word = arr[0].rstrip()

        if u_name and p_word:
            break
    return u_name, p_word


def rreplace(str, pattern, num_replacements):
    return str.rsplit(pattern, num_replacements)[0]


def get_available_servers_count(options=None, is_addl_pool=False,
                                os_version="", pool_id=None):
    if options.serverType == DOCKER:
        count = Server_Manager_Obj.get_available_count(os_type=os_version,
                                                       pool_id=pool_id,
                                                       docker=True)
    else:
        count = Server_Manager_Obj.get_available_count(os_type=os_version,
                                                       pool_id=pool_id)

    count = int(count)
    if count == 0:
        sleep_function(POLL_INTERVAL, 'no VMs')
    return count


def get_servers_cloud(options, descriptor, how_many, is_addl_pool, os_version,
                      pool_id):
    descriptor = urllib.parse.unquote(descriptor)
    type = "couchbase"
    if is_addl_pool:
        type = pool_id
    if options.serverType == AWS:
        ssh_key_path = OS.environ.get("AWS_SSH_KEY")
        return cloud_provision.aws_get_servers(
            descriptor, how_many, os_version, type, ssh_key_path,
            options.architecture), None
    elif options.serverType == GCP:
        ssh_key_path = OS.environ.get("GCP_SSH_KEY")
        return cloud_provision.gcp_get_servers(
            descriptor, how_many, os_version, type, ssh_key_path,
            options.architecture), None
    elif options.serverType == AZURE:
        """ Azure uses template with pw enable login.  No need key """
        ssh_key_path = ""
        return cloud_provision.az_get_servers(
            descriptor, how_many, os_version, type, ssh_key_path,
            options.architecture)
    elif options.serverType == SERVERLESS_ONCLOUD:
        return [], []
    elif options.serverType == PROVISIONED_ONCLOUD:
        return [], []
    elif options.serverType == SERVERLESS_COLUMNAR:
        return [], []


def get_servers(options=None, descriptor="", test=None, how_many=0,
                is_addl_pool=False, os_version="", pool_id=None):
    if options.serverType in CLOUD_SERVER_TYPES:
        return get_servers_cloud(options, descriptor, how_many, is_addl_pool,
                                 os_version, pool_id)

    if options.serverType == DOCKER:
        ip_list = Server_Manager_Obj.get_dockers(
            username=descriptor,
            count=how_many,
            pool_id=pool_id)
    else:
        ip_list = Server_Manager_Obj.get_servers(
            username=descriptor,
            count=how_many,
            os_type=os_version,
            expires_in=test['timeLimit'],
            pool_id=pool_id,
            dont_reserve=False)
    return ip_list, None


def check_servers_via_ssh(servers=[], test=None):
    alive_servers = []
    bad_servers = []
    for server in servers:
        if is_vm_alive(
                server=server,
                ssh_username=test['ssh_username'],
                ssh_password=test['ssh_password']):
            alive_servers.append(server)
        else:
            bad_servers.append(server)
    return alive_servers, bad_servers


def is_vm_alive(server="", ssh_username="", ssh_password=""):
    try:
        if '[' in server or ']' in server:
            server = server.replace('[', '')
            server = server.replace(']','')
        _ = ipaddress.ip_address(server).version
    except ValueError as e:
        log.critical(f"Invalid address for {server}: {str(e)}")
        return False
    num_retries = 1
    while num_retries <= SSH_NUM_RETRIES:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(
                hostname=server,
                username=ssh_username,
                password=ssh_password)
            ssh.close()
            log.info(f"SSH to {server} successful")
            return True
        except Exception as e:
            num_retries = num_retries + 1
            sleep_function(
                SSH_POLL_INTERVAL,
                f"Exception occured while trying to ssh {server}: {str(e)}")
            continue

    log.critical(f"{server} is unreachable. Tried {num_retries-1} times")
    return False


def flatten_param_to_str(value):
    """
    Convert dict/list -> str
    """
    result = ""
    if isinstance(value, dict):
        result = '{'
        for key, val in value.items():
            if isinstance(val, dict) or isinstance(val, list):
                result += flatten_param_to_str(val)
            else:
                try:
                    val = int(val)
                except ValueError:
                    val = '\"%s\"' % val
                result += '\"%s\":%s,' % (key, val)
        result = result.rstrip(",") + '}'
    elif isinstance(value, list):
        result = '['
        for val in value:
            if isinstance(val, dict) or isinstance(val, list):
                result += flatten_param_to_str(val)
            else:
                result += '"%s",' % val
        result = result.rstrip(",") + ']'
    return result


def extract_individual_tests_from_query_result(col_rel_version,
                                               query_result_row):
    def sub_component_is_supported_in_this_release(data_from_db_doc):
        if 'implementedIn' in data_from_db_doc \
                and RELEASE_VERSION < float(data_from_db_doc['implementedIn']):
            # Return empty list since feature not supported in this version
            print(
                f"{unsupported_feature_msg}:{data_from_db_doc['subcomponent']}")
            return False

        if 'maxVersion' in data_from_db_doc:
            if RELEASE_VERSION > float(data_from_db_doc['maxVersion']) \
                    or (col_rel_version
                        and col_rel_version > float(
                            data_from_db_doc['maxVersion'])):
                # Return empty list since feature not supported in this version
                print(f"{unsupported_feature_msg}:{data_from_db_doc['subcomponent']}")
                return False
        return True

    def populate_required_dispatcher_data(data_from_db_doc,
                                          sub_comp_dict_to_update):
        mixed_build_config = dict()
        if data["component"] == "columnar":
            if 'mixed_build_config' in data_from_db_doc:
                mixed_build_config = data_from_db_doc["mixed_build_config"]

        # Update dict with required values
        sub_comp_dict_to_update["subcomponent"] = data_from_db_doc['subcomponent']
        if "parameters" in data_from_db_doc:
            sub_comp_dict_to_update["parameters"] = data_from_db_doc['parameters']
        sub_comp_dict_to_update["mixed_build_config"] = urllib.parse.quote(
            flatten_param_to_str(mixed_build_config))

    # CBQE-8380
    global options, RELEASE_VERSION, REPO_PULLED

    # Having 'test_jobs_list' as list for the following cases:
    # Case 1: Simple dict where component/subcomponent resides in direct dict
    #         (Traditional way)
    # Case 2: subcomponents resides as list where single confFile maps to
    #         multiple subcomponents to run
    #         (GCP / AWS or even durability jobs with different levels)
    test_jobs_list = list()
    data = query_result_row['QE-Test-Suites']

    # Check if OS is compatible to run
    os_compatible = False
    if "os" not in data \
            or data['os'] == options.os \
            or (data['os'] == 'linux'
                and options.os in {'centos', 'ubuntu', 'debian'}):
        os_compatible = True

    if not os_compatible:
        # return empty list since nothing to run from this query result row
        print(f"OS not supported - {data['component']}")
        return test_jobs_list

    # Fetch common params for both cases
    # trailing spaces causes problems opening the files

    ini_file = str(data["config"]).strip()
    conf_file = str(data['confFile']).strip()
    component = str(data["component"]).strip()
    framework = data["framework"] if "framework" in data else "testrunner"
    jenkins_server_url = data['jenkins_server_url'] \
        if 'jenkins_server_url' in data else options.jenkins_server_url
    jenkins_server_url = str(jenkins_server_url)
    mailing_list = data['mailing_list'] \
        if 'mailing_list' in data else 'qa@couchbase.com'
    mode = data["mode"] \
        if 'mode' in data else "java"
    slave = data['slave'] \
        if 'slave' in data else "P0"
    support_py3 = data["support_py3"] \
        if 'support_py3' in data else "false"
    time_out = data["timeOut"] \
        if "timeOut" in data else "120"
    init_nodes = data['initNodes'].lower() == 'true' \
        if 'initNodes' in data else True
    parameters = data["parameters"] \
        if "parameters" in data else ""
    install_parameters = data["installParameters"] \
        if 'installParameters' in data else 'None'
    owner = data['owner'] \
        if 'owner' in data else 'QE'

    add_pool_id = options.addPoolId
    # if there's an additional pool, get the number
    # of additional servers needed from the ini
    add_pool_server_count = getNumberOfAddpoolServers(ini_file, add_pool_id)

    if options.serverType in CLOUD_SERVER_TYPES:
        config = configparser.ConfigParser()
        config.read(data['config'])

        for section in config.sections():
            if section == "cbbackupmgr" and config.has_option(section,
                                                              "endpoint"):
                add_pool_server_count = 1
                add_pool_id = "localstack"
                break

    ssh_u_name, ssh_p_word = get_ssh_username_password(ini_file)
    common_sub_comp_dict = {
        'component': component,
        'subcomponent': "None",
        'confFile': conf_file,
        'iniFile': ini_file,
        'serverCount': getNumberOfServers(ini_file),
        'ssh_username': ssh_u_name,
        'ssh_password': ssh_p_word,
        'addPoolServerCount': add_pool_server_count,
        'timeLimit': time_out,
        'parameters': parameters,
        'initNodes': init_nodes,
        'installParameters': install_parameters,
        'slave': slave,
        'owner': owner,
        'mailing_list': mailing_list,
        'mode': mode,
        'framework': framework,
        'addPoolId': add_pool_id,
        'target_jenkins': jenkins_server_url,
        'support_py3': support_py3,
        'mixed_build_config': {}}

    if not REPO_PULLED:
        if options.branch != "master":
            try:
                subprocess.run(
                    ["git", "checkout", "origin/" + options.branch,
                     "--", data['config']], check=True)
            except Exception:
                print(
                    'Git error: Did not find {} in {} branch'.format(
                        data['config'], options.branch))
        try:
            subprocess.run(["git", "pull"], check=True)
        except Exception:
            print("Git pull failed !!!")
        REPO_PULLED = True

    unsupported_feature_msg = f"Not supported in {options.version} - {component}:"
    if "subcomponents" in data:
        # Config entry has multiple subcomponents to evaluate
        user_input_sub_components = options.subcomponent.split(',')
        for inner_json_dict in data['subcomponents']:
            # Create a new copy to append multiple subcomponents
            sub_comp_dict = deepcopy(common_sub_comp_dict)
            if sub_component_is_supported_in_this_release(inner_json_dict):
                # Check to consider only user input sub-components
                # Eg: if user input is 'aws_upgrade_1', then only consider
                # that alone leaving out other sub-components from the list
                if user_input_sub_components[0] != 'None' \
                        and inner_json_dict["subcomponent"] not in user_input_sub_components:
                    continue
                populate_required_dispatcher_data(inner_json_dict, sub_comp_dict)
                # Append to list for returning back
                test_jobs_list.append(sub_comp_dict)
    else:
        # Traditional way (one sub-component case)
        # Use the above template as actual dict since no need to duplicate
        sub_comp_dict = common_sub_comp_dict
        if sub_component_is_supported_in_this_release(data):
            populate_required_dispatcher_data(data, sub_comp_dict)
            # Append to list for returning back
            test_jobs_list.append(sub_comp_dict)
    return test_jobs_list


def main():
    global TIMEOUT
    global SSH_NUM_RETRIES
    global SSH_POLL_INTERVAL
    global REPO_PULLED
    global RELEASE_VERSION
    global options
    global Server_Manager_Obj

    usage = '%prog -s suitefile -v version -o OS'
    parser = OptionParser(usage)
    parser.add_option('-v', '--version', dest='version')
    parser.add_option('-r', '--run', dest='run')  # Should be 12 hour or weekly
    parser.add_option('-o', '--os', dest='os')
    parser.add_option('-n', '--noLaunch', action="store_true", dest='noLaunch', default=False)
    parser.add_option('-c', '--component', dest='component', default=None)
    parser.add_option('-p', '--poolId', dest='poolId', default='12hour')
    parser.add_option('-a', '--addPoolId', dest='addPoolId', default=None)
    # Used the test Jenkins
    parser.add_option('-t', '--test', dest='test', default=False, action='store_true')
    parser.add_option('-s', '--subcomponent', dest='subcomponent', default=None)
    parser.add_option('--subcomponent_regex', dest='subcomponent_regex', default=None)
    parser.add_option('-e', '--extraParameters', dest='extraParameters', default=None)
    parser.add_option('-y', '--serverType', dest='serverType', type="choice",
                      default=DEFAULT_SERVER_TYPE,
                      choices=[VM, AWS, DOCKER, GCP, AZURE, CAPELLA_LOCAL,
                               ELIXIR_ONPREM, SERVERLESS_ONCLOUD,
                               PROVISIONED_ONCLOUD, ON_PREM_PROVISIONED,
                               SERVERLESS_COLUMNAR])  # or could be Docker
    # override server type passed to executor job e.g. CAPELLA_LOCAL
    parser.add_option('--server_type_name', dest='server_type_name', default=None)
    parser.add_option('-u', '--url', dest='url', default=None)
    parser.add_option('-j', '--jenkins', dest='jenkins', default=None)
    parser.add_option('-b', '--branch', dest='branch', default='master')
    parser.add_option('-g', '--cherrypick', dest='cherrypick', default=None)
    # whether to use production version of a test_suite_executor or test version
    parser.add_option('-l', '--launch_job', dest='launch_job', default='test_suite_executor')
    parser.add_option('-f', '--jenkins_server_url', dest='jenkins_server_url', default='http://qa.sc.couchbase.com')
    parser.add_option('-m', '--rerun_params', dest='rerun_params', default='')
    parser.add_option('-i', '--retries', dest='retries', default='1')
    parser.add_option('-q', '--fresh_run', dest='fresh_run',
                      default=True, action='store_false')
    parser.add_option('--rerun_condition', dest="rerun_condition",
                      default="all")
    parser.add_option('-k', '--include_tests', dest='include_tests', default=None)

    parser.add_option('--test_suite_db', dest="TEST_SUITE_DB",
                      default='172.23.105.178')
    parser.add_option('-z', '--timeout', dest='TIMEOUT', default='60')
    parser.add_option('-w', '--check_vm', dest='check_vm', default="False")
    parser.add_option('--ssh_poll_interval', dest='SSH_POLL_INTERVAL', default="20")
    parser.add_option('--ssh_num_retries', dest='SSH_NUM_RETRIES', default="3")
    parser.add_option('--job_params', dest='job_params', default=None)
    parser.add_option('--architecture', dest='architecture', default=DEFAULT_ARCHITECTURE)
    parser.add_option('--capella_url', dest='capella_url', default=None)
    parser.add_option('--capella_user', dest='capella_user', default=None)
    parser.add_option('--capella_password', dest='capella_password', default=None)
    parser.add_option('--capella_tenant', dest='capella_tenant', default=None)
    parser.add_option('--capella_token', dest='capella_token', default=None)
    parser.add_option('--build_url', dest='build_url', default=None)
    parser.add_option('--job_url', dest='job_url', default=None)
    parser.add_option('--sleep_between_trigger', dest='sleep_between_trigger', default=0)
    parser.add_option('--columnar_version', dest='columnar_version', default=0)
    parser.add_option('--is_dynamic_vms', dest='is_dynamic_vms', default="false")
    parser.add_option('--log_level', dest='log_level', default='INFO',
                      help='Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)')

    # set of parameters for testing purposes.
    # TODO: delete them after successful testing

    # dashboardReportedParameters is of the form param1=abc,param2=def
    parser.add_option('-d', '--dashboardReportedParameters',
                      dest='dashboardReportedParameters', default=None)

    options, args = parser.parse_args()

    # Setup logging with user-specified log level
    setup_logging(options.log_level)

    # Fix the OS for addPoolServers. See CBQE-5609 for details
    addPoolServer_os = "centos"
    RELEASE_VERSION = float('.'.join(options.version.split('.')[:2]))

    columnar_rel_version = float('.'.join(options.columnar_version.split('.')[:2])) \
        if options.columnar_version else float(0.0)

    print("""
          Run..................{}
          Version..............{}
          Release version......{}
          Branch...............{}
          OS...................{}
          URL..................{}
          cherry-pick..........{}
          reportedParameters...{}
          rerun params.........{}
          Timeout..............{}
          nolaunch.............{}
          is_dynamic_vms.......{}
          """.format(options.run, options.version, RELEASE_VERSION,
                     options.branch, options.os, options.url,
                     options.cherrypick, options.dashboardReportedParameters,
                     options.rerun_params, options.TIMEOUT, options.noLaunch,
                     options.is_dynamic_vms))

    if options.TEST_SUITE_DB:
        TEST_SUITE_DB = options.TEST_SUITE_DB
    if options.TIMEOUT:
        TIMEOUT = int(options.TIMEOUT)
    if options.SSH_POLL_INTERVAL:
        SSH_POLL_INTERVAL = int(options.SSH_POLL_INTERVAL)
    if options.SSH_NUM_RETRIES:
        SSH_NUM_RETRIES = int(options.SSH_NUM_RETRIES)

    if options.poolId:
        options.poolId = options.poolId.split(",")

    # What do we do with any reported parameters?
    # 1. Append them to the extra (testrunner) parameters
    # 2. Append the right hand of the equals sign to the subcomponent to make a report descriptor

    if options.extraParameters is None:
        if options.dashboardReportedParameters is None:
            runTimeTestRunnerParameters = None
        else:
            runTimeTestRunnerParameters = options.dashboardReportedParameters
    else:
        runTimeTestRunnerParameters = options.extraParameters
        if options.dashboardReportedParameters is not None:
            runTimeTestRunnerParameters = options.extraParameters + ',' \
                + options.dashboardReportedParameters

    testsToLaunch = []
    auth = ClusterOptions(PasswordAuthenticator(TEST_SUITE_DB_USER_NAME,
                                                TEST_SUITE_DB_PASSWORD))
    cluster = Cluster('couchbase://{}'.format(TEST_SUITE_DB), auth)
    qe_test_suite_bucket = cluster.bucket('QE-Test-Suites')
    Server_Manager_Obj = ServerManager(cluster, QE_SERVER_POOL_BUCKET,
                                       logger=log)

    if options.run == "12hr_weekly":
        suiteString = "('12hour' in partOf or 'weekly' in partOf)"
    else:
        suiteString = "'" + options.run + "' in partOf"

    if options.component is None or options.component == 'None':
        queryString = "select * from `QE-Test-Suites` where " + suiteString + " order by component"
    else:
        if (options.subcomponent is None or options.subcomponent == 'None') \
                and (options.subcomponent_regex is None or options.subcomponent_regex == "None"):
            splitComponents = options.component.split(',')
            componentString = ''
            for i in range(len(splitComponents)):
                componentString = componentString + "'" + splitComponents[i] + "'"
                if i < len(splitComponents) - 1:
                    componentString = componentString + ','

            queryString = "select * from `QE-Test-Suites` where {0} and component in [{1}] order by component;".format(
                suiteString, componentString)

        elif options.subcomponent_regex is None or options.subcomponent_regex == "None":
            # have a subcomponent, assume only 1 component
            splitSubcomponents = options.subcomponent.split(',')
            subcomponentString = ''
            for i in range(len(splitSubcomponents)):
                subcomponentString = subcomponentString + "'" + splitSubcomponents[i] + "'"
                if i < len(splitSubcomponents) - 1:
                    subcomponentString = subcomponentString + ','
            queryString = (
                "SELECT * FROM `QE-Test-Suites`"
                " WHERE {0}"
                " AND component in ['{1}']"
                " AND (subcomponent in [{2}]"
                "      OR ANY `inner_json` IN `subcomponents` SATISFIES `inner_json`.`subcomponent` in [{2}] END);")\
                .format(suiteString, options.component, subcomponentString)
        else:
            splitComponents = options.component.split(',')
            componentString = ''
            for comp in splitComponents:
                comp = f'"{comp}"'
                if componentString == '':
                    componentString = comp
                else:
                    componentString = f'{componentString},{comp}'

            queryString = (
                "SELECT * FROM `QE-Test-Suites`.`_default`.`_default` AS `QE-Test-Suites`"
                " WHERE {0}"
                " AND (component in [{1}] and subcomponent like \"{2}\""
                "      OR ANY `inner_json` IN `subcomponents` SATISFIES `inner_json`.`subcomponent` LIKE \"{2}\" END);")\
                .format(suiteString, componentString, options.subcomponent_regex)

    log.info("Query: '{}'".format(queryString))
    results = cluster.query(queryString)

    REPO_PULLED = False
    for row in results.rows():
        try:
            tests_dict_list = extract_individual_tests_from_query_result(
                columnar_rel_version, row)
            testsToLaunch.extend(tests_dict_list)
        except Exception:
            log.error('exception in querying tests, possible bad record')
            log.error((traceback.format_exc()))
            log.error(row)

    if not options.fresh_run:
        # Filter out jobs which have already passed in rerun (not a fresh_run)
        job_index_to_pop = list()
        for job_index, test_to_launch in enumerate(testsToLaunch):
            # build the dashboard descriptor
            dashboard_desc = urllib.parse.quote(
                test_to_launch['subcomponent'])
            if options.dashboardReportedParameters is not None:
                for o in options.dashboardReportedParameters.split(','):
                    dashboard_desc += '_' + o.split('=')[1]

            if runTimeTestRunnerParameters is None:
                parameters = test_to_launch['parameters']
            else:
                if test_to_launch['parameters'] == 'None':
                    parameters = runTimeTestRunnerParameters
                else:
                    parameters = test_to_launch['parameters'] \
                        + ',' + runTimeTestRunnerParameters
            dispatch_job = find_rerun_job.should_dispatch_job(
                options.os,
                test_to_launch['component'],
                dashboard_desc,
                options.version,
                parameters)
            if not dispatch_job:
                job_index_to_pop.append(job_index)

        # Popping in reverse so the indexes won't mess up
        job_index_to_pop.reverse()
        for job_index in job_index_to_pop:
            testsToLaunch.pop(job_index)

    total_req_servercount = 0
    total_req_addservercount = 0
    for i in testsToLaunch:
        # Reset server count request to 0 for provisioned
        if options.serverType == "PROVISIONED_ONCLOUD":
            i['serverCount'] = 0
        if options.serverType == "SERVERLESS_COLUMNAR":
            i['serverCount'] = 0
        total_req_servercount = total_req_servercount + i['serverCount']
        total_req_addservercount = total_req_addservercount + i['addPoolServerCount']
    table_view = TableView(log.critical)
    table_view.add_row(["Jobs to launch", len(testsToLaunch)])
    table_view.add_row(["Req. serverCount", total_req_servercount])
    table_view.add_row(["addPoolServerCount", total_req_addservercount])
    table_view.display("Total overview:")

    table_view.rows = list()
    table_view.set_headers(["component", "subcomponent", "serverCount",
                            "addPoolServerCount", "framework"])
    for i in testsToLaunch:
        table_view.add_row([i['component'], i['subcomponent'],
                            i['serverCount'], i['addPoolServerCount'],
                            i['framework']])
    table_view.display("Tests to launch:")
    print('\n')

    # this are VM/Docker dependent - or maybe not
    launchString = '/buildWithParameters?token=test_dispatcher&' + \
                   'version_number={0}&confFile={1}&descriptor={2}&component={3}&subcomponent={4}&' + \
                   'iniFile={5}&parameters={6}&os={7}&initNodes={8}&' \
                   'installParameters={9}&branch={10}&slave={11}&' \
                   'owners={12}&mailing_list={13}&mode={14}&timeout={15}&' \
                   'columnar_version_number={16}&mixed_build_config={17}&' \
                   'is_dynamic_vms={18}'
    if options.rerun_params:
        rerun_params = options.rerun_params.strip('\'')
        launchString = launchString + '&' + urllib.parse.urlencode({
            "rerun_params": rerun_params})
    launchString = launchString + '&retries=' + options.retries
    if options.include_tests:
        launchString = launchString + '&include_tests='+urllib.parse.quote(options.include_tests.replace("'", " ").strip())
    launchString = launchString + '&fresh_run=' + urllib.parse.quote(
        str(options.fresh_run).lower())
    if options.url is not None:
        launchString = launchString + '&url=' + options.url
    if options.cherrypick is not None:
        launchString = launchString + '&cherrypick=' + urllib.parse.quote(options.cherrypick)
    if options.architecture != DEFAULT_ARCHITECTURE:
        launchString = launchString + '&arch=' + options.architecture
    if options.serverType != DEFAULT_SERVER_TYPE or options.server_type_name is not None:
        server_type = options.serverType if options.server_type_name is None else options.server_type_name
        launchString = launchString + '&server_type=' + server_type
    b_url = options.build_url
    if b_url is None:
        b_url = OS.getenv("BUILD_URL")
    currentExecutorParams = get_jenkins_params.get_params(b_url)
    if currentExecutorParams is None:
        currentExecutorParams = {}
    if options.job_url is not None:
        currentExecutorParams['dispatcher_url'] = options.job_url
    elif OS.getenv('JOB_URL') is not None:
        currentExecutorParams['dispatcher_url'] = OS.getenv('JOB_URL')
    currentExecutorParams = json.dumps(currentExecutorParams)
    log.info(currentExecutorParams)
    summary = []
    total_jobs_count = len(testsToLaunch)
    job_index = 1
    total_servers_being_used = 0
    total_addl_servers_being_used = 0

    if options.noLaunch:
        log.critical("\n -- No launch selected -- \n")

    while len(testsToLaunch) > 0:
        try:
            if options.noLaunch:
                i = 0
                print("\n\n *** Dispatching job#{} of {} with {} servers (total={}) and {} "
                      "additional "
                      "servers(total={}) :  {}-{} with {}\n".format(job_index, total_jobs_count,
                                                                    testsToLaunch[i]['serverCount'],
                                                                    total_servers_being_used,
                                                                    testsToLaunch[i][
                                                                        'addPoolServerCount'],
                                                                    total_addl_servers_being_used,
                                                                    testsToLaunch[i]['component'],
                                                                    testsToLaunch[i][
                                                                        'subcomponent'],
                                                                    testsToLaunch[i][
                                                                        'framework'], ))
                # sample data as it is a noLaunch to get the general URL
                descriptor = "descriptor"
                dashboardDescriptor = "dashboardDescriptor"
                parameters = "parameters"
                url = launchString.format(options.version, testsToLaunch[i]['confFile'], descriptor,
                                          testsToLaunch[i]['component'], dashboardDescriptor,
                                          testsToLaunch[i]['iniFile'],
                                          urllib.parse.quote(parameters), options.os,
                                          testsToLaunch[i]['initNodes'],
                                          testsToLaunch[i]['installParameters'], options.branch,
                                          testsToLaunch[i]['slave'],
                                          urllib.parse.quote(testsToLaunch[i]['owner']),
                                          urllib.parse.quote(testsToLaunch[i]['mailing_list']),
                                          testsToLaunch[i]['mode'], testsToLaunch[i]['timeLimit'],
                                          options.columnar_version,
                                          testsToLaunch[i]['mixed_build_config'],
                                          options.is_dynamic_vms)
                url = url + '&dispatcher_params=' + urllib.parse.urlencode(
                    {"parameters": currentExecutorParams})
                # optional add [-docker] [-Jenkins extension] - TBD duplicate
                launchStringBase = testsToLaunch[i]['target_jenkins'] \
                    + '/job/' + str(options.launch_job)
                launchStringBaseF = launchStringBase
                if options.serverType == DOCKER:
                    launchStringBaseF = launchStringBase + '-docker'
                if options.test:
                    launchStringBaseF = launchStringBase + '-test'
                framework = testsToLaunch[i]['framework']
                if framework != 'testrunner':
                    launchStringBaseF = launchStringBaseF + "-" + framework
                elif options.jenkins is not None:
                    launchStringBaseF = launchStringBaseF + '-' + options.jenkins

                url = launchStringBaseF + url
                if options.job_params:
                    url = update_url_with_job_params(url, options.job_params)

                print('\n', time.asctime(time.localtime(time.time())), 'launching ', url)
                total_servers_being_used += testsToLaunch[0]['serverCount']
                total_addl_servers_being_used += testsToLaunch[0]['addPoolServerCount']
                testsToLaunch.pop(0)
                job_index += 1
                continue
            # this bit is Docker/VM dependent

            # see if we can match a test
            haveTestToLaunch = False
            i = 0
            pool_to_use = options.poolId[0]

            for pool_id in options.poolId:
                if options.serverType in CLOUD_SERVER_TYPES:
                    haveTestToLaunch = True
                    break

                serverCount = get_available_servers_count(
                    options=options,
                    os_version=options.os,
                    pool_id=pool_id)
                if not serverCount or serverCount == 0:
                    continue

                test_index = 0
                while not haveTestToLaunch and test_index < len(testsToLaunch):
                    if testsToLaunch[test_index]['serverCount'] <= serverCount:
                        if testsToLaunch[test_index]['addPoolServerCount']:
                            addPoolId = testsToLaunch[test_index]['addPoolId']
                            addlServersCount = get_available_servers_count(options=options, is_addl_pool=True, os_version=addPoolServer_os, pool_id=addPoolId)
                            if addlServersCount == 0:
                                print(time.asctime(time.localtime(time.time())), 'no {0} VMs at this time'.format(addPoolId))
                                test_index = test_index + 1
                            else:
                                print(time.asctime(
                                    time.localtime(time.time())), "there are {0} {1} servers available".format(
                                    addlServersCount, addPoolId))
                                haveTestToLaunch = True
                        else:
                            haveTestToLaunch = True
                    else:
                        test_index = test_index + 1
                i = test_index
                pool_to_use = pool_id

                if haveTestToLaunch:
                    break

            if haveTestToLaunch:
                if options.noLaunch:
                    job_index += 1
                    testsToLaunch.pop(i)
                    continue

                print("\n\n *** Dispatching job#{} of {} with {} servers "
                      "(total={}) and {} additional servers(total={}):  {}-{} with {}\n"
                      .format(job_index, total_jobs_count,
                              testsToLaunch[i]['serverCount'],
                              total_servers_being_used,
                              testsToLaunch[i]['addPoolServerCount'],
                              total_addl_servers_being_used,
                              testsToLaunch[i]['component'],
                              testsToLaunch[i]['subcomponent'],
                              testsToLaunch[i]['framework'],))

                if options.noLaunch:
                    job_index += 1
                    testsToLaunch.pop(i)
                    continue

                # build the dashboard descriptor
                dashboardDescriptor = urllib.parse.quote(testsToLaunch[i]['subcomponent'])
                if options.dashboardReportedParameters is not None:
                    for o in options.dashboardReportedParameters.split(','):
                        dashboardDescriptor += '_' + o.split('=')[1]

                dispatch_job = True
                if not options.fresh_run:
                    if runTimeTestRunnerParameters is None:
                        parameters = testsToLaunch[i]['parameters']
                    else:
                        if testsToLaunch[i]['parameters'] == 'None':
                            parameters = runTimeTestRunnerParameters
                        else:
                            parameters = testsToLaunch[i][
                                             'parameters'] + ',' + runTimeTestRunnerParameters
                    rerun_condition = options.rerun_condition
                    only_failed = False
                    only_pending = False
                    only_unstable = False
                    only_install_failed = False
                    if rerun_condition == "FAILED":
                        only_failed = True
                    elif rerun_condition == "UNSTABLE":
                        only_unstable = True
                    elif rerun_condition == "PENDING":
                        only_pending = True
                    elif rerun_condition == "INST_FAIL":
                        only_install_failed = True
                    dispatch_job = \
                        find_rerun_job.should_dispatch_job(
                            options.os, testsToLaunch[i][
                                'component'], dashboardDescriptor
                            , options.version, parameters,
                            only_pending, only_failed, only_unstable, only_install_failed)

                # and this is the Jenkins descriptor
                descriptor = testsToLaunch[i]['component'] + '-' + testsToLaunch[i]['subcomponent'] + '-' + time.strftime('%b-%d-%X') + '-' + options.version

                if options.serverType == GCP:
                    # GCP labels are limited to 63 characters which might be too small.
                    # Must also start with a lowercase letter. Just use a UUID which is 32 characters.
                    descriptor = "testrunner-" + str(uuid4())
                elif options.serverType == AWS:
                    # A stack name can contain only alphanumeric characters (case-sensitive) and hyphens
                    descriptor = descriptor.replace("_", "-")
                    descriptor = "".join(filter(lambda char: str.isalnum(char) or char == "-", descriptor))
                elif options.serverType == AZURE:
                    descriptor = "testrunner-" + str(uuid4())

                descriptor = urllib.parse.quote(descriptor)

                # grab the server resources
                # this bit is Docker/VM dependent
                servers = []
                internal_servers = None
                unreachable_servers = []
                how_many = testsToLaunch[i]['serverCount'] - len(servers)
                if options.check_vm == "True":
                    while how_many > 0:
                        unchecked_servers, _ = get_servers(
                            options=options,
                            descriptor=descriptor,
                            test=testsToLaunch[i],
                            how_many=how_many,
                            os_version=options.os,
                            pool_id=pool_to_use)
                        checked_servers, bad_servers = check_servers_via_ssh(
                            servers=unchecked_servers,
                            test=testsToLaunch[i])
                        for ss in checked_servers:
                            servers.append(ss)
                        for ss in bad_servers:
                            unreachable_servers.append(ss)
                        how_many = testsToLaunch[i]['serverCount'] - len(servers)

                else:
                    servers, internal_servers = get_servers(
                        options=options,
                        descriptor=descriptor,
                        test=testsToLaunch[i],
                        how_many=how_many,
                        os_version=options.os,
                        pool_id=pool_to_use)

                if options.serverType != DOCKER:
                    # sometimes there could be a race, before a dispatcher process acquires vms,
                    # another waiting dispatcher process could grab them, resulting in lesser vms
                    # for the second dispatcher process
                    if len(servers) != testsToLaunch[i]['serverCount']:
                        release_servers(options, descriptor)
                        sleep_function(POLL_INTERVAL,
                                       f"Received server count \"{len(servers)} != {testsToLaunch[i]['serverCount']}\" expected")
                        continue

                # get additional pool servers as needed
                addl_servers = []

                if testsToLaunch[i]['addPoolServerCount']:
                    how_many_addl = testsToLaunch[i]['addPoolServerCount'] - len(addl_servers)
                    addPoolId = testsToLaunch[i]['addPoolId']
                    addl_servers, _ = get_servers(options=options, descriptor=descriptor, test=testsToLaunch[i], how_many=how_many_addl, is_addl_pool=True, os_version=addPoolServer_os, pool_id=addPoolId)
                    if len(addl_servers) != testsToLaunch[i]['addPoolServerCount']:
                        print(
                            "Received additional servers count does not match the expected "
                            "test additional servers count!")
                        release_servers(options, descriptor)
                        continue

                # and send the request to the test executor

                # figure out the parameters, there are test suite specific, and added at dispatch time
                if runTimeTestRunnerParameters is None:
                    parameters = testsToLaunch[i]['parameters']
                else:
                    if testsToLaunch[i]['parameters'] == 'None':
                        parameters = runTimeTestRunnerParameters
                    else:
                        parameters = testsToLaunch[i]['parameters'] + ',' + runTimeTestRunnerParameters

                branch_to_trigger = options.branch
                slave_to_use = testsToLaunch[i]['slave']
                if (testsToLaunch[i]['framework'] == "TAF"
                        and str(options.version[:3]) == "8.0"
                        and options.branch == "morpheus"
                        and testsToLaunch[i]["support_py3"] == "false"):
                    # TAF jobs with support_py3=false on master_jython branch
                    branch_to_trigger = "master_jython"
                elif (testsToLaunch[i]['framework'] == "testrunner"
                        and float(options.version[:3]) >= 8.1):
                    # testrunner jobs with support_py3=true
                    slave_to_use = "deb12_jython_slave"
                    # Force update target_jenkins URL to point to specific IP
                    testsToLaunch[i]['target_jenkins'] = 'http://172.23.121.80'

                url = launchString.format(options.version,
                                          testsToLaunch[i]['confFile'],
                                          descriptor,
                                          testsToLaunch[i]['component'],
                                          dashboardDescriptor,
                                          testsToLaunch[i]['iniFile'],
                                          urllib.parse.quote(parameters),
                                          options.os,
                                          testsToLaunch[i]['initNodes'],
                                          testsToLaunch[i]['installParameters'],
                                          branch_to_trigger,
                                          slave_to_use,
                                          urllib.parse.quote(testsToLaunch[i]['owner']),
                                          urllib.parse.quote(
                                              testsToLaunch[i]['mailing_list']),
                                          testsToLaunch[i]['mode'],
                                          testsToLaunch[i]['timeLimit'],
                                          options.columnar_version,
                                          testsToLaunch[i]['mixed_build_config'],
                                          options.is_dynamic_vms)
                url = url + '&dispatcher_params=' + \
                                urllib.parse.urlencode({"parameters":
                                                currentExecutorParams})

                if options.serverType != DOCKER:
                    servers_str = json.dumps(servers).replace(' ','').replace('[','', 1)
                    servers_str = rreplace(servers_str, ']', 1)
                    url = url + '&servers=' + urllib.parse.quote(servers_str)

                    if internal_servers:
                        internal_servers_str = json.dumps(internal_servers).replace(' ','').replace('[','', 1)
                        internal_servers_str = rreplace(internal_servers_str, ']', 1)
                        url = url + '&internal_servers=' + urllib.parse.quote(internal_servers_str)

                    if testsToLaunch[i]['addPoolServerCount']:
                        addPoolServers = json.dumps(addl_servers).replace(' ','').replace('[','', 1)
                        addPoolServers = rreplace(addPoolServers, ']', 1)
                        url = url + '&addPoolServerId=' +\
                                testsToLaunch[i]['addPoolId'] +\
                                '&addPoolServers=' +\
                                urllib.parse.quote(addPoolServers)

                if len(unreachable_servers) > 0:
                    print("Marking following VM(s) as 'ssh_failed'")
                    for s in unreachable_servers:
                        Server_Manager_Obj.release_ip(s, "ssh_failed")

                # optional add [-docker] [-Jenkins extension]
                launchStringBase = "%s/job/%s" % (
                    testsToLaunch[i]['target_jenkins'],
                    str(options.launch_job))
                launchStringBaseF = launchStringBase
                if options.serverType == DOCKER:
                    launchStringBaseF = launchStringBase + '-docker'
                if options.test:
                    launchStringBaseF = launchStringBase + '-test'
                #     if options.framework.lower() == "jython":
                framework = testsToLaunch[i]['framework']
                if framework != 'testrunner':
                    launchStringBaseF = launchStringBaseF + "-" + framework
                elif options.jenkins is not None:
                    launchStringBaseF = launchStringBaseF + '-' + options.jenkins
                url = launchStringBaseF + url

                # For capella, invite new user for each test job to launch
                if options.serverType in [
                        SERVERLESS_ONCLOUD,
                        PROVISIONED_ONCLOUD,
                        SERVERLESS_COLUMNAR]:
                    if "cloud.couchbase.com" in options.capella_url:
                        print(
                            f'CAPELLA: Skipping Inviting new user to capella production'
                            f'tenant {options.capella_tenant} on {options.capella_url}')
                        url = update_url_with_job_params(
                            url,
                            f"capella_user={options.capella_user}&capella_password={options.capella_password}")
                    else:
                        print(f'CAPELLA: Inviting new user to capella tenant {options.capella_tenant} on {options.capella_url}')
                        invited_user, invited_password = capella.invite_user(
                            options.capella_token, options.capella_url,
                            options.capella_user, options.capella_password,
                            options.capella_tenant)
                        if invited_user is None or invited_password is None:
                            print("CAPELLA: We could not invite user to capella cluster. Skipping job.")
                            job_index += 1
                            testsToLaunch.pop(i)
                            continue
                        url = update_url_with_job_params(url, f"capella_user={invited_user}&capella_password={invited_password}")

                if options.job_params:
                    url = update_url_with_job_params(url, options.job_params)
                log.info(f"Launching {url}")

                if options.noLaunch or not dispatch_job:
                    # free the VMs
                    sleep_function(3, f"Freeing servers for {descriptor}")
                    if options.serverType == DOCKER:
                        # Docker gets only a single sever from
                        # get_server. Not hard coding the server
                        # we get, which as of now is constant,
                        # but might change in the future.
                        ipaddr = servers[0]
                        status = Server_Manager_Obj.release_dockers(descriptor,
                                                                    ipaddr)
                        log.info(f"Release docker status: {status}")
                    else:
                        release_servers(options, descriptor)
                else:
                    doc = qe_test_suite_bucket.default_collection().get(
                        "jenkins_cred")
                    if not doc.success:
                        raise Exception(
                            "Cred doc missing in QE_Test-Suites bucket")
                    doc = doc.value
                    credentials = "%s:%s" % (doc["username"], doc["password"])
                    auth = base64.encodebytes(credentials.encode('utf-8'))
                    auth = auth.decode('utf-8').rstrip('\n')
                    header = {'Content-Type': 'application/json',
                              'Authorization': 'Basic %s' % auth,
                              'Accept': '*/*'}
                    response, content = httplib2.Http(timeout=TIMEOUT)\
                        .request(url, 'GET', headers=header)
                    print("Response is: {0}".format(str(response)))
                    print("Content is: {0}".format(str(content)))
                    if int(response['status'])>=400:
                        raise Exception("Unexpected response while dispatching the request!")

                total_servers_being_used += testsToLaunch[0]['serverCount']
                total_addl_servers_being_used += testsToLaunch[0]['addPoolServerCount']
                job_index += 1
                testsToLaunch.pop(i)
                summary.append({
                    'test': descriptor,
                    'time': time.asctime(time.localtime(time.time()))})
                if options.noLaunch:
                    # no sleeping necessary
                    pass
                elif options.serverType == DOCKER:
                    sleep_function(240, f"Waiting for docker port allocation for {descriptor}")     # this is due to the docker port allocation race
                elif int(options.sleep_between_trigger) != 0:
                    sleep_function(int(options.sleep_between_trigger), f"Waiting for {options.sleep_between_trigger} seconds between triggers for {descriptor}")
                else:
                    sleep_function(30)
            else:
                sleep_function(POLL_INTERVAL, 'Not enough servers')
        except Exception as e:
            log.critical(f'Have an exception: {str(e)}\n'
                         f'{traceback.format_exc()}')
            if 'descriptor' in locals():
                try:
                    log.info(f"Releasing servers for {descriptor}")
                    release_servers(options, descriptor)
                except Exception:
                    pass
            sleep_function(POLL_INTERVAL, 'Exception in main loop')
    # endwhile

    if not options.noLaunch:
        print('\n\n\ndone, everything is launched')
        for i in summary:
            print((i['test'], 'was launched at', i['time']))
        print("\n\n *** Dispatched total {} jobs using total {} servers and {} additional "
              "servers "
              "\n".format(total_jobs_count,
                          total_servers_being_used,
                          total_addl_servers_being_used))
    else:
        print("\n Done!")
    return


def update_url_with_job_params(url, job_params):
    dict = {}
    newurl = ''
    url_keys_values = url.split('&')
    for param_key in url_keys_values:
        param_index = param_key.find('=', 1)
        rparam_name = param_key[0:param_index]
        if param_index < 0:
            rparam_value = ''
        else:
            rparam_value = param_key[param_index + 1:]
        dict[rparam_name] = rparam_value

    param_keys_values = job_params.split('&')
    for param_key in param_keys_values:
        param_index = param_key.find('=', 1)
        rparam_name = param_key[0:param_index]
        if param_index < 0:
            rparam_value = ''
        else:
            rparam_value = param_key[param_index + 1:]
        dict[rparam_name] = urllib.parse.urlencode({rparam_name: rparam_value})

    print(dict)
    for key in dict:
        if newurl == '':
            newurl = key + "=" + dict[key]
        elif dict[key].startswith(key+"="):
            newurl += '&' + dict[key]
        else:
            newurl += '&' + key + "=" + dict[key]
    return newurl


def release_servers_cloud(options, descriptor):
    if options.serverType == AWS:
        cloud_provision.aws_terminate(descriptor)
    elif options.serverType == GCP:
        cloud_provision.gcp_terminate(descriptor)
    elif options.serverType == AZURE:
        cloud_provision.az_terminate(descriptor)
    elif options.serverType == SERVERLESS_ONCLOUD:
        print("SERVERLESS: nothing to release")
    elif options.serverType == PROVISIONED_ONCLOUD:
        print("PROVISIONED: nothing to release")
    elif options.serverType == SERVERLESS_COLUMNAR:
        print("COLUMNAR: nothing to release")


def release_servers(options, descriptor):
    if options.serverType in CLOUD_SERVER_TYPES:
        descriptor = urllib.parse.unquote(descriptor)
        release_servers_cloud(options, descriptor)
    else:
        Server_Manager_Obj.release_servers(username=descriptor)


if __name__ == "__main__":
    main()
