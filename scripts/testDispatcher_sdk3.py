import base64
import logging
import urllib.request
import urllib.parse
import urllib.error
from uuid import uuid4
import httplib2
import json
import time
from optparse import OptionParser
import traceback
import os as OS
import paramiko
import ipaddress
import subprocess
import configparser

from couchbase.cluster import Cluster, ClusterOptions, PasswordAuthenticator
import get_jenkins_params
import find_rerun_job

import cloud_provision
import capella
from table_view import TableView

# takes an ini template as input, standard out is populated with the server pool
# need a descriptor as a parameter
# need a timeout param

POLL_INTERVAL = 60
SERVER_MANAGER = '172.23.104.162:8081'
ADDL_SERVER_MANAGER = '172.23.104.162:8081'
TEST_SUITE_DB = '172.23.104.162'
SERVER_MANAGER_USER_NAME = 'Administrator'
SERVER_MANAGER_PASSWORD = "esabhcuoc"
TIMEOUT = 60
SSH_NUM_RETRIES = 3
SSH_POLL_INTERVAL = 20

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
    except:
        return 0


def get_ssh_username(iniFile):
    f = open(iniFile)
    contents = f.readlines()
    for line in contents:
        if "username:" in line:
            arr = line.split(":")[1:]
            username = arr[0].rstrip()
            return username
    return ""


def get_ssh_password(iniFile):
    f = open(iniFile)
    contents = f.readlines()
    for line in contents:
        if "password:" in line:
            arr = line.split(":")[1:]
            password = arr[0].rstrip()
            return password
    return ""


def rreplace(str, pattern, num_replacements):
    return str.rsplit(pattern, num_replacements)[0]


def get_available_servers_count(options=None, is_addl_pool=False,
                                os_version="", pool_id=None):
    # this bit is Docker/VM dependent
    getAvailUrl = 'http://' + SERVER_MANAGER + '/getavailablecount/'

    if options.serverType == DOCKER:
        # may want to add OS at some point
        getAvailUrl = getAvailUrl + 'docker?os={0}&poolId={1}'\
            .format(os_version, pool_id)
    else:
        if is_addl_pool == True:
            SM = ADDL_SERVER_MANAGER
        else:
            SM = SERVER_MANAGER
        getAvailUrl = 'http://' + SM + '/getavailablecount/' \
                      + '{0}?poolId={1}'.format(os_version, pool_id)

    print("Connecting {}".format(getAvailUrl))
    count = 0
    response, content = httplib2.Http(timeout=TIMEOUT).request(getAvailUrl, 'GET')
    if response.status != 200:
        print(time.asctime(time.localtime(time.time())), 'invalid server response', content)
        time.sleep(POLL_INTERVAL)
    elif int(content) == 0:
        print(time.asctime(time.localtime(time.time())), 'no VMs')
        time.sleep(POLL_INTERVAL)
    else:
        count = int(content)
    return count


def get_servers_cloud(options, descriptor, how_many, is_addl_pool, os_version, pool_id):
    descriptor = urllib.parse.unquote(descriptor)
    type = "couchbase"
    if is_addl_pool:
        type = pool_id
    if options.serverType == AWS:
        ssh_key_path = OS.environ.get("AWS_SSH_KEY")
        return cloud_provision.aws_get_servers(descriptor, how_many, os_version, type, ssh_key_path, options.architecture), None
    elif options.serverType == GCP:
        ssh_key_path = OS.environ.get("GCP_SSH_KEY")
        return cloud_provision.gcp_get_servers(descriptor, how_many, os_version, type, ssh_key_path, options.architecture), None
    elif options.serverType == AZURE:
        """ Azure uses template with pw enable login.  No need key """
        ssh_key_path = ""
        return cloud_provision.az_get_servers(descriptor, how_many, os_version, type, ssh_key_path, options.architecture)
    elif options.serverType == SERVERLESS_ONCLOUD:
        return [], []
    elif options.serverType == PROVISIONED_ONCLOUD:
        return [], []
    elif options.serverType == SERVERLESS_COLUMNAR:
        return [], []


def get_servers(options=None, descriptor="", test=None, how_many=0, is_addl_pool=False, os_version="", pool_id=None):
    if options.serverType in CLOUD_SERVER_TYPES:
        return get_servers_cloud(options, descriptor, how_many, is_addl_pool, os_version, pool_id)
    elif options.serverType == DOCKER:
        getServerURL = 'http://' + SERVER_MANAGER + '/getdockers/{0}?count={1}&os={2}&poolId={3}'. \
                           format(descriptor, how_many, os_version, pool_id)
    else:
        if is_addl_pool == True:
            SM = ADDL_SERVER_MANAGER
        else:
            SM = SERVER_MANAGER
        getServerURL = 'http://' + SM + '/getservers/{0}?count={1}&expiresin={2}&os={3}&poolId={' \
                        '4}'.format(descriptor, how_many, test['timeLimit'], os_version, pool_id)

    response, content = httplib2.Http(timeout=TIMEOUT).request(getServerURL, 'GET')
    print("*" * 75)
    print('getServerURL: {}'.format(getServerURL))
    print('response: {}'.format(response))
    print('content: {}'.format(content))
    print("*" * 75)
    if response.status == 499:
        time.sleep(POLL_INTERVAL)  # some error checking here at some point
        return json.loads("[]"), None

    content1 = content.decode()
    if '[' not in content1:
        content2 = '["'+str(content1)+'"]'
        return json.loads(content2), None
    else:
        return json.loads(content1), None


def check_servers_via_ssh(servers=[], test=None):
    alive_servers = []
    bad_servers = []
    for server in servers:
        if is_vm_alive(server=server, ssh_username=test['ssh_username'], ssh_password=test['ssh_password']):
            alive_servers.append(server)
        else:
            bad_servers.append(server)
    return alive_servers, bad_servers


def is_vm_alive(server="", ssh_username="", ssh_password=""):
    try:
        if '[' in server or ']' in server:
            server = server.replace('[', '')
            server = server.replace(']','')
        ip_version = ipaddress.ip_address(server).version
    except ValueError as e:
        print("{0} is not recognized as valid IPv4 or IPv6 address.".format(server))
        return False
    num_retries = 1
    while num_retries <= SSH_NUM_RETRIES:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            #ssh.auth_password()
            ssh.connect(hostname=server, username=ssh_username, password=ssh_password)
            print("Successfully established test ssh connection to {0}. VM is recognized as valid.".format(server))
            return True
        except Exception as e:
            print("Exception occured while trying to establish ssh connection with {0}: {1}".format(server, str(e)))
            num_retries = num_retries +1
            time.sleep(SSH_POLL_INTERVAL)
            continue

    print("{0} is unreachable. Tried to establish ssh connection {1} times".format(server, num_retries-1))
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
                result += SiriusCouchbaseLoader.flatten_param_to_str(val)
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
                result += SiriusCouchbaseLoader.flatten_param_to_str(val)
            else:
                result += '"%s",' % val
        result = result.rstrip(",") + ']'
    return result


def main():
    global SERVER_MANAGER
    global ADDL_SERVER_MANAGER
    global TIMEOUT
    global SSH_NUM_RETRIES
    global SSH_POLL_INTERVAL

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
    parser.add_option('-k', '--include_tests', dest='include_tests', default=None)
    parser.add_option('-x', '--server_manager', dest='SERVER_MANAGER',
                      default='172.23.104.162:8081')
    parser.add_option('--addl_server_manager', dest='ADDL_SERVER_MANAGER',
                      default='172.23.104.162:8081')
    parser.add_option('--test_suite_db', dest="TEST_SUITE_DB",
                      default='172.23.104.162')
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

    # set of parameters for testing purposes.
    # TODO: delete them after successful testing

    # dashboardReportedParameters is of the form param1=abc,param2=def
    parser.add_option('-d', '--dashboardReportedParameters',
                      dest='dashboardReportedParameters', default=None)

    options, args = parser.parse_args()
    # Fix the OS for addPoolServers. See CBQE-5609 for details
    addPoolServer_os = "centos"
    releaseVersion = float('.'.join(options.version.split('.')[:2]))
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
          Server manager.......{}
          Timeout..............{}
          nolaunch.............{}
          """.format(options.run, options.version, releaseVersion,
                     options.branch, options.os, options.url,
                     options.cherrypick, options.dashboardReportedParameters,
                     options.rerun_params, options.SERVER_MANAGER,
                     options.TIMEOUT, options.noLaunch))

    if options.SERVER_MANAGER:
        SERVER_MANAGER = options.SERVER_MANAGER
    if options.ADDL_SERVER_MANAGER:
        ADDL_SERVER_MANAGER = options.ADDL_SERVER_MANAGER
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
            runTimeTestRunnerParameters = options.extraParameters + ',' + options.dashboardReportedParameters

    testsToLaunch = []
    auth = ClusterOptions(PasswordAuthenticator(SERVER_MANAGER_USER_NAME,
                                                SERVER_MANAGER_PASSWORD))
    cluster = Cluster('couchbase://{}'.format(TEST_SUITE_DB), auth)
    cb = cluster.bucket('QE-Test-Suites')

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
                print('subcomponentString is', subcomponentString)
                subcomponentString = subcomponentString + "'" + splitSubcomponents[i] + "'"
                if i < len(splitSubcomponents) - 1:
                    subcomponentString = subcomponentString + ','
            queryString = "select * from `QE-Test-Suites` where {0} and component in ['{1}'] and subcomponent in [{2}];". \
                format(suiteString, options.component, subcomponentString)
        else:
            splitComponents = options.component.split(',')
            componentString = ''
            for comp in splitComponents:
                comp = f'"{comp}"'
                if componentString == '':
                    componentString = comp
                else:
                    componentString = f'{componentString},{comp}'

            queryString = "select * from `QE-Test-Suites`.`_default`.`_default` where {0} and component in [{1}] and subcomponent like \"{2}\";". \
                format(suiteString, componentString, options.subcomponent_regex)

    print("Query: '{}'".format(queryString))
    results = cluster.query(queryString)

    repo_pulled = False
    for row in results.rows():
        try:
            data = row['QE-Test-Suites']
            data['config'] = data['config'].rstrip()  # trailing spaces causes problems opening the files

            # check any os specific
            if 'os' not in data or (data['os'] == options.os) or \
                    (data['os'] == 'linux' and options.os in {'centos', 'ubuntu','debian'}):

                # and also check for which release it is implemented in
                if 'implementedIn' not in data or releaseVersion >= float(data['implementedIn']):
                    # and check if there is a max version the tests
                    # can run in
                    if 'maxVersion' in data and releaseVersion > \
                            float(data['maxVersion']):
                        print((data['component'], data['subcomponent'], ' is not supported in this release'))
                        continue
                    if 'jenkins' in data:
                        # then this is sort of a special case, launch the old style Jenkins job
                        # not implemented yet
                        print(('Old style Jenkins', data['jenkins']))
                    else:
                        if 'initNodes' in data:
                            initNodes = data['initNodes'].lower() == 'true'
                        else:
                            initNodes = True
                        if 'installParameters' in data:
                            installParameters = data['installParameters']
                        else:
                            installParameters = 'None'
                        if 'slave' in data:
                            slave = data['slave']
                        else:
                            slave = 'P0'
                        if 'owner' in data:
                            owner = data['owner']
                        else:
                            owner = 'QE'
                        if 'mailing_list' in data:
                            mailing_list = data['mailing_list']
                        else:
                            mailing_list = 'qa@couchbase.com'
                        if 'mode' in data:
                            mode = data["mode"]
                        else:
                            mode = 'java'
                        if 'framework' in data:
                            framework = data["framework"]
                        else:
                            framework = 'testrunner'

                        if 'jenkins_server_url' in data:
                            jenkins_server_url = data['jenkins_server_url']
                        else:
                            jenkins_server_url = options.jenkins_server_url

                        if not repo_pulled:
                            if options.branch != "master":
                                try:
                                    subprocess.run(["git", "checkout", "origin/" + options.branch, "--", data['config']], check=True)
                                except Exception:
                                    print('Git error: Did not find {} in {} branch'.format(data['config'], options.branch))
                            try:
                                subprocess.run(["git", "pull"], check=True)
                            except Exception:
                                print("Git pull failed !!!")
                            repo_pulled = True

                        addPoolId = options.addPoolId

                        # if there's an additional pool, get the number
                        # of additional servers needed from the ini
                        addPoolServerCount = getNumberOfAddpoolServers(
                            data['config'],
                            addPoolId)

                        if options.serverType in CLOUD_SERVER_TYPES:
                            config = configparser.ConfigParser()
                            config.read(data['config'])

                            for section in config.sections():
                                if section == "cbbackupmgr" and config.has_option(section, "endpoint"):
                                    addPoolServerCount = 1
                                    addPoolId = "localstack"
                                    break

                        support_py3 = "false"
                        if 'support_py3' in data:
                            support_py3 = data["support_py3"]

                        mixed_build_config = dict()
                        if data["component"] == "columnar":
                            if 'mixed_build_config' in data:
                                mixed_build_config = data["mixed_build_config"]

                        testsToLaunch.append({
                            'component': data['component'],
                            'subcomponent': data['subcomponent'],
                            'confFile': data['confFile'],
                            'iniFile': data['config'],
                            'serverCount': getNumberOfServers(data['config']),
                            'ssh_username': get_ssh_username(data['config']),
                            'ssh_password': get_ssh_password(data['config']),
                            'addPoolServerCount': addPoolServerCount,
                            'timeLimit': data['timeOut'],
                            'parameters': data['parameters'],
                            'initNodes': initNodes,
                            'installParameters': installParameters,
                            'slave': slave,
                            'owner': owner,
                            'mailing_list': mailing_list,
                            'mode': mode,
                            'framework': framework,
                            'addPoolId': addPoolId,
                            'target_jenkins': str(jenkins_server_url),
                            'support_py3': support_py3,
                            'mixed_build_config': urllib.parse.quote(flatten_param_to_str(mixed_build_config)),
                        })
                else:
                    print((data['component'], data['subcomponent'], ' is not supported in this release'))
            else:
                print(('OS does not apply to', data['component'], data['subcomponent']))
        except Exception as e:
            print('exception in querying tests, possible bad record')
            print((traceback.format_exc()))
            print(data)

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
                   'iniFile={5}&parameters={6}&os={7}&initNodes={' \
                   '8}&installParameters={9}&branch={10}&slave={' \
                   '11}&owners={12}&mailing_list={13}&mode={14}&timeout={15}&' \
                   'columnar_version_number={16}&mixed_build_config={17}'
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
    print(currentExecutorParams)
    summary = []
    total_jobs_count = len(testsToLaunch)
    job_index = 1
    total_servers_being_used = 0
    total_addl_servers_being_used = 0

    if options.noLaunch:
        print("\n -- No launch selected -- \n")

    while len(testsToLaunch) > 0:
        try:
            if not options.noLaunch:
                print("\n\n *** Before dispatch, checking for the servers to run a  test suite\n")
            else:
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
                                          testsToLaunch[i]['mixed_build_config'])
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

                serverCount = get_available_servers_count(options=options, os_version=options.os, pool_id=pool_id)
                print("Server count={}".format(serverCount))

                if not serverCount or serverCount == 0:
                    continue

                print((time.asctime(time.localtime(time.time())), 'there are', serverCount, ' servers available in ', pool_id, ' pool'))

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
                    dispatch_job = \
                        find_rerun_job.should_dispatch_job(
                            options.os, testsToLaunch[i][
                                'component'], dashboardDescriptor
                            , options.version, parameters)

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
                        unchecked_servers, _ = get_servers(options=options, descriptor=descriptor, test=testsToLaunch[i],
                                                    how_many=how_many, os_version=options.os, pool_id=pool_to_use)
                        checked_servers, bad_servers = check_servers_via_ssh(servers=unchecked_servers,
                                                                            test=testsToLaunch[i])
                        for ss in checked_servers:
                            servers.append(ss)
                        for ss in bad_servers:
                            unreachable_servers.append(ss)
                        how_many = testsToLaunch[i]['serverCount'] - len(servers)

                else:
                    servers, internal_servers = get_servers(options=options, descriptor=descriptor, test=testsToLaunch[i],
                                            how_many=how_many, os_version=options.os, pool_id=pool_to_use)

                if options.serverType != DOCKER:
                    # sometimes there could be a race, before a dispatcher process acquires vms,
                    # another waiting dispatcher process could grab them, resulting in lesser vms
                    # for the second dispatcher process
                    if len(servers) != testsToLaunch[i]['serverCount']:
                        print("Received servers count does not match the expected test server count!")
                        release_servers(options, descriptor)
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
                if (testsToLaunch[i]['framework'] == "TAF"
                        and float(options.version[:3]) >= 8
                        and options.branch == "master"
                        and testsToLaunch[i]["support_py3"] == "true"):
                    branch_to_trigger = "master_py3_dev"
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
                                          testsToLaunch[i]['slave'],
                                          urllib.parse.quote(testsToLaunch[i]['owner']),
                                          urllib.parse.quote(
                                              testsToLaunch[i]['mailing_list']),
                                          testsToLaunch[i]['mode'],
                                          testsToLaunch[i]['timeLimit'],
                                          options.columnar_version,
                                          testsToLaunch[i]['mixed_build_config'])
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
                    print("The following VM(s) are unreachable for ssh connection:")
                    for s in unreachable_servers:
                        _, _ = httplib2.Http(timeout=TIMEOUT).request('http://' + SERVER_MANAGER + '/releaseip/' + s + '/ssh_failed', 'GET')
                        print(s)

                # optional add [-docker] [-Jenkins extension]
                launchStringBase = testsToLaunch[i]['target_jenkins'] \
                                   + '/job/' + str(options.launch_job)
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
                    SERVERLESS_ONCLOUD, PROVISIONED_ONCLOUD, SERVERLESS_COLUMNAR]:
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
                print('\n', time.asctime( time.localtime(time.time()) ), 'launching ', url)

                if options.noLaunch or not dispatch_job:
                    # free the VMs
                    time.sleep(3)
                    if options.serverType == DOCKER:
                        #Docker gets only a single sever from
                        # get_server. Not hard coding the server
                        # we get, which as of now is constant,
                        # but might change in the future.
                        ipaddr = servers[0]
                        response, content = httplib2.Http(
                            timeout=TIMEOUT). \
                            request(
                            'http://' + SERVER_MANAGER +
                            '/releasedockers/' + descriptor +
                            '?ipaddr=' + ipaddr,
                            'GET')
                        print(
                        'the release response', response, content)
                    else:
                        release_servers(options, descriptor)
                else:
                    doc = cb.get("jenkins_cred")
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
                summary.append( {'test':descriptor, 'time':time.asctime( time.localtime(time.time()) ) } )
                if options.noLaunch:
                    pass # no sleeping necessary
                elif options.serverType == DOCKER:
                    time.sleep(240)     # this is due to the docker port allocation race
                elif int(options.sleep_between_trigger) != 0:
                    time.sleep(int(options.sleep_between_trigger))
                else:
                    time.sleep(30)
            else:
                print('not enough servers at this time')
                time.sleep(POLL_INTERVAL)
        except Exception as e:
            print('have an exception')
            print((traceback.format_exc()))
            if 'descriptor' in locals():
                try:
                    print('Releasing servers for {} ...'.format(descriptor))
                    release_servers(options, descriptor)
                except Exception:
                    pass
            time.sleep(POLL_INTERVAL)
    # endwhile

    if not options.noLaunch:
        print('\n\n\ndone, everything is launched')
        for i in summary:
            print((i['test'], 'was launched at', i['time']))
        print("\n\n *** Dispatched total {} jobs using total {} servers and {} additional "
              "servers "
              "\n".format(total_jobs_count,
                          total_servers_being_used,
                          total_addl_servers_being_used ))
    else:
        print("\n Done!")
    return


def update_url_with_job_params(url, job_params):
    dict = {}
    newurl=''
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
        if newurl=='':
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
        release_url = "http://{}/releaseservers/{}/available".format(SERVER_MANAGER, descriptor)
        print('Release URL: {} '.format(release_url))
        response, content = httplib2.Http(timeout=TIMEOUT).request(release_url, 'GET')
        print('the release response', response, content)


if __name__ == "__main__":
    main()
