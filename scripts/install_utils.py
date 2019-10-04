import getopt
import re
import sys
import subprocess
import threading
import time

sys.path = [".", "lib"] + sys.path
import testconstants
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from membase.api.rest_client import RestConnection
import install_constants
import TestInput
import logging.config

logging.config.fileConfig("scripts.logging.conf")
log = logging.getLogger()

NodeHelpers = []
# Default params
params = {
    "version": None,
    "url": None,
    "debug_logs": False,
    "cb_edition": install_constants.CB_ENTERPRISE,
    "timeout": 300,
    "toy": None,
    "all_nodes_same_os": False,
    "skip_local_download": True,
    "init_nodes": True,
    "fts_quota": testconstants.FTS_QUOTA,
    "storage_mode": "plasma",
    "disable_consistency": False
}


class build:
    def __init__(self, name, url, path, product="cb"):
        self.name = name
        self.url = url
        self.path = path
        self.product = product


class NodeHelper:
    def __init__(self, node):
        self.node = node
        self.ip = node.ip
        self.params = params
        self.shell = RemoteMachineShellConnection(node)
        self.info = self.shell.extract_remote_info()
        self.build = None

    def get_services(self):
        if not self.node.services:
            return ["kv"]
        elif self.node.services:
            return self.node.services.split(',')

    def get_os(self):
        os = self.info.distribution_version.lower().replace(' ', '')
        os.replace("\n", '')
        return os

    def uninstall_cb(self):
        self.shell.execute_command(install_constants.CMDS[self.info.deliverable_type]["uninstall"])

    def pre_install_cb(self):
        pass

    def install_cb(self):
        self.pre_install_cb()
        self.shell.execute_command(install_constants.CMDS[self.info.deliverable_type]["install"]
                                   .format(self.build.path))
        self.post_install_cb()

    def post_install_cb(self):
        duration, event, timeout = install_constants.WAIT_TIMES[self.info.deliverable_type]["post_install"]
        self.wait_for_completion(duration, event)
        install_success = False
        start_time = time.time()
        while time.time() < start_time + timeout:
            o, e = self.shell.execute_command(install_constants.CMDS[self.info.deliverable_type]["post_install"])
            if o == ['1']:
                install_success = True
                break
            else:
                self.shell.execute_command(install_constants.CMDS[self.info.deliverable_type]["post_install_retry"])
                self.wait_for_completion(duration, event)
        if not install_success:
            print_error_and_exit("Install on {0} failed".format(self.ip))

    def init_cb(self):
        cluster_initialized = False
        start_time = time.time()
        while time.time() < start_time + 5 * 60:
            try:
                # Optionally change node name and restart server
                if params.get('use_domain_names', 0):
                    RemoteUtilHelper.use_hostname_for_server_settings(self.node)

                if params.get('enable_ipv6', 0):
                    status, content = self.shell.rename_node(
                        hostname=self.ip.replace('[', '').replace(']', ''))
                    if status:
                        log.info("Node {0} renamed to {1}".format(self.ip,
                                                                  self.ip.replace('[', '').
                                                                  replace(']', '')))
                    else:
                        log.error("Error renaming node {0} to {1}: {2}".
                                  format(self.ip,
                                         self.ip.replace('[', '').replace(']', ''),
                                         content))

                # Initialize cluster
                self.rest = RestConnection(self.node)
                info = self.rest.get_nodes_self()
                kv_quota = int(info.mcdMemoryReserved * testconstants.CLUSTER_QUOTA_RATIO)
                if kv_quota < 256:
                    kv_quota = 256
                log.info("quota for kv: %s MB" % kv_quota)
                self.rest.init_cluster_memoryQuota(self.node.rest_username, \
                                                   self.node.rest_password, \
                                                   kv_quota)
                if params["version"][:5] in testconstants.COUCHBASE_FROM_VULCAN:
                    self.rest.init_node_services(username=self.node.rest_username,
                                                 password=self.node.rest_password,
                                                 services=self.get_services())
                if "index" in self.get_services():
                    self.rest.set_indexer_storage_mode(storageMode=params["storage_mode"])

                self.rest.init_cluster(username=self.node.rest_username,
                                       password=self.node.rest_password)

                # Optionally disable consistency check
                if params["disable_consistency"]:
                    self.rest.set_couchdb_option(section='couchdb',
                                                 option='consistency_check_ratio',
                                                 value='0.0')
                cluster_initialized = True
                break
            except Exception as e:
                log.error("Exception thrown while initializing the cluster@{0}:\n{1}\n".format(self.ip, e.message))
            log.info('sleep for 5 seconds before trying again ...')
            time.sleep(5)
        if not cluster_initialized:
            sys.exit("unable to initialize couchbase node")

    def wait_for_completion(self, duration, event):
        log.info(event.format(duration))
        time.sleep(duration)

    def cleanup(self, err):
        self.shell.execute_command("rm -rf {0}".format(self.build.path))
        self.shell.stop_current_python_running(err)
        self.shell.disconnect()


def get_node_helper(ip):
    for node_helper in NodeHelpers:
        if node_helper.ip == ip:
            return node_helper


def print_error_and_exit(err=None):
    log.error(err)
    sys.exit("ERROR: " + err)


def process_user_input():
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'hi:p:', [])
        for o, a in opts:
            if o == "-h":
                print_error_and_exit(install_constants.USAGE)
        if len(sys.argv) <= 1:
            print_error_and_exit(install_constants.USAGE)
        userinput = TestInput.TestInputParser.get_test_input(sys.argv)
    except IndexError:
        print_error_and_exit(install_constants.USAGE)
    except getopt.GetoptError, err:
        print_error_and_exit(str(err))

    # Mandatory params
    if not userinput.servers:
        print_error_and_exit("No servers specified. Please use the -i parameter." + "\n" + install_constants.USAGE)
    else:
        params["servers"] = userinput.servers
    if not "product" in userinput.test_params.keys():
        print_error_and_exit("No product specified. Please use the product parameter." + "\n" + install_constants.USAGE)

    # Validate and extract remaining params
    for key, value in userinput.test_params.items():
        if key == 'v' or key == "version":
            if re.match('^[0-9\.\-]*$', value) and len(value) > 5:
                params["version"] = value
        if key == "url":
            if value.startswith("http"):
                params["url"] = value
        if key == "product":
            val = value.lower()
            if val in install_constants.SUPPORTED_PRODUCTS:
                params["product"] = val
            else:
                print_error_and_exit("Please specify valid product")
        if key == "toy":
            params["toy"] = value if len(value) > 1 else None
        if key == "openssl":
            params["openssl"] = int(value)
        # if key == "linux_repo":
        #     params["linux_repo"] = True if value.lower() == "true" else False
        if key == "debug_logs":
            params["debug_logs"] = True if value.lower() == "true" else False
        if key == "type" or key == "edition" and value.lower() in install_constants.CB_EDITIONS:
            params["edition"] = value.lower()
        if key == "timeout" and int(value) > 0:
            params["timeout"] = int(value)
        if key == "fts_quota" and int(value) > 0:
            params["fts_quota"] = int(value)
        if key == "init_nodes":
            params["init_nodes"] = False if value.lower() == "false" else True
        if key == "storage_mode":
            params["storage_mode"] = value
        if key == "disable_consistency":
            params["disable_consistency"] = True if value.lower() == "true" else False

    # Validation based on 2 or more params
    if not params["version"] and not params["url"]:
        print_error_and_exit("Need valid build version or url to proceed")

    # Create 1 NodeHelper instance per VM
    for server in params["servers"]:
        NodeHelpers.append(NodeHelper(server))

    # Version compatibility
    node_os = []
    for node in NodeHelpers:
        if node.get_os() not in install_constants.SUPPORTED_OS:
            print_error_and_exit("Install on {0} OS is not supported".format(node.get_os()))
        else:
            node_os.append(node.get_os())
    if len(set(node_os)) == 1:
        params["all_nodes_same_os"] = True
        _check_version_compatibility(NodeHelpers[0])
    else:
        for node in NodeHelpers:
            _check_version_compatibility(node)
    return params


# TODO: check if cb version is compatible with os
def _check_version_compatibility(node):
    pass


def pre_install_steps():
    _download_build()


def _execute_local(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
    process.communicate()[0].strip()


def __copy_thread(src_path, dest_path, node):
    logging.info("Copying %s to %s" % (src_path, node.ip))
    node.shell.copy_file_local_to_remote(src_path, dest_path)
    logging.info("Done copying build to %s.", node.ip)


def _copy_to_nodes(src_path, dest_path):
    copy_threads = []
    for node in NodeHelpers:
        copy_to_node = threading.Thread(target=__copy_thread, args=(src_path, dest_path, node))
        copy_threads.append(copy_to_node)
        copy_to_node.start()

    for thread in copy_threads:
        thread.join()


def __get_build_url(node, build_binary):
    return "http://{0}/builds/latestbuilds/couchbase-server/{1}/{2}/{3}".format(
        install_constants.CB_DOWNLOAD_SERVER,
        testconstants.CB_VERSION_NAME[(params["version"]).split('-')[0][:-2]],
        params["version"].split('-')[1],
        build_binary)


def _download_build():
    for node in NodeHelpers:
        build_binary = __get_build_binary_name(node)
        build_url = __get_build_url(node, build_binary)
        filepath = __get_download_dir(node.get_os()) + build_binary
        if __check_url_live(node, build_url):
            node.build = build(build_binary, build_url, filepath)
        else:
            print_error_and_exit("Build URL is not live, please check {0}".format(build_url))

    if params["all_nodes_same_os"] and not params["skip_local_download"]:
        build_url = NodeHelpers[0].build.url
        filepath = NodeHelpers[0].build.path
        cmd = install_constants.CURL_CMD.format(build_url, filepath,
                                                install_constants.WAIT_TIMES[NodeHelpers[0].info.deliverable_type]
                                                ["download_binary"])
        log.info("Downloading build binary to {0}..".format(filepath))
        _execute_local(cmd)
        _copy_to_nodes(filepath, filepath)
    else:
        for node in NodeHelpers:
            build_url = node.build.url
            filepath = node.build.path
            cmd = install_constants.CURL_CMD.format(build_url, filepath,
                                                    install_constants.WAIT_TIMES[node.info.deliverable_type]
                                                    ["download_binary"])
            logging.info("Downloading build binary to {0}:{1}..".format(node.ip, filepath))
            # node.shell.execute_command(cmd)
            node.shell.check_and_retry_download_binary(cmd, __get_download_dir(node.get_os()), build_binary)
    log.info("Done downloading build binary")

def __get_download_dir(os):
    if os in install_constants.LINUX_DISTROS:
        return install_constants.DOWNLOAD_DIR["LINUX_DISTROS"]
    elif os in install_constants.MACOS_VERSIONS:
        return install_constants.DOWNLOAD_DIR["MACOS_VERSIONS"]
    elif os in install_constants.WINDOWS_SERVER:
        return  install_constants.DOWNLOAD_DIR["WINDOWS_SERVER"]

def __get_build_binary_name(node):
    # couchbase-server-enterprise-6.5.0-4557-centos7.x86_64.rpm
    if "centos" in node.get_os():
        return "{0}-{1}-{2}.{3}.{4}".format(params["cb_edition"],
                                 params["version"],
                                 node.get_os(),
                                 node.info.architecture_type,
                                 node.info.deliverable_type)
    # couchbase-server-enterprise_6.5.0-4557-ubuntu16.04_amd64.deb
    elif "ubuntu" in node.get_os():
        return "{0}_{1}-{2}_{3}.{4}".format(params["cb_edition"],
                                            params["version"],
                                            node.get_os(),
                                            "amd64",
                                            node.info.deliverable_type)
    #couchbase-server-enterprise_6.5.0-4557-macos_x86_64.dmg
    elif node.get_os() in install_constants.MACOS_VERSIONS:
        return "{0}_{1}-{2}_{3}.{4}".format(params["cb_edition"],
                                            params["version"],
                                            "macos",
                                            node.info.architecture_type,
                                            node.info.deliverable_type)
    #couchbase-server-enterprise_6.5.0-4557-debian8_amd64.deb
    #couchbase-server-enterprise-6.5.0-4557-suse15.x86_64.rpm
    #couchbase-server-enterprise-6.5.0-4557-rhel8.x86_64.rpm
    #couchbase-server-enterprise-6.5.0-4557-oel7.x86_64.rpm
    #couchbase-server-enterprise-6.5.0-4557-amzn2.x86_64.rpm

def __check_url_live(node, url):
    url_valid = node.shell.is_url_live(url)
    return True if url_valid else False