import getopt
import re
import subprocess
import sys
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
    "install_tasks": install_constants.DEFAULT_INSTALL_TASKS,
    "url": None,
    "debug_logs": False,
    "cb_edition": install_constants.CB_ENTERPRISE,
    "timeout": 300,
    "toy": None,
    "all_nodes_same_os": False,
    "skip_local_download": True,
    "init_nodes": True,
    "storage_mode": "plasma",
    "disable_consistency": False,
    "enable_ipv6": False,
    "use_domain_names": False
}


class build:
    def __init__(self, name, url, path, product="cb"):
        self.name = name
        self.url = url
        self.path = path
        self.product = product
        self.version = params["version"]


class NodeHelper:
    def __init__(self, node):
        self.node = node
        self.ip = node.ip
        self.params = params
        self.shell = RemoteMachineShellConnection(node)
        self.info = self.shell.extract_remote_info()
        self.build = None
        self.queue = None
        self.thread = None
        self.rest = None

    def get_services(self):
        if not self.node.services:
            return ["kv"]
        elif self.node.services:
            return self.node.services.split(',')

    def get_os(self):
        os = self.info.distribution_version.lower()
        to_be_replaced = ['\n', ' ', 'gnu/linux']
        for _ in to_be_replaced:
            if _ in os:
                os = os.replace(_, '')
        return os

    def uninstall_cb(self):
        if install_constants.CMDS[self.info.deliverable_type]["uninstall"]:
            cmd = install_constants.CMDS[self.info.deliverable_type]["uninstall"]
            if "msi" in cmd:
                '''WINDOWS UNINSTALL'''
                self.shell.terminate_processes(self.info, [s for s in testconstants.WIN_PROCESSES_KILLED])
                self.shell.terminate_processes(self.info, \
                                               [s + "-*" for s in testconstants.COUCHBASE_FROM_VERSION_3])
                installed_version, _ = self.shell.execute_command(
                    "cat " + install_constants.DEFAULT_INSTALL_DIR["WINDOWS_SERVER"] + "VERSION.txt")
                if len(installed_version) == 1:
                    installed_msi, _ = self.shell.execute_command(
                        "cd " + install_constants.DOWNLOAD_DIR["WINDOWS_SERVER"] + "; ls *" + installed_version[
                            0] + "*.msi")
                    if len(installed_msi) == 1:
                        self.shell.execute_command(
                            install_constants.CMDS[self.info.deliverable_type]["uninstall"].replace("installed-msi",
                                                                                                    installed_msi[0]))
                for browser in install_constants.WIN_BROWSERS:
                    self.shell.execute_command("taskkill /F /IM " + browser + " /T")
            else:
                self.shell.execute_command(cmd,
                                           debug=self.params["debug_logs"])

    def pre_install_cb(self):
        if install_constants.CMDS[self.info.deliverable_type]["pre_install"]:
            cmd = install_constants.CMDS[self.info.deliverable_type]["pre_install"]
            if "HDIUTIL_DETACH_ATTACH" in cmd:
                hdiutil_attach(self.shell, self.build.path, self.build.version)

    def install_cb(self):
        self.pre_install_cb()
        if install_constants.CMDS[self.info.deliverable_type]["install"]:
            if "suse" in self.get_os():
                cmd = install_constants.CMDS[self.info.deliverable_type]["suse_install"]
            else:
                cmd = install_constants.CMDS[self.info.deliverable_type]["install"]
            cmd = cmd.replace("buildbinary", self.build.name)
            cmd = cmd.replace("buildpath", self.build.path)
            cmd = cmd.replace("mountpoint", "/tmp/couchbase-server-" + params["version"])
            timeout = install_constants.WAIT_TIMES[self.info.deliverable_type]
            if not timeout:
                output, _ = self.shell.execute_command(cmd,
                                                       debug=self.params["debug_logs"])
            else:
                output, _ = self.shell.execute_command(cmd,
                                                       debug=self.params["debug_logs"],
                                                       timeout=install_constants.WAIT_TIMES[self.info.deliverable_type]
                                                       ["install"])
        self.post_install_cb()

    def post_install_cb(self):
        duration, event, timeout = install_constants.WAIT_TIMES[self.info.deliverable_type]["post_install"]
        start_time = time.time()
        while time.time() < start_time + timeout:
            if install_constants.CMDS[self.info.deliverable_type]["post_install"]:
                cmd = install_constants.CMDS[self.info.deliverable_type]["post_install"].replace("buildversion",
                                                                                                 self.build.version)
                o, e = self.shell.execute_command(cmd, debug=self.params["debug_logs"])
                if o == ['1']:
                    break
                else:
                    if install_constants.CMDS[self.info.deliverable_type]["post_install_retry"]:
                        if self.info.deliverable_type == "msi":
                            check_if_downgrade, _ = self.shell.execute_command(
                                "cd " + install_constants.DOWNLOAD_DIR["WINDOWS_SERVER"] +
                                "; vi +\"set nobomb | set fenc=ascii | x\" install_status.txt; "
                                "grep 'Adding WIX_DOWNGRADE_DETECTED property' install_status.txt")
                            print(check_if_downgrade * 10)
                        else:
                            self.shell.execute_command(
                                install_constants.CMDS[self.info.deliverable_type]["post_install_retry"],
                                debug=self.params["debug_logs"])
            self.wait_for_completion(duration, event)

    def post_init_cb(self):
        # Optionally change node name and restart server
        if params.get('use_domain_names', False):
            RemoteUtilHelper.use_hostname_for_server_settings(self.node)

        # Optionally disable consistency check
        if params.get('disable_consistency', False):
            self.rest.set_couchdb_option(section='couchdb',
                                         option='consistency_check_ratio',
                                         value='0.0')

    def init_cb(self):
        self.wait_for_completion(5, "Waiting for node to be initialized")
        # Initialize cluster
        self.rest = RestConnection(self.node)

        if params.get('enable_ipv6', 0):
            RemoteUtilHelper.use_hostname_for_server_settings(self.node)
            self.rest.enable_ip_version()
            if self.node.ip.startswith('['):
                status, content = self.rest.rename_node(
                    hostname=self.node.ip.replace('[', '').replace(']', ''))
                if status:
                    log.debug("Node {0} renamed to {1}".format(self.node.ip,
                                                               self.node.ip.replace('[', '').
                                                               replace(']', '')))
                else:
                    log.error("Error renaming node {0} to {1}: {2}".
                              format(self.node.ip,
                                     self.node.ip.replace('[', '').replace(']', ''),
                                     content))

        info = self.rest.get_nodes_self()
        kv_quota = int(info.mcdMemoryReserved * testconstants.CLUSTER_QUOTA_RATIO)
        if kv_quota < 256:
            kv_quota = 256
        log.debug("quota for kv: %s MB" % kv_quota)
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

        self.post_init_cb()

    def wait_for_completion(self, duration, event):
        log.debug(event.format(duration, self.ip))
        time.sleep(duration)

    def cleanup_cb(self):
        print("ls -td {0}*.{1} | awk 'NR>{2}' | xargs rm -f"
              .format(''.join(self.build.path.split('couchbase')[:-1]),
                      self.info.deliverable_type, 2))

        self.shell.execute_command(
            "ls -td {0}*.{1} | awk 'NR>{2}' | xargs rm -f"
                .format(''.join(self.build.path.split('couchbase')[:-1]),
                        self.info.deliverable_type, 2), debug=self.params["debug_logs"])


def _get_mounted_volumes(shell):
    volumes, _ = shell.execute_command("ls /tmp | grep '{0}'".format("couchbase-server-"))
    return volumes


def hdiutil_detach(shell, volumes, max_attempts=3):
    attempt = 0
    while len(volumes) > 0 and attempt <= max_attempts:
        """ Unmount existing app """
        for volume in volumes:
            shell.execute_command("hdiutil detach " + '"' + "/tmp/" + volume + '"', timeout=10)
        volumes = _get_mounted_volumes(shell)
        attempt += 1
    else:
        if len(volumes) > 0:
            log.warn("Unable to detach {0} after {1} attempts".format(volumes, max_attempts))
    log.debug("Done detaching Couchbase Server Volumes")


def hdiutil_attach(shell, dmg_path, max_attempts=3):
    volumes = _get_mounted_volumes(shell)
    hdiutil_detach(shell, volumes)

    attempt = 0
    while len(volumes) < 1 and attempt <= max_attempts:
        shell.execute_command("hdiutil attach {0} -mountpoint /tmp/{1}; sleep 10".format(dmg_path,
                                                                                         "couchbase-server-"
                                                                                         + params["version"]))
        volumes = _get_mounted_volumes(shell)
        attempt += 1
    else:
        if len(volumes) < 1:
            log.warn("Unable to attach {0} after {1} attempts".format(dmg_path, max_attempts))
    log.debug("Done attaching Couchbase Server Volumes")


def get_node_helper(ip):
    for node_helper in NodeHelpers:
        if node_helper.ip == ip:
            return node_helper


def print_error_and_exit(err=None):
    log.error(err)
    sys.exit()


def process_user_input():
    params = _parse_user_input()
    _params_validation()
    return params


def _parse_user_input():
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

    # Validate and extract remaining params
    for key, value in userinput.test_params.items():
        if key == "install_tasks":
            tasks = []
            for task in value.split('-'):
                if task in install_constants.DEFAULT_INSTALL_TASKS:
                    tasks.append(task)
            if len(tasks) > 0:
                params["install_tasks"] = tasks
            log.info("INSTALL TASKS: {0}".format(params["install_tasks"]))
            if params["install_tasks"] == ["uninstall"]:
                return params  # No other parameters needed
        if key == 'v' or key == "version":
            if re.match('^[0-9\.\-]*$', value) and len(value) > 5:
                params["version"] = value
        if key == "url":
            if value.startswith("http"):
                params["url"] = value
            else:
                log.warn("URL:{0} is not valid, will use version to locate build".format(value))
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
        if key == "debug_logs":
            params["debug_logs"] = True if value.lower() == "true" else False
        if key == "type" or key == "edition" and value.lower() in install_constants.CB_EDITIONS:
            params["edition"] = value.lower()
        if key == "timeout" and int(value) > 0:
            params["timeout"] = int(value)
        if key == "init_nodes":
            params["init_nodes"] = False if value.lower() == "false" else True
        if key == "storage_mode":
            params["storage_mode"] = value
        if key == "disable_consistency":
            params["disable_consistency"] = True if value.lower() == "true" else False
        if key == "skip_local_download":
            params["skip_local_download"] = False if value.lower() == "false" else True
        if key == "enable_ipv6":
            if value.lower() == "true":
                for server in params["servers"]:
                    if re.match('\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', server.ip):
                        print_error_and_exit(
                            "Cannot enable IPv6 on an IPv4 machine: {0}. Please run without enable_ipv6=True.".format(
                                server.ip))
                params["enable_ipv6"] = True

    # Validation based on 2 or more params
    if not "product" in userinput.test_params.keys():
        print_error_and_exit("No product specified. Please use the product parameter." + "\n" + install_constants.USAGE)

    if not params["version"] and not params["url"]:
        print_error_and_exit("Need valid build version or url to proceed")

    return params


def _params_validation():
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


# TODO: check if cb version is compatible with os
def _check_version_compatibility(node):
    pass


def pre_install_steps():
    if "install" in params["install_tasks"]:
        if params["url"] is not None:
            if NodeHelpers[0].shell.is_url_live(params["url"]):
                params["all_nodes_same_os"] = True
                for node in NodeHelpers:
                    build_binary = __get_build_binary_name(node)
                    build_url = params["url"]
                    filepath = __get_download_dir(node.get_os()) + build_binary
                    node.build = build(build_binary, build_url, filepath)
            else:
                print_error_and_exit("URL {0} is not live. Exiting.".format(params["url"]))
        else:
            for node in NodeHelpers:
                build_binary = __get_build_binary_name(node)
                build_url = __get_build_url(node, build_binary)
                if not build_url:
                    print_error_and_exit(
                        "Build is not present in latestbuilds or release repos, please check {0}".format(build_binary))
                filepath = __get_download_dir(node.get_os()) + build_binary
                node.build = build(build_binary, build_url, filepath)
        _download_build()


def _execute_local(command, timeout):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True).wait(timeout)
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
    latestbuilds_url = "{0}{1}/{2}/{3}".format(
        testconstants.CB_REPO,
        testconstants.CB_VERSION_NAME[(params["version"]).split('-')[0][:-2]],
        params["version"].split('-')[1],
        build_binary)
    release_url = "{0}{1}/{2}/{3}".format(
        testconstants.CB_RELEASE_REPO,
        testconstants.CB_VERSION_NAME[(params["version"]).split('-')[0][:-2]],
        params["version"].split('-')[1],
        build_binary)
    if node.shell.is_url_live(latestbuilds_url, exit_if_not_live=False):
        return latestbuilds_url
    elif node.shell.is_url_live(release_url, exit_if_not_live=False):
        return release_url
    return None


def _download_build():
    if params["all_nodes_same_os"] and not params["skip_local_download"]:
        build_url = NodeHelpers[0].build.url
        filepath = NodeHelpers[0].build.path
        timeout = install_constants.WAIT_TIMES[NodeHelpers[0].info.deliverable_type]["download_binary"]
        cmd = install_constants.WGET_CMD.format(__get_download_dir(NodeHelpers[0].get_os()), build_url)
        log.debug("Downloading build binary to {0}..".format(filepath))
        _execute_local(cmd, timeout)
        _copy_to_nodes(filepath, filepath)
    else:
        for node in NodeHelpers:
            build_url = node.build.url
            filepath = node.build.path
            cmd = install_constants.DOWNLOAD_CMD[node.info.deliverable_type]
            if "curl" in cmd:
                cmd = cmd.format(build_url, filepath,
                                 install_constants.WAIT_TIMES[node.info.deliverable_type]
                                 ["download_binary"])

            elif "wget" in cmd:
                cmd = cmd.format(__get_download_dir(node.get_os()), build_url)
            logging.info("Downloading build binary to {0}:{1}..".format(node.ip, filepath))
            check_and_retry_download_binary(cmd, node)
    log.debug("Done downloading build binary")


def check_file_exists(node, filepath):
    output, _ = node.shell.execute_command("ls -lh {0}".format(filepath), debug=params["debug_logs"])
    for line in output:
        if line.find('No such file or directory') == -1:
            return True
    return False


def check_and_retry_download_binary(cmd, node, retry=3):
    attempt = 0
    while attempt < retry:
        attempt += 1
        if check_file_exists(node, node.build.path):
            break
        else:
            time.sleep(install_constants.WAIT_TIMES[node.info.deliverable_type]["download_binary"])
            node.shell.execute_command(cmd, debug=params["debug_logs"])
    else:
        print_error_and_exit("Cannot download binary after {0} attempts, exiting".format(retry))


def __get_download_dir(os):
    if os in install_constants.LINUX_DISTROS:
        return install_constants.DOWNLOAD_DIR["LINUX_DISTROS"]
    elif os in install_constants.MACOS_VERSIONS:
        return install_constants.DOWNLOAD_DIR["MACOS_VERSIONS"]
    elif os in install_constants.WINDOWS_SERVER:
        return install_constants.DOWNLOAD_DIR["WINDOWS_SERVER"]


def __get_build_binary_name(node):
    # couchbase-server-enterprise-6.5.0-4557-centos7.x86_64.rpm
    # couchbase-server-enterprise-6.5.0-4557-suse15.x86_64.rpm
    # couchbase-server-enterprise-6.5.0-4557-rhel8.x86_64.rpm
    # couchbase-server-enterprise-6.5.0-4557-oel7.x86_64.rpm
    # couchbase-server-enterprise-6.5.0-4557-amzn2.x86_64.rpm
    if node.get_os() in install_constants.X86:
        return "{0}-{1}-{2}.{3}.{4}".format(params["cb_edition"],
                                            params["version"],
                                            node.get_os(),
                                            node.info.architecture_type,
                                            node.info.deliverable_type)

    # couchbase-server-enterprise_6.5.0-4557-ubuntu16.04_amd64.deb
    # couchbase-server-enterprise_6.5.0-4557-debian8_amd64.deb
    # couchbase-server-enterprise_6.5.0-4557-windows_amd64.msi
    elif node.get_os() in install_constants.AMD64:
        if "windows" in node.get_os():
            node.info.deliverable_type = "msi"
        return "{0}_{1}-{2}_{3}.{4}".format(params["cb_edition"],
                                            params["version"],
                                            node.get_os(),
                                            "amd64",
                                            node.info.deliverable_type)

    # couchbase-server-enterprise_6.5.0-4557-macos_x86_64.dmg
    elif node.get_os() in install_constants.MACOS_VERSIONS:
        return "{0}_{1}-{2}_{3}.{4}".format(params["cb_edition"],
                                            params["version"],
                                            "macos",
                                            node.info.architecture_type,
                                            node.info.deliverable_type)
