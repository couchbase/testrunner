import getopt
import re
import subprocess
import sys
import threading
import time
import os
from time import sleep

from couchbase_helper.cluster import Cluster

from lib.membase.api.exception import ServerUnavailableException
from lib.membase.api.on_prem_rest_client import RestHelper

sys.path = [".", "lib"] + sys.path
import testconstants
from remote.remote_util import RemoteMachineShellConnection, RemoteUtilHelper
from membase.api.rest_client import RestConnection
import install_constants
import TestInput
import logging.config
import os.path
import urllib.request

from testconstants import CB_RELEASE_BUILDS

logging.config.fileConfig("scripts.logging.conf")
log = logging.getLogger()

NodeHelpers = []
# Default params
params = {
    "version": None,
    "install_tasks": install_constants.DEFAULT_INSTALL_TASKS,
    "url": None,
    "cb_non_package_installer_url":None,
    "debug_logs": False,
    "cb_edition": install_constants.CB_ENTERPRISE,
    "timeout": install_constants.INSTALL_TIMEOUT,
    "all_nodes_same_os": False,
    "skip_local_download": True,
    "storage_mode": "plasma",
    "disable_consistency": False,
    "enable_ipv6": False,
    "fts_quota": testconstants.FTS_QUOTA,
    "fts_query_limit": 0,
    "cluster_version": None,
    "bkrs_client": None,
    "ntp": False,
    "install_debug_info": False,
    "use_hostnames": False,
    "force_reinstall": True,

    # For serverless change
    # None - Leave it default
    # default - Explicitly load default profile
    # serverless - Load serverless profile
    "cluster_profile": None
}


class build:
    def __init__(self, name, url, path, product="cb",
                 debug_build_present=False, debug_name=None,
                 debug_url=None, debug_path=None):
        self.name = name
        self.url = url
        self.path = path
        self.product = product
        self.debug_build_present = debug_build_present
        self.debug_name = debug_name
        self.debug_url = debug_url
        self.debug_path = debug_path
        self.version = params["version"]
        self.bkrs_client = None


class NodeHelper:
    def __init__(self, node):
        self.node = node
        self.ip = node.ip
        self.params = params
        self.build = None
        self.queue = None
        self.thread = None
        self.rest = None
        self.install_success = None
        self.connect_ok = False
        self.shell = None
        self.info = None
        self.enable_ipv6 = False
        self.check_node_reachable()
        self.nonroot = self.shell.nonroot
        self.actions_dict = install_constants.NON_ROOT_CMDS if self.nonroot else install_constants.CMDS

    def check_node_reachable(self):
        start_time = time.time()
        # Try 3 times
        while time.time() < start_time + 60:
            try:
                self.shell = RemoteMachineShellConnection(self.node, exit_on_failure=False)
                self.info = self.shell.extract_remote_info()
                self.connect_ok = True
                if self.connect_ok:
                    break
            except Exception as e:
                log.warning("{0} unreachable, {1}, retrying..".format(self.ip, e))
                time.sleep(20)

    def print_node_stats(self, display_process_info=False):
        if self.get_os() in install_constants.LINUX_DISTROS:
            commands=["df -hl", "free -h"]
            if display_process_info:
                commands = commands + ["top -n1", "ps -aef"]
            for cmd in commands:
                out, _ =  self.shell.execute_command(cmd, get_pty=True)
                log.info("-" * 100)
                log.info("$ {}".format(cmd))
                for line in out:
                    log.info(line)
                log.info("-" * 100)


    def get_os(self):
        os = self.info.distribution_version.lower()
        to_be_replaced = ['\n', ' ', 'gnu/linux']
        for _ in to_be_replaced:
            if _ in os:
                os = os.replace(_, '')
        if self.info.deliverable_type == "dmg":
            major_version = os.split('.')
            os = major_version[0] + '.' + major_version[1]
        if self.info.distribution_type == "Amazon Linux 2":
            os = "amzn2"
        if self.info.distribution_type == "Amazon Linux 2023":
            os = "al2023"
        return os

    def uninstall_cb(self):
        need_nonroot_relogin = False
        if self.shell.nonroot:
            self.node.ssh_username = "root"
            self.shell = RemoteMachineShellConnection(self.node, exit_on_failure=False)
            need_nonroot_relogin = True
        if self.actions_dict[self.info.deliverable_type]["uninstall"]:
            cmd = self.actions_dict[self.info.deliverable_type]["uninstall"]
            if "suse" in self.get_os():
                cmd = self.actions_dict[self.info.deliverable_type]["suse_uninstall"]
            if "msi" in cmd:
                '''WINDOWS UNINSTALL'''
                self.shell.terminate_processes(self.info, [s for s in testconstants.WIN_PROCESSES_KILLED])
                self.shell.terminate_processes(self.info,
                                               [s + "-*" for s in testconstants.CB_RELEASE_BUILDS.keys()])
                installed_version, _ = self.shell.execute_command(
                    "cat " + install_constants.DEFAULT_INSTALL_DIR["WINDOWS_SERVER"] + "/VERSION.txt")
                if len(installed_version) == 1:
                    installed_msi, _ = self.shell.execute_command(
                        "cd " + install_constants.DOWNLOAD_DIR["WINDOWS_SERVER"] + "; ls *" + installed_version[
                            0] + "*.msi")
                    if len(installed_msi) == 1:
                        self.shell.execute_command(
                            self.actions_dict[self.info.deliverable_type]["uninstall"].replace("installed-msi",
                                                                                                    installed_msi[0]))
                for browser in install_constants.WIN_BROWSERS:
                    self.shell.execute_command("taskkill /F /IM " + browser + " /T")
            else:
                duration, event, timeout = install_constants.WAIT_TIMES[self.info.deliverable_type]["uninstall"]
                start_time = time.time()
                while time.time() < start_time + timeout:
                    try:
                        o, e = self.shell.execute_command(cmd, debug=self.params["debug_logs"])
                        if o == ['1']:
                            break
                        self.wait_for_completion(duration, event)
                    except Exception as e:
                        log.warning("uninstall_cb: Exception {0} occurred on {1}, "
                                    "retrying..".format(e, self.ip))
                        self.wait_for_completion(duration, event)
            self.shell.terminate_processes(self.info, install_constants.PROCESSES_TO_TERMINATE)

        if need_nonroot_relogin:
            self.node.ssh_username = "nonroot"
            self.shell = RemoteMachineShellConnection(self.node, exit_on_failure=False)

    def pre_install_cb(self):
        if self.actions_dict[self.info.deliverable_type]["pre_install"]:
            cmd = self.actions_dict[self.info.deliverable_type]["pre_install"]
            duration, event, timeout = install_constants.WAIT_TIMES[self.info.deliverable_type]["pre_install"]
            if cmd is not None:
                if "HDIUTIL_DETACH_ATTACH" in cmd:
                    start_time = time.time()
                    while time.time() < start_time + timeout:
                        try:
                            ret = hdiutil_attach(self.shell, self.build.path)
                            if ret:
                                break
                            self.wait_for_completion(duration, event)
                        except Exception as e:
                            log.warning("pre_install_cb: Exception {0} occurred on {1}, "
                                        "retrying..".format(e, self.ip))
                            self.wait_for_completion(duration, event)
                else:
                    self.shell.execute_command(cmd, debug=self.params["debug_logs"])
                    self.wait_for_completion(duration, event)

        # Serverless changes (MB-52406)
        if self.get_os() in install_constants.LINUX_DISTROS:
            key = "LINUX_DISTROS"

            # Remove the existing serverless_profile file (if any) on the node
            cmd = install_constants.RM_CONF_PROFILE_FILE[key]
            self.shell.execute_command(cmd)

            # Explicitly set the profile mode value (if provided by user)
            if params["cluster_profile"] in ["default", "serverless", "provisioned"]:
                cmd = install_constants.CREATE_SERVERLESS_PROFILE_FILE[key] \
                      % params["cluster_profile"]
                self.shell.execute_command(cmd)
        # End of Serverless changes

    def set_vm_swappiness_and_thp(self):
        # set vm_swapiness to 0, and thp to never by default
        # Check if this key is defined for this distribution/os
        if "set_vm_swappiness_and_thp" in self.actions_dict[self.info.deliverable_type]:
            try:
                cmd = self.actions_dict[self.info.deliverable_type]["set_vm_swappiness_and_thp"]
                o, e = self.shell.execute_command(cmd, debug=self.params["debug_logs"])
            except Exception as e:
                log.warning("Could not set vm swappiness/THP.Exception {0} occurred on {1} ".format(e, self.ip))

    def install_cb(self):
        self.pre_install_cb()
        self.set_vm_swappiness_and_thp()
        cmd_d = None
        cmd_debug = None
        if self.actions_dict[self.info.deliverable_type]["install"]:
            if "suse" in self.get_os():
                cmd = self.actions_dict[self.info.deliverable_type]["suse_install"]
                cmd_d = self.actions_dict[self.info.deliverable_type][
                    "suse_install"]
                cmd_debug = None
            else:
                cmd = self.actions_dict[self.info.deliverable_type]["install"]
                cmd_d = self.actions_dict[self.info.deliverable_type][
                    "install"]
                cmd_debug = None
            if self.nonroot:
                cb_non_package_installer_name = params["cb_non_package_installer_url"].split("/")[-1] \
                    if params["cb_non_package_installer_url"] \
                        else install_constants.CB_NON_PACKAGE_INSTALLER_NAME
                cmd = self.actions_dict[self.info.deliverable_type]["install"].format(cb_non_package_installer_name)
                cmd_d = self.actions_dict[self.info.deliverable_type]["install"].format(cb_non_package_installer_name)
                cmd_debug = None

            cmd = cmd.replace("buildbinary", self.build.name)
            cmd = cmd.replace("buildpath", self.build.path)
            cmd = cmd.replace("mountpoint", "/tmp/couchbase-server-" + params["version"])
            if self.get_os() in \
                    install_constants.DEBUG_INFO_SUPPORTED and cmd_d \
                    is not None and self.build.debug_build_present:
                cmd_debug = cmd_d.replace("buildpath",
                                          self.build.debug_path)
            duration, event, timeout = install_constants.WAIT_TIMES[self.info.deliverable_type]["install"]
            start_time = time.time()
            while time.time() < start_time + timeout:
                try:
                    o, e = self.shell.execute_command(cmd, debug=self.params["debug_logs"])
                    if is_fatal_error(e):
                        log.warning("Setting install status to failed due to {}".format(e))
                        self.install_success = False
                    if o == ['1']:
                        break
                    self.wait_for_completion(duration, event)
                except Exception as e:
                    log.warning("install_cb: Exception {0} occurred on {1}, retrying..".format(e,
                                                                                         self.ip))
                    self.wait_for_completion(duration, event)
            if cmd_debug is not None and self.build.debug_build_present:
                start_time = time.time()
                while time.time() < start_time + timeout:
                    try:
                        ou, er = self.shell.execute_command(cmd_debug,
                                                                  debug=
                                                                  self.params[
                                                                      "debug_logs"])
                        if ou == ['1']:
                            break
                        self.wait_for_completion(duration, event)
                    except Exception as e:
                        log.warning(
                            "Exception {0} occurred on {1}, retrying.."
                            .format(e, self.ip))
                        self.wait_for_completion(duration, event)
        self.post_install_cb()

    def post_install_cb(self):
        duration, event, timeout = install_constants.WAIT_TIMES[self.info.deliverable_type]["post_install"]
        start_time = time.time()
        while time.time() < start_time + timeout:
            try:
                if self.actions_dict[self.info.deliverable_type]["post_install"]:
                    cmd = self.actions_dict[self.info.deliverable_type]["post_install"].replace("buildversion", self.build.version)
                    o, e = self.shell.execute_command(cmd, debug=self.params["debug_logs"])
                    if o[-1] == '1':
                        break
                    else:
                        if self.actions_dict[self.info.deliverable_type]["post_install_retry"]:
                            if self.info.deliverable_type == "msi":
                                check_if_downgrade, _ = self.shell.execute_command(
                                    "cd " + install_constants.DOWNLOAD_DIR["WINDOWS_SERVER"] +
                                    "; cat install_status.txt | "
                                    "grep 'Adding WIX_DOWNGRADE_DETECTED property'")
                                print((check_if_downgrade * 10))
                            else:
                                self.shell.execute_command(
                                    self.actions_dict[self.info.deliverable_type]["post_install_retry"],
                                    debug=self.params["debug_logs"])
                        self.wait_for_completion(duration, event)
            except Exception as e:
                log.warning("post_install_cb: Exception {0} occurred on {1}, retrying..".format(e,
                                                                                               self.ip))
                self.wait_for_completion(duration, event)

    def set_cbft_env_options(self, name, value, retries=3):
        if self.get_os() in install_constants.LINUX_DISTROS:
            while retries > 0:
                if self.shell.file_exists("/opt/couchbase/bin/", "couchbase-server"):
                    ret, _ = self.shell.execute_command(install_constants.CBFT_ENV_OPTIONS[name].format(value))
                    self.shell.stop_server()
                    self.shell.start_server()
                    time.sleep(10)
                    if ret == ['1']:
                        log.info("{0} set to {1} on {2}".format(name, value, self.ip))
                        break
                else:
                    time.sleep(20)
                retries -= 1
            else:
                print_result_and_exit("Unable to set fts_query_limit on {0}".format(self.ip))

    def _get_cli_path(self):
        if self.nonroot:
            if self.get_os() in install_constants.LINUX_DISTROS:
                return install_constants.DEFAULT_NONROOT_CLI_PATH["LINUX_DISTROS"]
            elif self.get_os() in install_constants.MACOS_VERSIONS:
                return install_constants.DEFAULT_NONROOT_CLI_PATH["MACOS_VERSIONS"]
            elif self.get_os() in install_constants.WINDOWS_SERVER:
                return install_constants.DEFAULT_NONROOT_CLI_PATH["WINDOWS_SERVER"]
        else:
            if self.get_os() in install_constants.LINUX_DISTROS:
                return install_constants.DEFAULT_CLI_PATH["LINUX_DISTROS"]
            elif self.get_os() in install_constants.MACOS_VERSIONS:
                return install_constants.DEFAULT_CLI_PATH["MACOS_VERSIONS"]
            elif self.get_os() in install_constants.WINDOWS_SERVER:
                return install_constants.DEFAULT_CLI_PATH["WINDOWS_SERVER"]

    def _set_ip_version(self, retries=3):
        if params["enable_ipv6"]:
            self.enable_ipv6 = True
            if self.node.ip.startswith("["):
                hostname = self.node.ip[self.node.ip.find("[") + 1:self.node.ip.find("]")]
            else:
                hostname = self.node.ip
            cmd = install_constants.NODE_INIT["ipv6"].format(self._get_cli_path(),
                                                         self.ip,
                                                         hostname,
                                                         self.node.rest_username,
                                                         self.node.rest_password)
        else:
            if params["use_hostnames"]:
                ipv4_cmd = "ipv4_hostname"
            else:
                ipv4_cmd = "ipv4"
            cmd = install_constants.NODE_INIT[ipv4_cmd].format(self._get_cli_path(),
                                                            self.node.cluster_ip,
                                                            self.node.rest_username,
                                                            self.node.rest_password)
        while retries > 0:
            ret, err = self.shell.execute_command(cmd)
            if ret == ['0']:
                log.error(err)
                time.sleep(30)
                retries -= 1
            else:
                break
        else:
            log.warning("Unable to init node {0} due to {1}".format(self.ip, err))

    def pre_init_cb(self):
        try:
            self._set_ip_version()
            if params["fts_query_limit"] > 0:
                self.set_cbft_env_options("fts_query_limit", params["fts_query_limit"])
        except Exception as e:
            log.warning("pre_init_cb: Exception {0} occurred on {1}".format(e, self.ip))

    def post_init_cb(self):
        # Optionally disable consistency check
        if params.get('disable_consistency', False):
            self.rest.set_couchdb_option(section='couchdb',
                                         option='consistency_check_ratio',
                                         value='0.0')

    def get_services(self):
        if not self.node.services:
            return ["kv"]
        elif self.node.services:
            return self.node.services.split(',')

    def allocate_memory_quotas(self):
        kv_quota = 0

        start_time = time.time()
        while time.time() < start_time + 30 and kv_quota == 0:
            info = self.rest.get_nodes_self()
            kv_quota = int(info.mcdMemoryReserved * testconstants.CLUSTER_QUOTA_RATIO)
            time.sleep(1)

        self.services = self.get_services()
        if "index" in self.services:
            log.info("Setting INDEX memory quota as {0} MB on {1}".format(testconstants.INDEX_QUOTA, self.ip))
            self.rest.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=testconstants.INDEX_QUOTA)
            kv_quota -= testconstants.INDEX_QUOTA
        if "fts" in self.services:
            log.info("Setting FTS memory quota as {0} MB on {1}".format(params["fts_quota"], self.ip))
            self.rest.set_service_memoryQuota(service='ftsMemoryQuota', memoryQuota=params["fts_quota"])
            kv_quota -= params["fts_quota"]
        if "cbas" in self.services:
            log.info("Setting CBAS memory quota as {0} MB on {1}".format(testconstants.CBAS_QUOTA, self.ip))
            self.rest.set_service_memoryQuota(service="cbasMemoryQuota", memoryQuota=testconstants.CBAS_QUOTA)
            kv_quota -= testconstants.CBAS_QUOTA
        if "kv" in self.services:
            if kv_quota < testconstants.MIN_KV_QUOTA:
                log.warning("KV memory quota is {0}MB but needs to be at least {1}MB on {2}".format(kv_quota,
                                                                                                        testconstants.MIN_KV_QUOTA,
                                                                                                        self.ip))
                kv_quota = testconstants.MIN_KV_QUOTA
            log.info("Setting KV memory quota as {0} MB on {1}".format(kv_quota, self.ip))
        self.rest.init_cluster_memoryQuota(self.node.rest_username, self.node.rest_password, kv_quota)

    def wait_for_couchbase_reachable(self):
        duration, event, timeout = 5, "Waiting {0}s for {1} to be reachable..", 60
        start_time = time.time()
        log.info("Waiting for couchbase to be reachable")
        while time.time() < start_time + timeout:
            try:
                RestConnection(self.node)
                return
            except Exception as e:
                log.error(e)
                self.wait_for_completion(duration, event)
        raise Exception("Couchbase was not reachable after {}s".format(timeout))

    def init_cb(self):
        self.wait_for_couchbase_reachable()
        duration, event, timeout = install_constants.WAIT_TIMES[self.info.deliverable_type]["init"]
        start_time = time.time()
        while time.time() < start_time + timeout:
            try:
                init_success = False
                self.pre_init_cb()

                self.rest = RestConnection(self.node)

                if self.node.internal_ip:
                    self.rest.set_alternate_address(self.ip)

                # Make sure that data_path and index_path are writable by couchbase user
                for path in set([_f for _f in [self.node.data_path,
                                               self.node.index_path,
                                               self.node.cbas_path] if _f]):
                    for cmd in ("rm -rf %s/*" % path,
                                "chown -R couchbase:couchbase %s" % path):
                        self.shell.execute_command(cmd)
                self.rest.set_data_path(data_path=self.node.data_path,
                                        index_path=self.node.index_path,
                                        cbas_path=self.node.cbas_path)
                self.allocate_memory_quotas()
                self.rest.init_node_services(hostname=None,
                                             username=self.node.rest_username,
                                             password=self.node.rest_password,
                                             services=self.get_services())

                if "index" in self.get_services():
                    if params["cb_edition"] == install_constants.CB_COMMUNITY:
                        params["storage_mode"] = "forestdb"
                    self.rest.set_indexer_storage_mode(storageMode=params["storage_mode"])

                self.rest.init_cluster(username=self.node.rest_username,
                                       password=self.node.rest_password)
                init_success = True
                if init_success:
                    break
                self.wait_for_completion(duration, event)
            except Exception as e:
                log.warning("init_cb: Exception {0} occurred on {1}, retrying..".format(e, self.ip))
                self.wait_for_completion(duration, event)
        self.post_init_cb()

    def wait_for_completion(self, duration, event):
        if params["debug_logs"]:
            log.info(event.format(duration, self.ip))
        time.sleep(duration)

    def cleanup_cb(self):
        cmd = self.actions_dict[self.info.deliverable_type]["cleanup"]
        if cmd:
            try:
                # Delete all but the most recently accessed build binaries
                self.shell.execute_command(cmd, debug=self.params["debug_logs"])
            except:
                #ok to ignore
                pass

def _get_mounted_volumes(shell):
    volumes, _ = shell.execute_command("ls /tmp | grep '{0}'".format("couchbase-server-"))
    return volumes


def hdiutil_attach(shell, dmg_path):
    volumes = _get_mounted_volumes(shell)
    for volume in volumes:
        shell.execute_command("hdiutil detach " + '"' + "/tmp/" + volume + '"')
        shell.execute_command("umount " + '"' + "/tmp/" + volume + '"')

    shell.execute_command("hdiutil attach {0} -mountpoint /tmp/{1}".
                          format(dmg_path, "couchbase-server-" + params["version"]))
    return shell.file_exists("/tmp/", "couchbase-server-" + params["version"])


def check_if_version_already_installed(server, install_version, cb_edition,
                                       cluster_profile):
    if cluster_profile is None:
        cluster_profile = "default"
    try:
        pools_info = RestConnection(server, timeout=3).get_pools_info()
        existing_ver, existing_build_num, existing_edition = pools_info[
            "implementationVersion"].split("-")
        if "%s-%s" % (existing_ver, existing_build_num) == install_version \
                and cb_edition == existing_edition:
            if float(ver[:3]) >= 7.5:
                if cluster_profile != pools_info["configProfile"]:
                    return False
            return True
    except (ServerUnavailableException, Exception):
        pass
    return False


def reinit_node(server):
    rest = RestConnection(server)
    data_path = rest.get_data_path()
    version = rest.get_nodes_self().version
    if float(version[:3]) >= 7.6:
        rest.reset_node()
    else:
        shell = RemoteMachineShellConnection(server)
        shell.stop_couchbase()
        count = 0
        while shell.is_couchbase_running() and count < 10:
            time.sleep(2)
            print("Waiting for 2 sec to check if Couchbase is still running")
            count += 2

        # Delete Config
        shell.cleanup_data_config(data_path)

        # Start Couchbase
        shell.start_couchbase()
    is_running = RestHelper(rest).is_ns_server_running(timeout_in_seconds=60)
    if not is_running:
        return False
    return True


def get_node_helper(ip):
    for node_helper in NodeHelpers:
        if node_helper.ip == ip:
            return node_helper
    return None

def check_nodes_statuses(servers, display_process_info=False):
    for server in servers:
        node = get_node_helper(server.ip)
        node.check_node_reachable()
        if node.connect_ok:
            node.print_node_stats(display_process_info=display_process_info)

def print_result_and_exit(err=None):
    if err:
        log.error(err)
    success = []
    fail = []
    install_not_started = []
    for server in params["servers"]:
        node = get_node_helper(server.ip)
        if not node or not node.install_success:
            if node.install_success is False:
                fail.append(server)
            else:
                install_not_started.append(server)
        elif node.install_success:
            success.append(server)

    # Print node status if install failed or not started
    # Commenting as the commands print a lot of lines
    # TODO - Redirect the info to a file
    # if len(fail) > 0 or len(install_not_started) > 0:
    #     check_nodes_statuses(fail + install_not_started,
    #                          display_process_info=True)

    log.info("-" * 100)
    for server in install_not_started:
        log.error("INSTALL NOT STARTED ON: \t{0}".format(server.ip))
    log.info("-" * 100)
    for server in fail:
        log.error("INSTALL FAILED ON: \t{0}".format(server.ip))
    log.info("-" * 100)
    for server in success:
        log.info("INSTALL COMPLETED ON: \t{0}".format(server.ip))
    log.info("-" * 100)
    if len(fail) > 0 or len(install_not_started) > 0:
        sys.exit(1)


def process_user_input():
    params = _parse_user_input()
    _params_validation()
    return params


def _parse_user_input():
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'hi:p:', [])
        for o, a in opts:
            if o == "-h":
                print_result_and_exit(install_constants.USAGE)
        if len(sys.argv) <= 1:
            print_result_and_exit(install_constants.USAGE)
        userinput = TestInput.TestInputParser.get_test_input(sys.argv)
    except IndexError:
        print_result_and_exit(install_constants.USAGE)
    except getopt.GetoptError as err:
        print_result_and_exit(str(err))

    # Mandatory params
    if not userinput.servers:
        print_result_and_exit("No servers specified. Please use the -i parameter." + "\n" + install_constants.USAGE)
    else:
        params["servers"] = userinput.servers
        params["clusters"] = userinput.clusters

    if userinput.bkrs_client:
        params["bkrs_client"] = userinput.bkrs_client
        params["servers"].append(params["bkrs_client"])

    # Validate and extract remaining params
    for key, value in list(userinput.test_params.items()):
        if key == "debug_logs":
            params["debug_logs"] = True if value.lower() == "true" else False
        if key == "install_tasks":
            tasks = []
            for task in value.split('-'):
                if task in install_constants.DEFAULT_INSTALL_TASKS and task not in tasks:
                    tasks.append(task)
                if task == "tools":
                    tasks.append(task)
            if len(tasks) > 0:
                params["install_tasks"] = tasks
            log.info("INSTALL TASKS: {0}".format(params["install_tasks"]))
            if "install" not in params["install_tasks"] and "init" not in params["install_tasks"]:
                return params  # No other parameters needed
        if key == 'v' or key == "version":
            if re.match('^[0-9\.\-]*$', value) and len(value) > 5:
                params["version"] = value
        if key == "force_reinstall":
            params["force_reinstall"] = True \
                if value.lower() == "true" else False
        if key == "url":
            if value.startswith("http"):
                params["url"] = value
            else:
                log.warning('URL:{0} is not valid, will use version to locate build'.format(value))
        if key == "cb_non_package_installer_url":
            if value.startswith("http"):
                params["cb_non_package_installer_url"] = value
            else:
                log.warning('URL:{0} is not valid, will use default url to locate installer'.format(value))
        if key == "type" or key == "edition":
            if "community" in value.lower():
                params["cb_edition"] = install_constants.CB_COMMUNITY
        if key == "timeout" and int(value) > 60:
            params["timeout"] = int(value)
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
                        print_result_and_exit(
                            "Cannot enable IPv6 on an IPv4 machine: {0}. Please run without enable_ipv6=True.".format(
                                server.ip))
                params["enable_ipv6"] = True
        if key == "fts_quota" and int(value) >= 256:
            params["fts_quota"] = int(value)
        if key == "fts_query_limit" and int(value) > 0:
            params["fts_query_limit"] = int(value)
        if key == "variant":
            params["variant"] = value
        if key == "cluster_version":
            params["cluster_version"] = value
        if key == "ntp":
            params["ntp"] = value
        if key == "init_clusters":
            params["init_clusters"] = True if value.lower() == "true" else False
        if key == "install_debug_info":
            params['install_debug_info'] = True if value.lower() == \
                                                   "true" else False
        if key == "use_hostnames" or key == "h":
            params["use_hostnames"] = value.lower() == "true"
        if key == "cluster_profile":
            params["cluster_profile"] = value

    if userinput.bkrs_client and not params["cluster_version"]:
        print_result_and_exit("Need 'cluster_version' in params to proceed")

    if not params["version"] and not params["url"]:
        print_result_and_exit("Need valid build version or url to proceed")

    return params


def __check_servers_reachable():
    reachable = []
    unreachable = []
    for server in params["servers"]:
        try:
            RemoteMachineShellConnection(server, exit_on_failure=False)
            reachable.append(server.ip)
        except Exception as e:
            log.error(e)
            unreachable.append(server.ip)

    if len(unreachable) > 0:
        log.info("-" * 100)
        for _ in unreachable:
            log.error("INSTALL FAILED ON: \t{0}".format(_))
        log.info("-" * 100)
        for _ in reachable:
            # Marking this node as "completed" so it is not moved to failedInstall state
            log.info("INSTALL COMPLETED ON: \t{0}".format(_))
        log.info("-" * 100)
        sys.exit(1)


def _params_validation():
    __check_servers_reachable()

    # Create 1 NodeHelper instance per VM
    for server in params["servers"]:
        NodeHelpers.append(NodeHelper(server))

    # Version compatibility
    node_os = []
    for node in NodeHelpers:
        if node.get_os() not in install_constants.SUPPORTED_OS:
            print_result_and_exit("Install on {0} OS is not supported".format(node.get_os()))
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


def pre_install_steps(n_helpers):
    # CBQE-6402
    if "ntp" in params and params["ntp"]:
        ntp_threads = []
        for node in n_helpers:
            ntp_thread = threading.Thread(target=node.shell.is_ntp_installed)
            ntp_threads.append(ntp_thread)
            ntp_thread.start()
        for ntpt in ntp_threads:
            ntpt.join()

    if "tools" in params["install_tasks"]:
        for node in n_helpers:
            tools_name = __get_tools_package_name(node)
            node.tools_name = tools_name
            tools_url = __get_tools_url(node, tools_name)
            node.tools_url = tools_url
    if "install" in params["install_tasks"] and n_helpers:
        if params["url"] is not None:
            if n_helpers[0].shell.is_url_live(params["url"]):
                params["all_nodes_same_os"] = True
                for node in n_helpers:
                    build_binary = __get_build_binary_name(node)
                    build_url = params["url"]
                    filepath = __get_download_dir(node) + build_binary
                    node.build = build(build_binary, build_url, filepath)
            else:
                print_result_and_exit("URL {0} is not live. Exiting.".format(params["url"]))
        else:
            install_debug_info = params["install_debug_info"]
            is_release_build = check_for_release_or_latest_build()
            for node in n_helpers:
                build_binary = __get_build_binary_name(node, is_release_build)
                debug_binary = __get_debug_binary_name(node, is_release_build) if \
                    install_debug_info else None
                build_url = __get_build_url(node, build_binary)
                debug_url = __get_build_url(node, debug_binary) if \
                    install_debug_info else None
                debug_build_present = install_debug_info
                if not build_url:
                    print_result_and_exit(
                        "Build is not present in latestbuilds or release repos, please check {0}".format(build_binary))
                if not debug_url and install_debug_info:
                    print("Debug info build not present. Debug info "
                          "build will not be installed for this run.")
                    debug_build_present = False
                filepath = __get_download_dir(node) + build_binary
                filepath_debug = __get_download_dir(node) + \
                                 debug_binary if debug_binary else None
                node.build = build(build_binary, build_url, filepath,
                                   debug_build_present=debug_build_present,
                                   debug_name=debug_binary,
                                   debug_url=debug_url,
                                   debug_path=filepath_debug)

def check_for_release_or_latest_build():
    cb_version = params["version"].split('-')[0]
    cb_build = params["version"].split('-')[1]
    if cb_version in CB_RELEASE_BUILDS \
         and CB_RELEASE_BUILDS[cb_version] == cb_build:
            return True
    else:
        return False


def _execute_local(command, timeout):
    # -- Uncomment the below 2 lines for python 3
    # process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True).wait(timeout)
    # process.communicate()[0].strip()
    # -- python 2
    returncode = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True).wait()
    return returncode


def __copy_thread(src_path, dest_path, node):
    logging.info("Copying %s to %s" % (src_path, node.ip))
    node.shell.copy_file_local_to_remote(src_path, dest_path)
    logging.info("Done copying build to %s.", node.ip)


def _copy_to_nodes(node_helpers, debug=False):
    copy_threads = []
    for node in node_helpers:
        if debug:
            src_path = node.build.debug_path
        else:
            src_path = node.build.path
        dst_path = __get_download_dir(node, disregard_skip_local_download=True) \
                   + node.build.name
        node.build.path = dst_path
        # don't copy if the file already exists and size matches
        if check_file_size(node, debug):
            continue
        copy_to_node = threading.Thread(target=__copy_thread, args=(src_path, dst_path, node))
        copy_threads.append(copy_to_node)
        copy_to_node.start()

    for thread in copy_threads:
        thread.join()


def __get_build_url(node, build_binary):
    cb_version = params["version"]
    if params["bkrs_client"]:
        if node.ip != params["bkrs_client"].ip:
            cb_version = params["cluster_version"]
    latestbuilds_url = "{0}{1}/{2}/{3}".format(
        testconstants.CB_REPO,
        testconstants.CB_VERSION_NAME[cb_version.split('-')[0][:-2]],
        cb_version.split('-')[1],
        build_binary)
    release_url = "{0}{1}/{2}".format(
        testconstants.CB_RELEASE_REPO,
        cb_version.split('-')[0],
        build_binary)
    if node.shell.is_url_live(latestbuilds_url, exit_if_not_live=False):
        return latestbuilds_url
    elif node.shell.is_url_live(release_url, exit_if_not_live=False):
        return release_url
    return None

def __get_tools_url(node, tools_package):
    cb_version = params["version"]
    latestbuilds_url = "{0}{1}/{2}/{3}".format(
        testconstants.CB_REPO,
        testconstants.CB_VERSION_NAME[cb_version.split('-')[0][:-2]],
        cb_version.split('-')[1],
        tools_package)
    if node.shell.is_url_live(latestbuilds_url, exit_if_not_live=False):
        return latestbuilds_url
    return None


def download_build(node_helpers):
    if not node_helpers:
        # List is empty meaning no node needs download process
        return
    log.debug("Downloading the builds now")
    all_nodes_same_version = len(set([node.build.url for node in node_helpers])) == 1
    if params["all_nodes_same_os"] and all_nodes_same_version and not params["skip_local_download"]:
        check_and_retry_download_binary_local(node_helpers[0])
        _copy_to_nodes(node_helpers, debug=False)
        if node_helpers[0].build.debug_build_present:
            _copy_to_nodes(node_helpers, debug=True)
        ok = True
        for node in node_helpers:
            if not check_file_exists(node, node.build.path) \
                    or not check_file_size(node):
                node.install_success = False
                ok = False
                log.error("Unable to copy build to {} at {}, exiting"
                          .format(node.ip, node.build.path))
        if not ok:
            print_result_and_exit()
        for node in node_helpers:
            if node.build.debug_build_present:
                if not check_file_exists(node, node.build.debug_path) \
                        or not check_file_size(node, debug_build=True):
                    node.install_success = False
                    ok = False
                    log.error("Unable to copy debug build to {} at {}, exiting"
                              .format(node.ip, node.build.debug_path))
        if not ok:
            print_result_and_exit()
    else:
        for node in node_helpers:
            build_url = node.build.url
            filepath = node.build.path
            debug_url = node.build.debug_url if \
                node.build.debug_build_present else None
            filepath_debug = node.build.debug_path if \
                node.build.debug_build_present else None
            cmd_master = install_constants.DOWNLOAD_CMD[
                node.info.deliverable_type]
            cmd = None
            cmd_debug = None
            if "curl" in cmd_master:
                cmd = cmd_master.format(build_url, filepath,
                                        install_constants.WAIT_TIMES[
                                            node.info.deliverable_type]
                                        ["download_binary"])
                if node.build.debug_build_present:
                    cmd_debug = cmd_master.format(debug_url, filepath_debug,
                                                  install_constants.WAIT_TIMES[
                                                      node.info.deliverable_type]
                                                  ["download_binary"])
                else:
                    cmd_debug = None
            elif "wget" in cmd_master:
                cmd = cmd_master.format(__get_download_dir(node),
                                        node.build.name,
                                        build_url)
                if node.build.debug_build_present:
                    cmd_debug = cmd_master.format(__get_download_dir(node),
                                                  node.build.debug_name,
                                                  debug_url)
                else:
                    cmd_debug = None
            if cmd:
                check_and_retry_download_binary(cmd, node,
                                                node.build.path)
            if cmd_debug and node.build.debug_build_present:
                check_and_retry_download_binary(cmd_debug, node,
                                                node.build.debug_path
                                                , debug_build=True)
    log.debug("Done downloading build binary")
    for node in node_helpers:
        if node.shell.nonroot:
            download_cb_non_package_installer()


def download_cb_non_package_installer():
    log.debug("Downloading install script now")
    for node in NodeHelpers:
        cmd_master = install_constants.DOWNLOAD_CMD[node.info.deliverable_type]
        download_dir =  __get_download_dir(node, disregard_skip_local_download=True)
        url = params["cb_non_package_installer_url"] if params["cb_non_package_installer_url"] else install_constants.CB_NON_PACKAGE_INSTALLER_URL
        cb_non_package_installer_name = params["cb_non_package_installer_url"].split("/")[-1] \
            if params["cb_non_package_installer_url"] \
                else install_constants.CB_NON_PACKAGE_INSTALLER_NAME
        if "curl" in cmd_master:
            cmd = cmd_master.format(url,
                                    download_dir)
        elif "wget" in cmd_master:
            cmd = cmd_master.format(download_dir,
                                    cb_non_package_installer_name,
                                    url)
        if cmd:
            node.shell.execute_command(cmd, debug=True)
        node.shell.execute_command("chmod a+x {0}{1}".format(download_dir,
                                                             cb_non_package_installer_name))


def install_tools(node_helpers):
    log.debug("Downloading the tools package now")
    for node in node_helpers:
        cmd_master = install_constants.DOWNLOAD_CMD[node.info.deliverable_type]
        download_dir = __get_download_dir(node)
        if "curl" in cmd_master:
            cmd = cmd_master.format(node.tools_url, download_dir)
        elif "wget" in cmd_master:
            cmd = cmd_master.format(download_dir, node.tools_name, node.tools_url)
        if cmd:
            node.shell.execute_command(cmd, debug=True)

        if "windows" in node.get_os():
            install_cmd = "unzip {0} -d {1}"
        else:
            install_cmd = "tar -xzf {0} -C {1}"

        install_dir = "/tmp/tools_package"
        if "windows" in node.get_os():
            install_dir = f"/cygdrive/c{install_dir}"
        node.shell.execute_command(f"mkdir -p {install_dir}", debug=True)

        cmd = install_cmd.format(f"{download_dir}/{node.tools_name}", install_dir)
        node.shell.execute_command(cmd, debug=True)
        node.shell.execute_command(f"rm {download_dir}/{node.tools_name}")


def check_and_retry_download_binary_local(node):
    log.info("Downloading build binary to {0}..".format(node.build.path))
    if node.build.debug_build_present:
        log.info("Downloading debug binary to {0}..".format(
            node.build.debug_path))
    duration, event, timeout = install_constants.WAIT_TIMES[node.info.deliverable_type][
        "download_binary"]
    cmd = install_constants.WGET_CMD.format(__get_download_dir(node),
                                            node.build.name,
                                            node.build.url)
    cmd_debug = None
    if node.build.debug_build_present:
        cmd_debug = install_constants.WGET_CMD.format(
            __get_download_dir(node),
            node.build.debug_name,
            node.build.debug_url)
    start_time = time.time()
    while time.time() < start_time + timeout:
        try:
            log.info("Executing cmd on local : {}".format(cmd))
            exit_code = _execute_local(cmd, timeout)
            if exit_code == 0 and os.path.exists(node.build.path):
                break
            time.sleep(duration)
        except Exception as e:
            log.warn("Unable to download build: {0}, retrying..".format(e.message))
            time.sleep(duration)
    else:
        print_result_and_exit("Unable to download build in {0}s on {1}, exiting".format(timeout,
                                                                                        node.build.path))
    if node.build.debug_build_present:
        # Download debug info build
        start_time = time.time()
        while time.time() < start_time + timeout:
            try:
                exit_code_debug = 1
                if cmd_debug:
                    exit_code_debug = _execute_local(cmd_debug, timeout)
                if exit_code_debug == 0 and os.path.exists(
                        node.build.debug_path):
                    break
                time.sleep(duration)
            except Exception as e:
                log.warn("Unable to download build: {0}, "
                         "retrying..".format(e))
                time.sleep(duration)
        else:
            print_result_and_exit("Unable to download debug build in "
                                  "{0}s on {1}, exiting".format(
                timeout, node.build.debug_path))


def check_file_exists(node, filepath):
    output, _ = node.shell.execute_command("ls -lh {0}".format(filepath), debug=params["debug_logs"])
    for line in output:
        if line.find('No such file or directory') == -1:
            return True
    return False


def get_remote_build_size(node, debug_build=False):
    if debug_build:
        url = node.build.debug_url
    else:
        url = node.build.url
    response = urllib.request.urlopen(url)
    remote_build_size = int(response.info()["Content-Length"])
    return remote_build_size


def get_local_build_size(node, debug_build=False):
    if debug_build:
        binary_name= node.build.debug_name
    else:
        binary_name = node.build.name
    output, _ = node.shell.execute_command(
        install_constants.LOCAL_BUILD_SIZE_CMD.format(
            __get_download_dir(node, disregard_skip_local_download=True),
            binary_name))
    local_build_size = int(output[0].strip().split(" ")[0])
    return local_build_size


def check_file_size(node, debug_build=False):
    try:
        expected_size = get_remote_build_size(node, debug_build)
        actual_size = get_local_build_size(node, debug_build)
        return expected_size == actual_size
    except Exception:
        return False


def check_and_retry_download_binary(cmd, node, path, debug_build=False):
    if "amazonaws" in cmd:
        cmd += " --no-check-certificate "
    duration, event, timeout = install_constants.WAIT_TIMES[node.info.deliverable_type]["download_binary"]
    start_time = time.time()
    while time.time() < start_time + timeout:
        try:
            _, _, download_exit_code = node.shell.execute_command(cmd, debug=params["debug_logs"], get_exit_code=True)
            if download_exit_code == 0 \
                    and check_file_size(node, debug_build) \
                    and check_file_exists(node, path):
                break
            time.sleep(duration)
        except Exception as e:
            log.warning("Unable to download build: {0}, retrying..".format(e))
            time.sleep(duration)
    else:
        print_result_and_exit("Unable to download build in {0}s on {1}, exiting".format(timeout, node.ip))


def __get_download_dir(node, disregard_skip_local_download=False):
    os = node.get_os()
    if os in install_constants.LINUX_DISTROS:
        if node.shell.nonroot and \
            (params["skip_local_download"] or disregard_skip_local_download):
            return install_constants.NON_ROOT_DOWNLOAD_DIR['LINUX_DISTROS']
        else:
            return install_constants.DOWNLOAD_DIR["LINUX_DISTROS"]
    elif os in install_constants.MACOS_VERSIONS:
        return install_constants.DOWNLOAD_DIR["MACOS_VERSIONS"]
    elif os in install_constants.WINDOWS_SERVER:
        return install_constants.DOWNLOAD_DIR["WINDOWS_SERVER"]


def __get_build_binary_name(node, is_release_build=False):
    # couchbase-server-enterprise-6.5.0-4557-centos7.x86_64.rpm
    # couchbase-server-enterprise-6.5.0-4557-suse15.x86_64.rpm
    # couchbase-server-enterprise-6.5.0-4557-rhel8.x86_64.rpm
    # couchbase-server-enterprise-6.5.0-4557-oel7.x86_64.rpm
    # couchbase-server-enterprise-6.5.0-4557-amzn2.x86_64.rpm
    #All the above ones replaced by couchbase-server-enterprise-6.5.0-4557-linux.x86_64.rpm for 7.1 and above

    cb_version = params["version"]
    if is_release_build:
        cb_version = cb_version.split("-")[0]

    if params["bkrs_client"]:
        if node.ip != params["bkrs_client"].ip:
            cb_version = params["cluster_version"]
    if node.get_os() in install_constants.X86:
        if float(cb_version[:3]) < 7.1 :
            return "{0}-{1}-{2}{3}.{4}.{5}".format(params["cb_edition"],
                                            cb_version,
                                            node.get_os(),
                                            "-" + params["variant"] if "variant" in params else "",
                                            node.info.architecture_type,
                                            node.info.deliverable_type)
        return "{0}-{1}-{2}{3}.{4}.{5}".format(params["cb_edition"],
                                            cb_version,
                                            "linux",
                                            "-" + params["variant"] if "variant" in params else "",
                                            node.info.architecture_type,
                                            node.info.deliverable_type)

    # couchbase-server-enterprise_6.5.0-4557-ubuntu16.04_amd64.deb
    # couchbase-server-enterprise_6.5.0-4557-debian8_amd64.deb
    #All the above ones replaced by couchbase-server-enterprise-6.5.0-4557-linux_amd64.deb for 7.1 and above
    elif node.get_os() in install_constants.LINUX_AMD64:
        if node.get_os() in install_constants.LINUX_DISTROS and node.info.architecture_type == "aarch64":
            return "{0}_{1}-{2}_{3}.{4}".format(params["cb_edition"],
                                                cb_version,
                                                "linux",
                                                "arm64",
                                                node.info.deliverable_type)
        if float(cb_version[:3]) < 7.1 :
            return "{0}_{1}-{2}_{3}.{4}".format(params["cb_edition"],
                                            cb_version,
                                            node.get_os(),
                                            "amd64",
                                            node.info.deliverable_type)
        return "{0}_{1}-{2}_{3}.{4}".format(params["cb_edition"],
                                            cb_version,
                                            "linux",
                                            "amd64",
                                            node.info.deliverable_type)

    # couchbase-server-enterprise_6.5.0-4557-windows_amd64.msi
    elif node.get_os() in install_constants.WINDOWS_SERVER:
        if "windows" in node.get_os():
            node.info.deliverable_type = "msi"
        return "{0}_{1}-{2}_{3}.{4}".format(params["cb_edition"],
                                            cb_version,
                                            node.get_os(),
                                            "amd64",
                                            node.info.deliverable_type)

    # couchbase-server-enterprise_6.5.0-4557-macos_x86_64.dmg
    elif node.get_os() in install_constants.MACOS_VERSIONS:
        return "{0}_{1}-{2}_{3}-{4}.{5}".format(params["cb_edition"],
                                            cb_version,
                                            "macos",
                                            node.info.architecture_type,
                                            "unnotarized",
                                            node.info.deliverable_type)

def __get_tools_package_name(node):
    cb_version = params["version"]
    # couchbase-server-tools_7.1.1-3103-windows_amd64.zip
    if "windows" in node.get_os():
        return f"couchbase-server-tools_{cb_version}-windows_amd64.zip"
    # couchbase-server-tools_7.1.1-3103-macos_x86_64.tar.gz
    if node.get_os() in install_constants.MACOS_VERSIONS:
        return f"couchbase-server-tools_{cb_version}-macos_x86_64.tar.gz"
    # Default to linux, since we don't differentiate between x86 and amd64 distros
    # couchbase-server-tools_7.1.1-3103-linux_x86_64.tar.gz
    return f"couchbase-server-tools_{cb_version}-linux_x86_64.tar.gz"

def is_fatal_error(errors):
    for line in errors:
        for fatal_error in install_constants.FATAL_ERRORS:
            if fatal_error in line:
                return True
    return False

def init_clusters(timeout=60, retries=3):
    """ For each cluster defined in the config with more than 1 node, add the nodes to form a cluster
    """

    force_stop = time.time() + timeout
    # only need to init clusters with more than 1 node
    todo = list(filter(lambda cluster: len(cluster) > 1, params["clusters"].values()))

    while len(todo) > 0 and retries > 0:
        tasks = []

        for servers in todo:
            cluster = Cluster()
            try:
                # add rest of nodes to first node and rebalance to form a cluster
                # use the services defined for that node in the config or kv if no services defined
                task = cluster.async_rebalance(servers, servers[1:], [], services=[server.services or "kv" for server in servers[1:]])
            except Exception:
                task = None
            tasks.append((task, servers, cluster))

        todo = []

        for (task, servers, cluster) in tasks:
            try:
                task.result(timeout=max(0, force_stop - time.time()))
            except Exception:
                # retry failed rebalance
                todo.append(servers)
            cluster.shutdown(force=True)

        retries -= 1

    # set cluster name using the first node in each cluster
    for [i, cluster] in enumerate(params["clusters"].values()):
        retries = 3
        while retries > 0:
            try:
                rest = RestConnection(cluster[0])
                ok = rest.set_cluster_name("C{}".format(i + 1))
                if ok:
                    break
            except Exception:
                time.sleep(5)
                retries -= 1

def __get_debug_binary_name(node, is_release_build=False):
    # couchbase-server-enterprise-debuginfo-6.5.0-4557-centos7.x86_64.rpm
    # couchbase-server-enterprise-debuginfo-6.5.0-4557-suse15.x86_64.rpm
    # couchbase-server-enterprise-debuginfo-6.5.0-4557-rhel8.x86_64.rpm
    # couchbase-server-enterprise-debuginfo-6.5.0-4557-oel7.x86_64.rpm
    # couchbase-server-enterprise-debuginfo-6.5.0-4557-amzn2.x86_64.rpm
    #All the above ones replaced by couchbase-server-enterprise-debuginfo-6.5.0-4557-linux.x86_64.rpm in 7.1 and above

    version = params["version"]
    if is_release_build:
        version = version.split("-")[0]

    if node.get_os() in install_constants.X86:
        if float(params["cb_edition"][:3]) < 7.1 :
            return "{0}-{1}-{2}.{3}.{4}".format(
                params["cb_edition"] + "-debuginfo",
                version,
                node.get_os(),
                node.info.architecture_type,
                node.info.deliverable_type)
        return "{0}-{1}-{2}.{3}.{4}".format(
            params["cb_edition"] + "-debuginfo",
            params["version"],
            "linux",
            node.info.architecture_type,
            node.info.deliverable_type)

    # couchbase-server-enterprise-dbg_6.5.0-4557-ubuntu16.04_amd64.deb
    # couchbase-server-enterprise-dbg_6.5.0-4557-debian8_amd64.deb
    #All the above ones replaced by couchbase-server-enterprise-dbg_6.5.0-4557-linux_amd64.deb in 7.1 and above
    elif node.get_os() in install_constants.LINUX_AMD64:
        if float(params["cb_edition"][:3]) < 7.1 :
            return "{0}_{1}-{2}_{3}.{4}".format(
                params["cb_edition"] + "-dbg",
                version,
                node.get_os(),
                node.info.architecture_type,
                node.info.deliverable_type)
        return "{0}_{1}-{2}_{3}.{4}".format(
            params["cb_edition"] + "-dbg",
            version,
            "linux",
            node.info.architecture_type,
            node.info.deliverable_type)

    # couchbase-server-enterprise-dbg_6.5.0-4557-windows_amd64.msi
    elif node.get_os() in install_constants.WINDOWS_SERVER:
        node.info.deliverable_type = "msi"
        return "{0}_{1}-{2}_{3}.{4}".format(
            params["cb_edition"] + "-dbg",
            version,
            node.get_os(),
            "amd64",
            node.info.deliverable_type)
    return ""
