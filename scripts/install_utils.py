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
import os.path

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
    "timeout": install_constants.INSTALL_TIMEOUT,
    "all_nodes_same_os": False,
    "skip_local_download": True,
    "storage_mode": "plasma",
    "disable_consistency": False,
    "enable_ipv6": False,
    "use_domain_names": False,
    "fts_quota": testconstants.FTS_QUOTA,
    "fts_query_limit": 0
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
        self.build = None
        self.queue = None
        self.thread = None
        self.rest = None
        self.install_success = False
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

    def get_os(self):
        os = self.info.distribution_version.lower()
        to_be_replaced = ['\n', ' ', 'gnu/linux']
        for _ in to_be_replaced:
            if _ in os:
                os = os.replace(_, '')
        if self.info.deliverable_type == "dmg":
            major_version = os.split('.')
            os = major_version[0] + '.' + major_version[1]
        return os

    def uninstall_cb(self):
        need_nonroot_relogin = False
        if self.shell.nonroot:
            self.node.ssh_username = "root"
            self.shell = RemoteMachineShellConnection(self.node, exit_on_failure=False)
            need_nonroot_relogin = True
        if self.actions_dict[self.info.deliverable_type]["uninstall"]:
            cmd = self.actions_dict[self.info.deliverable_type]["uninstall"]
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
                        log.warning("Exception {0} occurred on {1}, retrying..".format(e, self.ip))
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
                            log.warning("Exception {0} occurred on {1}, retrying..".format(e, self.ip))
                            self.wait_for_completion(duration, event)
                else:
                    self.shell.execute_command(cmd, debug=self.params["debug_logs"])
                    self.wait_for_completion(duration, event)

    def set_vm_swappiness_and_thp(self):
        # set vm_swapiness to 0, and thp to never by default
        if self.actions_dict[self.info.deliverable_type]["set_vm_swapiness_and_thp"]:
            cmd = self.actions_dict[self.info.deliverable_type]["set_vm_swapiness_and_thp"]
            o, e = self.shell.execute_command(cmd, debug=self.params["debug_logs"])

    def install_cb(self):
        self.pre_install_cb()
        self.set_vm_swappiness_and_thp()
        if self.actions_dict[self.info.deliverable_type]["install"]:
            if "suse" in self.get_os():
                cmd = self.actions_dict[self.info.deliverable_type]["suse_install"]
            else:
                cmd = self.actions_dict[self.info.deliverable_type]["install"]
            cmd = cmd.replace("buildbinary", self.build.name)
            cmd = cmd.replace("buildpath", self.build.path)
            cmd = cmd.replace("mountpoint", "/tmp/couchbase-server-" + params["version"])
            duration, event, timeout = install_constants.WAIT_TIMES[self.info.deliverable_type]["install"]
            start_time = time.time()
            while time.time() < start_time + timeout:
                try:
                    o, e = self.shell.execute_command(cmd, debug=self.params["debug_logs"])
                    if o == ['1']:
                        break
                    self.wait_for_completion(duration, event)
                except Exception as e:
                    log.warning("Exception {0} occurred on {1}, retrying..".format(e, self.ip))
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
                    if o == ['1']:
                        break
                    else:
                        if self.actions_dict[self.info.deliverable_type]["post_install_retry"]:
                            if self.info.deliverable_type == "msi":
                                check_if_downgrade, _ = self.shell.execute_command(
                                    "cd " + install_constants.DOWNLOAD_DIR["WINDOWS_SERVER"] +
                                    "; vi +\"set nobomb | set fenc=ascii | x\" install_status.txt; "
                                    "grep 'Adding WIX_DOWNGRADE_DETECTED property' install_status.txt")
                                print((check_if_downgrade * 10))
                            else:
                                self.shell.execute_command(
                                    self.actions_dict[self.info.deliverable_type]["post_install_retry"],
                                    debug=self.params["debug_logs"])
                        self.wait_for_completion(duration, event)
            except Exception as e:
                log.warning("Exception {0} occurred on {1}, retrying..".format(e, self.ip))
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

    def _set_ip_version(self):
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
            cmd = install_constants.NODE_INIT["ipv4"].format(self._get_cli_path(),
                                                         self.ip,
                                                         self.node.rest_username,
                                                         self.node.rest_password)

        self.shell.execute_command(cmd)

    def pre_init_cb(self):
        try:
            self._set_ip_version()

            if params["fts_query_limit"] > 0:
                self.set_cbft_env_options("fts_query_limit", params["fts_query_limit"])


        except Exception as e:
            log.warning("Exception {0} occurred during pre-init".format(e))

    def post_init_cb(self):
        # Optionally change node name and restart server
        if params.get('use_domain_names', False):
            RemoteUtilHelper.use_hostname_for_server_settings(self.node)

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
        info = self.rest.get_nodes_self()

        start_time = time.time()
        while time.time() < start_time + 30 and kv_quota == 0:
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

    def init_cb(self):
        duration, event, timeout = install_constants.WAIT_TIMES[self.info.deliverable_type]["init"]
        self.wait_for_completion(duration * 2, event)
        start_time = time.time()
        while time.time() < start_time + timeout:
            try:
                init_success = False
                self.pre_init_cb()

                self.rest = RestConnection(self.node)
                # Make sure that data_path and index_path are writable by couchbase user
                for path in set([_f for _f in [self.node.data_path, self.node.index_path] if _f]):
                    for cmd in ("rm -rf {0}/*".format(path),
                                "chown -R couchbase:couchbase {0}".format(path)):
                        self.shell.execute_command(cmd)
                self.rest.set_data_path(data_path=self.node.data_path, index_path=self.node.index_path)
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
                log.warning("Exception {0} occurred on {1}, retrying..".format(e, self.ip))
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


def get_node_helper(ip):
    for node_helper in NodeHelpers:
        if node_helper.ip == ip:
            return node_helper
    return None


def print_result_and_exit(err=None):
    if err:
        log.error(err)
    success = []
    fail = []
    for server in params["servers"]:
        node = get_node_helper(server.ip)
        if not node or not node.install_success:
            fail.append(server.ip)

        elif node.install_success:
            success.append(server.ip)
    log.info("-" * 100)
    for _ in fail:
        log.error("INSTALL FAILED ON: \t{0}".format(_))
    log.info("-" * 100)
    for _ in success:
        log.info("INSTALL COMPLETED ON: \t{0}".format(_))
    log.info("-" * 100)
    if len(fail) > 0:
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

    # Validate and extract remaining params
    for key, value in list(userinput.test_params.items()):
        if key == "debug_logs":
            params["debug_logs"] = True if value.lower() == "true" else False
        if key == "install_tasks":
            tasks = []
            for task in value.split('-'):
                if task in install_constants.DEFAULT_INSTALL_TASKS and task not in tasks:
                    tasks.append(task)
            if len(tasks) > 0:
                params["install_tasks"] = tasks
            log.info("INSTALL TASKS: {0}".format(params["install_tasks"]))
            if "install" not in params["install_tasks"] and "init" not in params["install_tasks"]:
                return params  # No other parameters needed
        if key == 'v' or key == "version":
            if re.match('^[0-9\.\-]*$', value) and len(value) > 5:
                params["version"] = value
        if key == "url":
            if value.startswith("http"):
                params["url"] = value
            else:
                log.warning('URL:{0} is not valid, will use version to locate build'.format(value))
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


def pre_install_steps():
    if "install" in params["install_tasks"]:
        if params["url"] is not None:
            if NodeHelpers[0].shell.is_url_live(params["url"]):
                params["all_nodes_same_os"] = True
                for node in NodeHelpers:
                    build_binary = __get_build_binary_name(node)
                    build_url = params["url"]
                    filepath = __get_download_dir(node) + build_binary
                    node.build = build(build_binary, build_url, filepath)
            else:
                print_result_and_exit("URL {0} is not live. Exiting.".format(params["url"]))
        else:
            for node in NodeHelpers:
                build_binary = __get_build_binary_name(node)
                build_url = __get_build_url(node, build_binary)
                if not build_url:
                    print_result_and_exit(
                        "Build is not present in latestbuilds or release repos, please check {0}".format(build_binary))
                filepath = __get_download_dir(node) + build_binary
                node.build = build(build_binary, build_url, filepath)
        _download_build()


def _execute_local(command, timeout):
    # -- Uncomment the below 2 lines for python 3
    # process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True).wait(timeout)
    # process.communicate()[0].strip()
    # -- python 2
    process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True).wait()


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
    if params["enable_ipv6"]:
        ipv6_url = "{0}{1}/{2}/{3}".format(
            testconstants.CB_FQDN_REPO,
            testconstants.CB_VERSION_NAME[(params["version"]).split('-')[0][:-2]],
            params["version"].split('-')[1],
            build_binary)
        if node.shell.is_url_live(ipv6_url, exit_if_not_live=False):
            return ipv6_url
    else:
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
        check_and_retry_download_binary_local(NodeHelpers[0])
        _copy_to_nodes(NodeHelpers[0].build.path, NodeHelpers[0].build.path)
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
                cmd = cmd.format(__get_download_dir(node), build_url)
            logging.info("Downloading build binary to {0}:{1}..".format(node.ip, filepath))
            check_and_retry_download_binary(cmd, node)
    log.debug("Done downloading build binary")

def check_and_retry_download_binary_local(node):
    log.info("Downloading build binary to {0}..".format(node.build.path))
    duration, event, timeout = install_constants.WAIT_TIMES[node.info.deliverable_type][
        "download_binary"]
    cmd = install_constants.WGET_CMD.format(__get_download_dir(node), node.build.url)
    start_time = time.time()
    while time.time() < start_time + timeout:
        try:
            _execute_local(cmd, timeout)
            if os.path.exists(node.build.path):
                break
            time.sleep(duration)
        except Exception as e:
            log.warn("Unable to download build: {0}, retrying..".format(e.message))
            time.sleep(duration)
    else:
        print_result_and_exit("Unable to download build in {0}s on {1}, exiting".format(timeout,
                                                                                        node.build.path))

def check_file_exists(node, filepath):
    output, _ = node.shell.execute_command("ls -lh {0}".format(filepath), debug=params["debug_logs"])
    for line in output:
        if line.find('No such file or directory') == -1:
            return True
    return False


def check_and_retry_download_binary(cmd, node):
    duration, event, timeout = install_constants.WAIT_TIMES[node.info.deliverable_type]["download_binary"]
    start_time = time.time()
    while time.time() < start_time + timeout:
        try:
            node.shell.execute_command(cmd, debug=params["debug_logs"])
            if check_file_exists(node, node.build.path):
                break
            time.sleep(duration)
        except Exception as e:
            log.warning("Unable to download build: {0}, retrying..".format(e))
            time.sleep(duration)
    else:
        print_result_and_exit("Unable to download build in {0}s on {1}, exiting".format(timeout, node.ip))


def __get_download_dir(node):
    os = node.get_os()
    if os in install_constants.LINUX_DISTROS:
        if node.shell.nonroot:
            return install_constants.NON_ROOT_DOWNLOAD_DIR['LINUX_DISTROS']
        else:
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
        return "{0}_{1}-{2}_{3}-{4}.{5}".format(params["cb_edition"],
                                            params["version"],
                                            "macos",
                                            node.info.architecture_type,
                                            "unnotarized",
                                            node.info.deliverable_type)
