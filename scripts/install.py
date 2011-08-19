# example : python scripts/install.py -i /tmp/ubuntu.ini -p product=cb,version=2.0.0r-71
# example : python scripts/install.py -i /tmp/ubuntu.ini -p product=mb,version=1.7.1r-38

# TODO: add installer support for membasez 

import copy
import sys
from threading import Thread

sys.path.append(".")
sys.path.append("lib")
from builds.build_query import BuildQuery
import logger
from membase.api.exception import ServerUnavailableException
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection
import time
import TestInput

product = "membase-server(ms),couchbase-single-server(css),couchbase-server(cs),zynga(z)"

errors = {"UNREACHABLE": "",
          "UNINSTALL-FAILED": "unable to uninstall the product",
          "INSTALL-FAILED": "unable to install",
          "BUILD-NOT-FOUND": "unable to find build",
          "INVALID-PARAMS": "invalid params given"}


def installer_factory(params):
    mb_alias = ["membase", "membase-server", "mbs", "mb"]
    cb_alias = ["couchbase", "couchbase-server", "cb"]
    css_alias = ["couchbase-single", "couchbase-single-server", "css"]
    if params["product"] in mb_alias:
        return MembaseServerInstaller()
    elif params["product"] in cb_alias:
        return CouchbaseServerInstaller()
    elif params["product"] in css_alias:
        return CouchbaseSingleServerInstaller()


class Installer(object):

    def install(self, params):
        pass

    def initialize(self, params):
        pass

    def uninstall(self, params):
        remote_client = RemoteMachineShellConnection(params["server"])
        remote_client.membase_uninstall()
        remote_client.couchbase_uninstall(params["product"])
        remote_client.couchbase_single_uninstall()

    def build_url(self, params):
        errors = []
        #vars
        version = ''
        server = ''
        names = []
        win = ''

        #replace "v" with version
        #replace p with product
        tmp = {}
        for k in params:
            value = params[k]
            if k == "v":
                tmp["version"] = value
            elif k == "p":
                tmp["version"] = value
            else:
                tmp[k] = value
        params = tmp
        ok = True
        if not "version" in params:
            errors.append(errors["INVALID-PARAMS"])
            ok = False
        else:
            version = params["version"]

        if ok:
            if not "product" in params:
                errors.append(errors["INVALID-PARAMS"])
                ok = False
        if ok:
            if not "server" in params:
                errors.append(errors["INVALID-PARAMS"])
                ok = False
            else:
                server = params["server"]

        if ok:
            mb_alias = ["membase", "membase-server", "mbs", "mb"]
            cb_alias = ["couchbase", "couchbase-server", "cb"]
            css_alias = ["couchbase-single", "couchbase-single-server", "css"]

            if params["product"] in mb_alias:
                names = ['membase-server-enterprise', 'membase-server-community']
            elif params["product"] in cb_alias:
                names = ['couchbase-server-enterprise', 'couchbase-server-community']
            elif params["product"] in css_alias:
                names = ['couchbase-single-server-enterprise', 'couchbase-single-server-community']
            else:
                ok = False
                errors.append(errors["INVALID-PARAMS"])

        if ok:
            info = RemoteMachineShellConnection(server).extract_remote_info()
            builds, changes = BuildQuery().get_all_builds()
            for name in names:
                build = BuildQuery().find_build(builds, name, info.deliverable_type,
                                                info.architecture_type, version)
                if build:
                    if 'amazon' in params:
                        build.url = build.url.replace("http://builds.hq.northscale.net",
                                                      "http://packages.northscale.com")
                        build.url = build.url.replace("enterprise", "community")
                        build.name = build.name.replace("enterprise", "community")
                    return build

            errors.append(errors["BUILD-NOT-FOUND"])
            ok = False

        raise Exception("unable to find a build...")

class MembaseServerInstaller(Installer):
    def __init__(self):
        Installer.__init__(self)


    def initialize(self, params):
        log = logger.new_logger("Installer")
        start_time = time.time()
        cluster_initialized = False
        server = params["server"]
        while time.time() < (start_time + (10 * 60)):
            rest = RestConnection(server)
            try:
                rest.init_cluster(username=server.rest_username, password=server.rest_password)
		rest.init_cluster_memoryQuota(memoryQuota=rest.get_nodes_self().mcdMemoryReserved)
                cluster_initialized = True
                break
            except ServerUnavailableException:
                log.error("error happened while initializing the cluster @ {0}".format(server.ip))
            log.info('sleep for 5 seconds before trying again ...')
            time.sleep(5)
        if not cluster_initialized:
            raise Exception("unable to initialize membase node")


    def install(self, params):
        log = logger.new_logger("Installer")
        build = self.build_url(params)
        remote_client = RemoteMachineShellConnection(params["server"])
        downloaded = remote_client.download_build(build)
        if not downloaded:
            log.error(downloaded, 'unable to download binaries : {0}'.format(build.url))
        remote_client.membase_install(build)
        ready = RestHelper(RestConnection(params["server"])).is_ns_server_running(60)
        if not ready:
            log.error("membase-server did not start...")
        log.info('wait 5 seconds for membase server to start')
        time.sleep(5)

class CouchbaseServerInstaller(Installer):
    def __init__(self):
        Installer.__init__(self)


    def initialize(self, params):
        log = logger.new_logger("Installer")
        start_time = time.time()
        cluster_initialized = False
        server = params["server"]
        while time.time() < (start_time + (10 * 60)):
            rest = RestConnection(server)
            try:
                rest.init_cluster(username=server.rest_username, password=server.rest_password)
                rest.init_cluster_memoryQuota(memoryQuota=rest.get_nodes_self().mcdMemoryReserved)
                cluster_initialized = True
                break
            except ServerUnavailableException:
                log.error("error happened while initializing the cluster @ {0}".format(server.ip))
            log.info('sleep for 5 seconds before trying again ...')
            time.sleep(5)
        if not cluster_initialized:
            raise Exception("unable to initialize membase node")


    def install(self, params):
        log = logger.new_logger("Installer")
        build = self.build_url(params)
        remote_client = RemoteMachineShellConnection(params["server"])
        info = remote_client.extract_remote_info()
        type = info.type.lower()
        if type == "windows":
            os_type = "exe"
            task = "install"
            bat_file = "install.bat"
            server_path = "/cygdrive/c/Program Files/Couchbase/Server/"
            dir_paths = ['/cygdrive/c/automation','/cygdrive/c/tmp']

            log = logger.new_logger("Installer")
            build = self.build_url(params)
            remote_client.create_multiple_dir(dir_paths)
            remote_client.copy_files_local_to_remote('resources/windows/automation', '/cygdrive/c/automation')
            downloaded = remote_client.download_binary_in_win(build.url, params["product"], params["version"])
            if downloaded:
                log.info('Successful download {0}_{1}.exe'.format( params["product"], params["version"]))
            else:
                log.error('Download {0}_{1}.exe failed'.format( params["product"], params["version"]))
            # modify bat file to update new build version
            remote_client.modify_bat_file('/cygdrive/c/automation', bat_file, params["product"],
                                           info.architecture_type, info.windows_name, params["version"], task)
            log.info('sleep for 5 seconds before running task schedule install me')
            time.sleep(5)
            # run task schedule to install couchbase server
            output, error = remote_client.execute_command("cmd /c schtasks /run /tn installme")
            remote_client.log_command_output(output, error)
            remote_client.wait_till_file_added(server_path, "VERSION.txt", timeout_in_seconds=600)
            log.info('wait 30 seconds for server to start up completely')
            time.sleep(30)
        else:
            downloaded = remote_client.download_build(build)
            if not downloaded:
                log.error(downloaded, 'unable to download binaries : {0}'.format(build.url))
            #TODO: need separate methods in remote_util for couchbase and membase install
            remote_client.membase_install(build)
            log.info('wait 5 seconds for membase server to start')
            time.sleep(5)


class CouchbaseSingleServerInstaller(Installer):
    def __init__(self):
        Installer.__init__(self)


    def install(self, params):
        log = logger.new_logger("Installer")
        build = self.build_url(params)
        remote_client = RemoteMachineShellConnection(params["server"])
        downloaded = remote_client.download_build(build)
        if not downloaded:
            log.error(downloaded, 'unable to download binaries : {0}'.format(build.url))
        remote_client.couchbase_single_install(build)
        log.info('wait 5 seconds for couchbase-single server to start')
        time.sleep(5)


class CouchbaseServerWindowsInstaller(Installer):

    def __init__(self):
        Installer.__init__(self)

    def initialize(self, params):
        log = logger.new_logger("Installer")
        start_time = time.time()
        cluster_initialized = False
        server = params["server"]
        while time.time() < (start_time + (10 * 60)):
            rest = RestConnection(server)
            try:
                rest.init_cluster(username=server.rest_username, password=server.rest_password)
                cluster_initialized = True
                break
            except ServerUnavailableException:
                log.error("error happened while initializing the cluster @ {0}".format(server.ip))
            log.info('sleep for 5 seconds before trying again ...')
            time.sleep(5)
            log.error()
        if not cluster_initialized:
            raise Exception("unable to initialize membase node")

    def install(self, params):
        version = params["version"]
        abbr_pro = params["product"]
        task = 'install'
        ex_type = 'exe'
        bat_file = 'install.bat'
        os_name = params["win"]
        if abbr_pro == 'cse':
            server_path = "/cygdrive/c/Program Files/Couchbase/Server/"
        log = logger.new_logger("Installer")
        build = self.build_url(params)
        remote_client = RemoteMachineShellConnection(params["server"])
        info = remote_client.extract_remote_info()
        if info.architecture_type == "x86_64":
            os_type = "64"
        else:
            os_type = "32"
        # remove and recreate automaion directory
        dir_paths = ['/cygdrive/c/automation','/cygdrive/c/tmp']
        remote_client.create_multiple_dir(dir_paths)
        remote_client.copy_files_local_to_remote('resources/windows/automation', '/cygdrive/c/automation')
        # download required build to install on this server
        log.info('build url {0}'.format(build.url))
        downloaded = remote_client.download_binary_in_win(build.url, abbr_pro, version)
        if downloaded:
            log.info('Successful download {0}_{1}.exe'.format( abbr_pro, version))
        else:
            log.error('Download {0}_{1}.exe failed'.format( abbr_pro, version))
        # modify bat file to update new build version
        remote_client.modify_bat_file('/cygdrive/c/automation', bat_file,
                                        abbr_pro, os_type, os_name, version, task)
        log.info('sleep for 5 seconds before running task schedule install me')
        time.sleep(5)
        # run task schedule to install couchbase server
        output, error = remote_client.execute_command("cmd /c schtasks /run /tn installme")
        remote_client.log_command_output(output, error)
        remote_client.wait_till_file_added(server_path, "VERSION.txt", timeout_in_seconds=600)
        log.info('wait 30 seconds for server to start up completely')
        time.sleep(30)

class InstallerJob(object):
    def sequential_install(self, params):
        for server in input.servers:
            _params = copy.deepcopy(params)
            _params["server"] = server
            installer = installer_factory(_params)
            try:
                installer.uninstall(_params)
                print "uninstall succeeded"
                installer.install(_params)
                try:
                    installer.initialize(_params)
                except Exception as ex:
                    print "unable to initialize the server after successful installation", ex
            except Exception as ex:
                print "unable to complete the installation", ex


    def parallel_install(self, servers, params):
        uninstall_threads = []
        install_threads = []
        initializer_threads = []
        for server in servers:
            _params = copy.deepcopy(params)
            _params["server"] = server
            u_t = Thread(target=installer_factory(params).uninstall,
                       name="uninstaller-thread-{0}".format(server.ip),
                       args=(_params,))
            i_t = Thread(target=installer_factory(params).install,
                       name="installer-thread-{0}".format(server.ip),
                       args=(_params,))
            init_t = Thread(target=installer_factory(params).initialize,
                       name="installer-thread-{0}".format(server.ip),
                       args=(_params,))
            uninstall_threads.append(u_t)
            install_threads.append(i_t)
            initializer_threads.append(init_t)
        for t in uninstall_threads:
            t.start()
        for t in uninstall_threads:
            print "thread {0} finished".format(t.name)
            t.join()
        for t in install_threads:
            t.start()
        for t in install_threads:
            print "thread {0} finished".format(t.name)
            t.join()
        for t in initializer_threads:
            t.start()
        for t in initializer_threads:
            print "thread {0} finished".format(t.name)
            t.join()

params = {"ini": "resources/jenkins/fusion.ini",
          "product": "ms", "version": "1.7.1r-31", "amazon": "false"}

if __name__ == "__main__":
    input = TestInput.TestInputParser.get_test_input(sys.argv)
    if "parallel" in input.test_params:
        InstallerJob().parallel_install(input.servers, input.test_params)
    else:
        InstallerJob().sequential_install(input.test_params)

