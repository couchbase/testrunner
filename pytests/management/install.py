#!/usr/bin/env python

import time
import os
from TestInput import TestInputSingleton
from builds.build_query import BuildQuery
import unittest
import logger
from membase.api.exception import ServerUnavailableException
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from threading import Thread
import TestInput


# we will have InstallUninstall as one test case
# Install as another test case
class InstallTest(unittest.TestCase):
    input = None
    servers = None
    machine_infos = None
    log = None

    def setUp(self):
        self.log = logger.Logger.get_logger()
        self.input = TestInputSingleton.input
        self.assertTrue(self.input, msg="input parameters missing...")
        #TODO: we shouldn't start the test if membase_build does not have a version/url
        self.servers = self.input.servers
        self.machine_infos = {}
        for serverInfo in self.servers:
            remote_client = RemoteMachineShellConnection(serverInfo)
            info = remote_client.extract_remote_info()
            self.machine_infos[serverInfo.ip] = info
            remote_client.disconnect()
            self.log.info('IP : {0} Distribution : {1} Arch: {2} Version : {3}'
            .format(info.ip, info.distribution_type, info.architecture_type, info.distribution_version))

    def test_reset(self):
        for serverInfo in self.servers:
            remote_client = RemoteMachineShellConnection(serverInfo)
            remote_client.stop_membase()
            remote_client.start_membase()
            remote_client.disconnect()
        time.sleep(10)

    def reset_couchbase(self):
        for serverInfo in self.servers:
            remote_client = RemoteMachineShellConnection(serverInfo)
            remote_client.stop_couchbase()
            remote_client.start_couchbase()
            remote_client.disconnect()
        time.sleep(10)

    def _test_install(self,serverInfo,version,builds):
        query = BuildQuery()
        info = self.machine_infos[serverInfo.ip]
        names = ['membase-server-enterprise',
                 'membase-server-community',
                 'couchbase-server-enterprise',
                 'couchbase-server-community']
        build = None
        for name in names:
            build = query.find_membase_build(builds,
                                             name,
                                             info.deliverable_type,
                                             info.architecture_type,
                                             version.strip())
            if build:
                break

        if not build:
            self.fail('unable to find any {0} build for {1} for arch : {2} '.format(info.distribution_type,
                                                                                    info.architecture_type,
                                                                                    version.strip()))
        print 'for machine : ', info.architecture_type, info.distribution_type, 'relevant build : ', build
        remote_client = RemoteMachineShellConnection(serverInfo)
        remote_client.membase_uninstall()
        remote_client.couchbase_uninstall()
        if 'amazon' in self.input.test_params:
            build.url = build.url.replace("http://builds.hq.northscale.net/latestbuilds/",
                                          "http://packages.northscale.com/latestbuilds/")
            build.url = build.url.replace("enterprise", "community")
            build.name = build.name.replace("enterprise", "community")
        downloaded = remote_client.download_build(build)
        self.assertTrue(downloaded, 'unable to download binaries :'.format(build.url))
        remote_client.membase_install(build)
        #TODO: we should poll the 8091 port until it is up and running
        self.log.info('wait 5 seconds for membase server to start')
        time.sleep(5)
        start_time = time.time()
        cluster_initialized = False
        while time.time() < (start_time + (10 * 60)):
            rest = RestConnection(serverInfo)
            try:
                if serverInfo.data_path:
                    self.log.info("setting data path to " + serverInfo.data_path)
                    rest.set_data_path(serverInfo.data_path)
                rest.init_cluster(username=serverInfo.rest_username, password=serverInfo.rest_password)
                cluster_initialized = True
                break
            except ServerUnavailableException:
                self.log.error("error happened while initializing the cluster @ {0}".format(serverInfo.ip))
            self.log.info('sleep for 5 seconds before trying again ...')
            time.sleep(5)
        self.assertTrue(cluster_initialized,
                        "error happened while initializing the cluster @ {0}".format(serverInfo.ip))
        if not cluster_initialized:
            self.log.error("error happened while initializing the cluster @ {0}".format(serverInfo.ip))
            raise Exception("error happened while initializing the cluster @ {0}".format(serverInfo.ip))
        nodeinfo = rest.get_nodes_self()
        rest.init_cluster_memoryQuota(memoryQuota=nodeinfo.mcdMemoryReserved)
        rest.init_cluster_memoryQuota(256)

    #this method should only be used for amazon tests
    def test_install_parallel(self):
        # find the right deliverable for this os?
        query = BuildQuery()
        version = self.input.test_params['version']
        builds, changes = query.get_all_builds()

        threads = []
        for serverInfo in self.servers:
            new_thread = Thread(None, self._test_install, None, (serverInfo, version, builds))
            new_thread.start()
            threads.append(new_thread)

        self.log.info("waiting for all installer threads to complete...")
        for t in threads:
            self.log.info("thread {0} finished".format(t))
            t.join()

    def test_win_install(self):
        query = BuildQuery()
        builds, changes = query.get_all_builds()
        version = self.input.test_params['version']
        os_version = self.input.test_params['win']

        task = 'install'
        ok = True
        ex_type = 'exe'
        bat_file = 'install.bat'
        version_file = 'VERSION.txt'
        if self.input.test_params["ostype"] == '64':
            Arch = 'x86_64'
            os_type = '64'
        elif self.input.test_params["ostype"] == '32':
            Arch = 'x86'
            os_type = '32'
        else:
            ok = False
            self.log.error("Unknown os version.")

        product = self.input.test_params["product"]
        if product == 'cse':
            name = 'couchbase-server-enterprise'
        elif product == 'csse':
            name = 'couchbase-single-server-enterprise'
        elif product == 'csc':
            name = 'couchbase-server-community'
        elif product == 'cssc':
            name = 'couchbase-single-server-community'
        else:
            ok = False
            self.log.error("Unknon product type.")

        cb_server_alias = ['cse','csc']
        cb_single_alias = ['csse','cssc']
        if product in cb_server_alias:
            server_path = "/cygdrive/c/Program Files/Couchbase/Server/"
        elif product in cb_single_alias:
            server_path = "/cygdrive/c/Program Files (x86)/Couchbase/Server/"

        if ok:
            for serverInfo in self.servers:
                remote_client = RemoteMachineShellConnection(serverInfo)
                info = RemoteMachineShellConnection(serverInfo).extract_remote_info()
                build = query.find_build(builds, name, ex_type, Arch, version)
                #self.log.info("what is this {0}".format(build.url))
                # check if previous couchbase server installed
                exist = remote_client.file_exists("/cygdrive/c/Program Files/Couchbase/Server/", version_file)
                if exist:
                    # call uninstall function to install couchbase server
                    self.log.info("Start uninstall cb server on this server")
                    self.test_win_uninstall(remote_client, product, os_type, os_version, version, server_path)
                else:
                    self.log.info('I am free. You can install couchbase server now')
                # directory path in remote server used to create or delete directory
                dir_paths = ['/cygdrive/c/automation','/cygdrive/c/tmp']
                remote_client.create_multiple_dir(dir_paths)
                # copy files from local server to remote server
                remote_client.copy_files_local_to_remote('resources/windows/automation', '/cygdrive/c/automation')
                downloaded = remote_client.download_binary_in_win(build.url,product,version)
                if downloaded:
                    self.log.info('Successful download {0}_{1}.exe'.format(product, version))
                else:
                    self.log.error('Download {0}_{1}.exe failed'.format(product, version))
                remote_client.modify_bat_file('/cygdrive/c/automation', bat_file,
                                               product, os_type, os_version, version, task)
                self.log.info('sleep for 5 seconds before running task schedule install me')
                time.sleep(5)
                # run task schedule to install Couchbase Server
                output, error = remote_client.execute_command("cmd /c schtasks /run /tn installme")
                remote_client.log_command_output(output, error)
                remote_client.wait_till_file_added(server_path, version_file, timeout_in_seconds=600)
                self.log.info('sleep 15 seconds before running the next job ...')
                time.sleep(15)
        else:
            self.log.error("Can not install Couchbase Server.")

    def test_win_uninstall(self, remote_client, product, os_type, os_version, build_version, server_path):
        query = BuildQuery()
        builds, changes = query.get_all_builds()
        os_version = self.input.test_params['win']

        task = 'uninstall'
        ex_type = 'exe'
        bat_file = 'uninstall.bat'
        version_file = 'VERSION.txt'
        if os_type == '64':
            Arch = 'x86_64'
        elif os_type == '32':
            Arch = 'x86'

        if product == 'cse':
            name = 'couchbase-server-enterprise'
        elif product == 'csse':
            name = 'couchbase-single-server-enterprise'
        elif product == 'csc':
            name = 'couchbase-server-community'
        elif product == 'cssc':
            name = 'couchbase-single-server-community'
        else:
            self.log.error("Unknon product type.")

        # no need later
        cb_server_alias = ['cse','csc']
        cb_single_alias = ['csse','cssc']
        if product in cb_server_alias:
            server_path = "/cygdrive/c/Program Files/Couchbase/Server/"
        elif product in cb_single_alias:
            server_path = "/cygdrive/c/Program Files (x86)/Couchbase/Server/"

        info = remote_client.extract_remote_info()
        build_name, version = remote_client.find_build_version(server_path, version_file)
        self.log.info('build needed to do auto uninstall {0}'.format(build_name))
        # find installed build in tmp directory
        build_name = build_name.rstrip() + ".exe"
        self.log.info('Check if {0} is in tmp directory'.format(build_name))
        exist = remote_client.file_exists("/cygdrive/c/tmp/", build_name)
        if not exist:
            build = query.find_build(builds, name, ex_type, Arch, version)
            downloaded = remote_client.download_binary_in_win(build.url,product,version)
            if downloaded:
                self.log.info('Successful download {0}_{1}.exe'.format(product, version))
            else:
                self.log.error('Download {0}_{1}.exe failed'.format(product, version))
        # modify uninstall bat file to change build name.
        remote_client.modify_bat_file('/cygdrive/c/automation', bat_file,
                                       product, os_type, os_version, version, task)
        self.log.info('sleep for 5 seconds before running task schedule uninstall')
        time.sleep(5)
        # run task schedule to uninstall Couchbase Server
        self.log.info('Start to uninstall couchbase {0}_{1}'.format(product, version))
        output, error = remote_client.execute_command("cmd /c schtasks /run /tn removeme")
        remote_client.log_command_output(output, error)
        remote_client.wait_till_file_deleted(server_path, version_file, timeout_in_seconds=600)
        self.log.info('sleep 15 seconds before running the next job ...')
        time.sleep(15)

    def test_win_uninstall_standalone(self):
        query = BuildQuery()
        builds, changes = query.get_all_builds()
        os_version = self.input.test_params['win']

        task = 'uninstall'
        ex_type = 'exe'
        bat_file = 'uninstall.bat'
        version_file = 'VERSION.txt'
        if self.input.test_params["ostype"] == '64':
            Arch = 'x86_64'
            os_type = '64'
        elif self.input.test_params["ostype"] == '32':
            Arch = 'x86'
            os_type = '32'
        else:
            ok = False
            self.log.error("Unknown os version.")

        product = self.input.test_params["product"]
        if product == 'cse':
            name = 'couchbase-server-enterprise'
        elif product == 'csse':
            name = 'couchbase-single-server-enterprise'
        elif product == 'csc':
            name = 'couchbase-server-community'
        elif product == 'cssc':
            name = 'couchbase-single-server-community'
        else:
            self.log.error("Unknon product type.")

        # no need later
        cb_server_alias = ['cse','csc']
        cb_single_alias = ['csse','cssc']
        if product in cb_server_alias:
            server_path = "/cygdrive/c/Program Files/Couchbase/Server/"
        elif product in cb_single_alias:
            server_path = "/cygdrive/c/Program Files (x86)/Couchbase/Server/"

        for serverInfo in self.servers:
                remote_client = RemoteMachineShellConnection(serverInfo)
                info = RemoteMachineShellConnection(serverInfo).extract_remote_info()

                exist = remote_client.file_exists(server_path, version_file)
                if exist:
                    build_name, version = remote_client.find_build_version(server_path, version_file)
                    self.log.info('build needed to do auto uninstall {0}'.format(build_name))
                    # find installed build in tmp directory
                    build_name = build_name.rstrip() + ".exe"
                    self.log.info('Check if {0} is in tmp directory'.format(build_name))
                    exist = remote_client.file_exists("/cygdrive/c/tmp/", build_name)
                    if not exist:
                        build = query.find_build(builds, name, ex_type, Arch, version)
                        downloaded = remote_client.download_binary_in_win(build.url,product,version)
                        if downloaded:
                            self.log.info('Successful download {0}_{1}.exe'.format(product, version))
                        else:
                            self.log.error('Download {0}_{1}.exe failed'.format(product, version))
                    # modify uninstall bat file to change build name.
                    remote_client.modify_bat_file('/cygdrive/c/automation', bat_file,
                                                      product, os_type, os_version, version, task)
                    self.log.info('sleep for 5 seconds before running task schedule uninstall')
                    time.sleep(5)
                    # run task schedule to uninstall Couchbase Server
                    self.log.info('Start to uninstall couchbase {0}_{1}'.format(product, version))
                    output, error = remote_client.execute_command("cmd /c schtasks /run /tn removeme")
                    remote_client.log_command_output(output, error)
                    remote_client.wait_till_file_deleted(server_path, version_file, timeout_in_seconds=600)
                    self.log.info('sleep 15 seconds before running the next job ...')
                    time.sleep(15)
                else:
                    self.log.info('Couchbase server may not install on this server')

    def test_install(self):
        # find the right deliverable for this os?
        query = BuildQuery()
        version = self.input.test_params['version']
        builds, changes = query.get_all_builds()
        for serverInfo in self.servers:
            self._test_install(serverInfo,version,builds)
