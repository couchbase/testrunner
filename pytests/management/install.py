#!/usr/bin/env python

import time
from TestInput import TestInputSingleton
from builds.build_query import BuildQuery
import unittest
import logger
from membase.api.exception import ServerUnavailableException
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection


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
            remote_client.execute_command("killall -9 memcached;killall -9 moxi;")
            time.sleep(5)
            info = remote_client.extract_remote_info()
            self.machine_infos[serverInfo.ip] = info
            remote_client.disconnect()

    def test_install(self):
        # find the right deliverable for this os?
        query = BuildQuery()
        version = self.input.test_params['version']
        builds, changes = query.get_all_builds()
        for serverInfo in self.servers:
            info = self.machine_infos[serverInfo.ip]
            build = query.find_membase_build(builds,
                                             'membase-server-enterprise',
                                             info.deliverable_type,
                                             info.architecture_type,
                                             version)
            if not build:
                self.log.info('find community edition build')
                build = query.find_membase_build(builds,
                                                 'membase-server-community',
                                                 info.deliverable_type,
                                                 info.architecture_type,
                                                 version)
                #try community ?
            if not build:
                self.fail('unable to find any {0} build for {1} for arch : {2} '.format(info.distribution_type,
                                                                                        info.architecture_type,
                                                                                        version))
            print 'for machine : ', info.architecture_type, info.distribution_type, 'relevant build : ', build
            remote_client = RemoteMachineShellConnection(serverInfo)
            remote_client.membase_uninstall()
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
            rest = RestConnection(serverInfo)
            start_time = time.time()
            cluster_initialized = False
            while time.time() < (start_time + (10 * 60)):
                try:
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
            rest.init_cluster_memoryQuota(200)