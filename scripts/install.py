#!/usr/bin/python

# TODO: add installer support for membasez

import getopt
import copy
import logging
import sys
from threading import Thread
from datetime import datetime
import socket

sys.path.append(".")
sys.path.append("lib")
import testconstants
import couchdb
import time
from builds.build_query import BuildQuery
import logging.config
from membase.api.exception import ServerUnavailableException
from membase.api.rest_client import RestConnection, RestHelper
from remote.remote_util import RemoteMachineShellConnection
from membase.helper.cluster_helper import ClusterOperationHelper
import TestInput


logging.config.fileConfig("scripts.logging.conf")
log = logging.getLogger()

def usage(err=None):
    print """\
Syntax: install.py [options]

Options:
 -p <key=val,...> Comma-separated key=value info.
 -i <file>        Path to .ini file containing cluster information.

Available keys:
 product=cb|mb           Used to specify couchbase or membase.
 version=SHORT_VERSION   Example: "2.0.0r-71".
 parallel=false          Useful when you're installing a cluster.
 standalone=false        Install without cluster management
 toy=                    Install a toy build
 vbuckets=               The number of vbuckets in the server installation.

Examples:
 install.py -i /tmp/ubuntu.ini -p product=cb,version=2.0.0r-71
 install.py -i /tmp/ubuntu.ini -p product=mb,version=1.7.1r-38,parallel=true,toy=keith
 install.py -i /tmp/ubuntu.ini -p product=mongo,version=2.0.2
"""
    sys.exit(err)


product = "membase-server(ms),couchbase-single-server(css),couchbase-server(cs),zynga(z)"

errors = {"UNREACHABLE": "",
          "UNINSTALL-FAILED": "unable to uninstall the product",
          "INSTALL-FAILED": "unable to install",
          "BUILD-NOT-FOUND": "unable to find build",
          "INVALID-PARAMS": "invalid params given"}


def installer_factory(params):
    if params.get("product", None) is None:
        sys.exit("ERROR: don't know what product you want installed")

    mb_alias = ["membase", "membase-server", "mbs", "mb"]
    cb_alias = ["couchbase", "couchbase-server", "cb"]
    css_alias = ["couchbase-single", "couchbase-single-server", "css"]
    mongo_alias = ["mongo"]

    if params["product"] in mb_alias:
        if "standalone" in params:
            return MembaseServerStandaloneInstaller()
        else:
            return MembaseServerInstaller()
    elif params["product"] in cb_alias:
        if "standalone" in params:
            return CouchbaseServerStandaloneInstaller()
        else:
            return CouchbaseServerInstaller()
    elif params["product"] in css_alias:
        return CouchbaseSingleServerInstaller()
    elif params["product"] in mongo_alias:
        return MongoInstaller()

    sys.exit("ERROR: don't know about product " + params["product"])


class Installer(object):

    def install(self, params):
        pass

    def initialize(self, params):
        pass

    def uninstall(self, params):
        remote_client = RemoteMachineShellConnection(params["server"])
        remote_client.membase_uninstall()
        remote_client.couchbase_uninstall()
        remote_client.couchbase_single_uninstall()

    def build_url(self, params):
        _errors = []
        #vars
        version = ''
        server = ''
        names = []

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
            _errors.append(errors["INVALID-PARAMS"])
            ok = False
        else:
            version = params["version"]

        if ok:
            if not "product" in params:
                _errors.append(errors["INVALID-PARAMS"])
                ok = False
        if ok:
            if not "server" in params:
                _errors.append(errors["INVALID-PARAMS"])
                ok = False
            else:
                server = params["server"]

        if ok:
            if "toy" in params:
                toy = params["toy"]
            else:
                toy = ""

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
                _errors.append(errors["INVALID-PARAMS"])

        if ok:
            info = RemoteMachineShellConnection(server).extract_remote_info()
            builds, changes = BuildQuery().get_all_builds()
            releases_version = ["1.6.5.4", "1.7.0", "1.7.1", "1.7.1.1"]
            for name in names:
                if version in releases_version:
                     build = BuildQuery().find_membase_release_build(deliverable_type=info.deliverable_type,
                                                                     os_architecture=info.architecture_type,
                                                                     build_version=version,
                                                                     product='membase-server-enterprise')
                else:
                    build = BuildQuery().find_build(builds, name, info.deliverable_type,
                                                    info.architecture_type, version, toy=toy)

                if build:
                    if 'amazon' in params:
                        type = info.type.lower()
                        if type == 'windows' and version in releases_version:
                            build.url = build.url.replace("http://builds.hq.northscale.net",
                                                          "https://s3.amazonaws.com/packages.couchbase")
                            build.url = build.url.replace("enterprise", "community")
                            build.name = build.name.replace("enterprise", "community")
                        else:
                            build.url = build.url.replace("http://builds.hq.northscale.net",
                                                          "http://packages.northscale.com")
                            build.url = build.url.replace("enterprise", "community")
                            build.name = build.name.replace("enterprise", "community")
                    return build

            _errors.append(errors["BUILD-NOT-FOUND"])
        msg = "unable to find a build for product {0} version {1} for package_type {2}"
        info = RemoteMachineShellConnection(server).extract_remote_info()
        raise Exception(msg.format(names, version, info.deliverable_type))

    def is_socket_active(self, host, port, timeout=300):
        """ Check if remote socket is open and active

        Keyword arguments:
        host -- remote address
        port -- remote port
        timeout -- check timeout (in seconds)

        Returns:
        True -- socket is active
        False -- otherwise

        """
        start_time = time.time()

        sckt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        while time.time() - start_time < timeout:
            try:
                sckt.connect((host, port))
                sckt.shutdown(2)
                sckt.close()
                return True
            except:
                time.sleep(10)

        return False

class MembaseServerInstaller(Installer):
    def __init__(self):
        Installer.__init__(self)

    def initialize(self, params):
#        log = logger.new_logger("Installer")
        start_time = time.time()
        cluster_initialized = False
        server = params["server"]
        while time.time() < (start_time + (10 * 60)):
            rest = RestConnection(server)
            try:
                if server.data_path:
                    remote_client = RemoteMachineShellConnection(server)
                    remote_client.execute_command('rm -rf {0}/*'.format(server.data_path))
                    # Make sure that data_path is writable by membase user
                    remote_client.execute_command("chown -R membase.membase {0}".format(server.data_path))
                    rest.set_data_path(data_path=server.data_path)
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
#        log = logger.new_logger("Installer")
        build = self.build_url(params)
        remote_client = RemoteMachineShellConnection(params["server"])
        info = remote_client.extract_remote_info()
        type = info.type.lower()
        server = params["server"]
        if type == "windows":
            build = self.build_url(params)
            remote_client.download_binary_in_win(build.url, params["product"], params["version"])
            remote_client.membase_install_win(build, params["version"])
        else:
            downloaded = remote_client.download_build(build)
            if not downloaded:
                log.error(downloaded, 'unable to download binaries : {0}'.format(build.url))
            path = server.data_path or '/tmp'
            remote_client.membase_install(build, path=path)
            ready = RestHelper(RestConnection(params["server"])).is_ns_server_running(60)
            if not ready:
                log.error("membase-server did not start...")
            log.info('wait 5 seconds for membase server to start')
            time.sleep(5)


class MembaseServerStandaloneInstaller(Installer):
    def __init__(self):
        Installer.__init__(self)

    def initialize(self, params):
#        log = logger.new_logger("Installer")
        remote_client = RemoteMachineShellConnection(params["server"])
        remote_client.create_directory("/opt/membase/var/lib/membase/data")
        remote_client.execute_command("chown membase:membase /opt/membase/var/lib/membase/data")
        remote_client.create_file("/tmp/init.sql", """PRAGMA journal_mode = TRUNCATE;
PRAGMA synchronous = NORMAL;""")
        remote_client.execute_command('/opt/membase/bin/memcached -d -v -c 80000 -p 11211 -E /opt/membase/lib/memcached/ep.so -r -e "tap_idle_timeout=10;dbname=/opt/membase/var/lib/membase/data/default;ht_size=12582917;min_data_age=0;queue_age_cap=900;initfile=/tmp/init.sql;vb0=true" -u membase > /tmp/memcache.log </dev/null 2>&1')
        time.sleep(5)

    def install(self, params):
#        log = logger.new_logger("Installer")
        build = self.build_url(params)
        remote_client = RemoteMachineShellConnection(params["server"])
        downloaded = remote_client.download_build(build)
        if not downloaded:
            log.error(downloaded, 'unable to download binaries : {0}'.format(build.url))
        remote_client.membase_install(build, False)


class CouchbaseServerInstaller(Installer):
    def __init__(self):
        Installer.__init__(self)

    def initialize(self, params):
#        log = logger.new_logger("Installer")
        start_time = time.time()
        cluster_initialized = False
        server = params["server"]
        remote_client = RemoteMachineShellConnection(params["server"])
        while time.time() < (start_time + (10 * 60)):
            rest = RestConnection(server)
            try:
                rest.init_cluster(username=server.rest_username, password=server.rest_password)
                rest.init_cluster_memoryQuota(memoryQuota=rest.get_nodes_self().mcdMemoryReserved)
                if server.data_path:
                    time.sleep(3)
                    # Make sure that data_path is writable by couchbase user
                    #remote_client.stop_couchbase()
                    remote_client.execute_command('rm -rf {0}/*'.format(server.data_path))
                    remote_client.execute_command("chown -R couchbase:couchbase {0}".format(server.data_path))
                    rest.set_data_path(data_path=server.data_path)
                    # Symlink data-dir to custom path
                    #remote_client.execute_command('mv /opt/couchbase/var {0}'.format(server.data_path))
                    #remote_client.execute_command('ln -s {0}/var /opt/couchbase/var'.format(server.data_path))
                    #remote_client.execute_command("chown -h couchbase:couchbase /opt/couchbase/var")
                    #remote_client.start_couchbase()
                    time.sleep(3)
                cluster_initialized = True
                break
            except ServerUnavailableException:
                log.error("error happened while initializing the cluster @ {0}".format(server.ip))
            log.info('sleep for 5 seconds before trying again ...')
            time.sleep(5)
        if not cluster_initialized:
            raise Exception("unable to initialize membase node")

    def install(self, params):
#        log = logger.new_logger("Installer")
        build = self.build_url(params)
        remote_client = RemoteMachineShellConnection(params["server"])
        info = remote_client.extract_remote_info()
        type = info.type.lower()
        server = params["server"]
        if "vbuckets" in params:
            vbuckets = int(params["vbuckets"][0])
        else:
            vbuckets = None
        if type == "windows":
            build = self.build_url(params)
            remote_client.download_binary_in_win(build.url, params["product"], params["version"])
            remote_client.membase_install_win(build, params["version"])
        else:
            downloaded = remote_client.download_build(build)
            if not downloaded:
                log.error(downloaded, 'unable to download binaries : {0}'.format(build.url))
            #TODO: need separate methods in remote_util for couchbase and membase install
            path = server.data_path or '/tmp'
            remote_client.membase_install(build, path=path, vbuckets=vbuckets)
            log.info('wait 5 seconds for membase server to start')
            time.sleep(5)
        if "rest_vbuckets" in params:
            rest_vbuckets = int(params["rest_vbuckets"])
            ClusterOperationHelper.set_vbuckets(server, rest_vbuckets)
        # Optionally disable consistency check
        if params.get('disable_consistency', 0):
            rest = RestConnection(params['server'])
            rest.set_couchdb_option(section='couchdb',
                                    option='consistency_check_ratio',
                                    value='0.0')

class CouchbaseServerStandaloneInstaller(Installer):
    def __init__(self):
        Installer.__init__(self)

    def initialize(self, params):
#        log = logger.new_logger("Installer")
        remote_client = RemoteMachineShellConnection(params["server"])
        remote_client.create_directory("/opt/couchbase/var/lib/membase/data")
        remote_client.execute_command("chown couchbase:couchbase /opt/couchbase/var/lib/membase/data")
        remote_client.create_file("/tmp/init.sql", """PRAGMA journal_mode = TRUNCATE;
PRAGMA synchronous = NORMAL;""")
        remote_client.execute_command('/opt/couchbase/bin/memcached -d -v -c 80000 -p 11211 -E /opt/couchbase/lib/memcached/ep.so -r -e "dbname=/opt/couchbase/var/lib/membase/data/default;ht_size=12582917;min_data_age=0;queue_age_cap=900;initfile=/tmp/init.sql;vb0=true" -u couchbase > /tmp/memcache.log </dev/null 2>&1')
        time.sleep(5)

    def install(self, params):
#        log = logger.new_logger("Installer")
        build = self.build_url(params)
        remote_client = RemoteMachineShellConnection(params["server"])
        downloaded = remote_client.download_build(build)
        if not downloaded:
            log.error(downloaded, 'unable to download binaries : {0}'.format(build.url))
        remote_client.membase_install(build, False)


class CouchbaseSingleServerInstaller(Installer):
    def __init__(self):
        Installer.__init__(self)

    def install(self, params):
#        log = logger.new_logger("CouchbaseSingleServerInstaller")
        build = self.build_url(params)
        remote_client = RemoteMachineShellConnection(params["server"])
        downloaded = remote_client.download_build(build)
        if not downloaded:
            log.error(downloaded, 'unable to download binaries : {0}'.format(build.url))
        remote_client.couchbase_single_install(build)
        log.info('wait 5 seconds for couchbase-single server to start')
        time.sleep(5)

    def initialize(self, params):
#        log = logger.new_logger("CouchbaseSingleServerInstaller")
        start_time = time.time()
        server = params["server"]
        remote_client = RemoteMachineShellConnection(params["server"])
        replace_127_0_0_1_cmd = "sed -i 's/127.0.0.1/0.0.0.0/g' {0}".format(
            testconstants.COUCHBASE_SINGLE_DEFAULT_INI_PATH)
        o, r = remote_client.execute_command(replace_127_0_0_1_cmd)
        remote_client.log_command_output(o, r)
        remote_client.stop_couchbase()
        remote_client.start_couchbase()
        couchdb_ok = False

        while time.time() < (start_time + (1 * 60)):
            try:
                couch_ip = "http://{0}:5984/".format(server.ip)
                log.info("connecting to couch @ {0}".format(couch_ip))
                couch = couchdb.Server(couch_ip)
                couch.config()
                #TODO: verify version number and other properties
                couchdb_ok = True
                break
            except Exception as ex:
                msg = "error happened while creating connection to couchbase single server @ {0} , error : {1}"
                log.error(msg.format(server.ip, ex))
            log.info('sleep for 5 seconds before trying again ...')
            time.sleep(5)
        if not couchdb_ok:
            raise Exception("unable to initialize couchbase single server")


class MongoInstaller(Installer):
    def get_server(self, params):
        version = params["version"]
        server = params["server"]
        server.product_name = "mongodb-linux-x86_64-" + version
        server.product_tgz = server.product_name + ".tgz"
        server.product_url = "http://fastdl.mongodb.org/linux/" + server.product_tgz
        return server

    def mk_remote_client(self, server):
        remote_client = RemoteMachineShellConnection(server)

        info = remote_client.extract_remote_info()
        type = info.type.lower()
        if type == "windows":
            sys.exit("ERROR: please teach me about windows one day.")

        return remote_client

    def uninstall(self, params):
        server = self.get_server(params)
        remote_client = self.mk_remote_client(server)
        remote_client.execute_command("killall mongod mongos")
        remote_client.execute_command("killall -9 mongod mongos")
        remote_client.execute_command("rm -rf ./{0}".format(server.product_name))

    def install(self, params):
        server = self.get_server(params)
        remote_client = self.mk_remote_client(server)

        downloaded = remote_client.download_binary(server.product_url, "tgz", server.product_tgz)
        if not downloaded:
            log.error(downloaded, 'unable to download binaries : {0}'.format(server.product_url))

        remote_client.execute_command("tar -xzvf /tmp/{0}".format(server.product_tgz))

    def initialize(self, params):
        server = self.get_server(params)
        remote_client = self.mk_remote_client(server)
        remote_client.execute_command("mkdir -p {0}/data/data-27019 {0}/data/data-27018 {0}/log". \
                                          format(server.product_name))
        remote_client.execute_command("./{0}/bin/mongod --port 27019 --fork --rest --configsvr" \
                                          " --logpath ./{0}/log/mongod-27019.out" \
                                          " --dbpath ./{0}/data/data-27019". \
                                          format(server.product_name))
        remote_client.execute_command("./{0}/bin/mongod --port 27018 --fork --rest --shardsvr" \
                                          " --logpath ./{0}/log/mongod-27018.out" \
                                          " --dbpath ./{0}/data/data-27018". \
                                          format(server.product_name))

        log.info("check that config server started before launching mongos")
        if self.is_socket_active(host=server.ip, port=27019):
            remote_client.execute_command(("./{0}/bin/mongos --port 27017 --fork" \
                                           " --logpath ./{0}/log/mongos-27017.out" \
                                           " --configdb " + server.ip + ":27019"). \
                                          format(server.product_name))
        else:
            log.error("Connection with MongoDB config server was not established.")
            sys.exit()

class InstallerJob(object):
    def sequential_install(self, params):
        installers = []

        for server in input.servers:
            _params = copy.deepcopy(params)
            _params["server"] = server
            installers.append((installer_factory(_params), _params))

        for installer, _params in installers:
            try:
                installer.uninstall(_params)
                print "uninstall succeeded"
            except Exception as ex:
                print "unable to complete the uninstallation: ", ex

        for installer, _params in installers:
            try:
                installer.install(_params)
                try:
                    installer.initialize(_params)
                except Exception as ex:
                    print "unable to initialize the server after successful installation", ex
            except Exception as ex:
                print "unable to complete the installation: ", ex

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


def check_build(input):
#        log = logger.new_logger("Get build")
        _params = copy.deepcopy(input.test_params)
        _params["server"] = input.servers[0]
        installer = installer_factory(_params)
        try:
            build = installer.build_url(_params)
            log.info("Found build: {0}".format(build))
        except Exception:
            log.error("Cannot find build {0}".format(_params))
            exit(1)

params = {"ini": "resources/jenkins/fusion.ini",
          "product": "ms", "version": "1.7.1r-31", "amazon": "false"}

if __name__ == "__main__":
    try:
        (opts, args) = getopt.getopt(sys.argv[1:], 'hi:p:', [])
        for o, a in opts:
            if o == "-h":
                usage()

        if len(sys.argv) <= 1:
            usage()

        input = TestInput.TestInputParser.get_test_input(sys.argv)
        if not input.servers:
            usage("ERROR: no servers specified. Please use the -i parameter.")
    except IndexError:
        usage()
    except getopt.GetoptError, err:
        usage("ERROR: " + str(err))
    #TODO: This is not broken, but could be something better
    #      like a validator, to check SSH, input params etc
    #check_build(input)
    if "parallel" in input.test_params:
        # workaround for a python2.6 bug of using strptime with threads
        datetime.strptime("30 Nov 00", "%d %b %y")
        InstallerJob().parallel_install(input.servers, input.test_params)
    else:
        InstallerJob().sequential_install(input.test_params)
