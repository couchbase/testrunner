#whether membase server is installed or not
#install version x on ip y - raise a meaningful if we can't install membase
#server not reachable , installed but can't reach moxi
# can't reach the 8091
# which part of server is not working
import logger
from remote.remote_util import RemoteMachineShellConnection


log = logger.Logger.get_logger()
class InstallerHelper(object):

    @staticmethod
    # this helper will install membase server build on the given ip
    # or throws a detailed exception for the failure
    def install_membase(ip,username,key_location,build):
        connection = RemoteMachineShellConnection(ip = ip,
                                                  username = username,
                                                  pkey_location = key_location)
        remote_client = RemoteMachineShellConnection(ip = ip,
                                                     pkey_location = self.get_pkey())
        downloaded = connection.download_build(build)
        if not downloaded:
            raise BuildException(build)
        connection.membase_uninstall()
        remote_client.membase_install(build)
        #TODO: we should poll the 8091 port until it is up and running
        log.info('wait 5 seconds for membase server to start')
        time.sleep(5)
        rest = RestConnection(ip = ip,username = 'Administrator',
                              password = 'password')
        #try this max for 2 minutes
        start_time = time.time()
        cluster_initialized = False
        while time.time() < (start_time + (2 * 60)):
            try:
                rest.init_cluster(username = 'Administrator',password = 'password')
                cluster_initialized = True
            except ServerUnavailableException:
                log.error("error happened while initializing the cluster @ {0}".format(ip))
            log.info('wait 5 for seconds before trying again...')
            time.sleep(5)
        if not cluster_initialized:
            log.error("error happened while initializing the cluster @ {0}".format(ip))
            raise Exception("error happened while initializing the cluster @ {0}".format(ip))





# tests . create your own bucket , use it for testing then get rid of it
# rebalance : delete the bucket in all the clusters ?

class MembaseInstallationExceptionTypes(object):

    UNAUTHORIZED = 1000
    BUILD_ERROR = 1001
    NODE_ALREADY_JOINED = 1002

#base exception class for membase apis
class MembaseInstallationException(Exception):
    def __init__(self):
        self._message = ""
        self.type = ""
        #you can embed the params values here
        #dictionary mostly
        self.parameters = dict()

    def __init__(self,message,type,parameters):
        self._message = message
        self.type = type
        #you can embed the params values here
        #dictionary mostly
        self.parameters = parameters

    def __str__(self):
        string = ''
        if self._message:
            string += self._message
        return string

class UnauthorizedException(MembaseInstallationException):
    def __init__(self,username='',password=''):
        self._message = 'user not logged in'
        self.parameters = dict()
        self.parameters['username'] = username
        self.parameters['password'] = password
        self.type = MembaseInstallationExceptionTypes.UNAUTHORIZED


class BuildException(MembaseInstallationException):
    def __init__(self,build):
        self.parameters = dict()
        self.parameters['build'] = build
        self.type = MembaseInstallationExceptionTypes.BUILD_ERROR
        self._message = 'unable to download or retrieve build from url : {0}'.format(build)