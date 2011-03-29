
class MembaseHttpExceptionTypes(object):

    UNAUTHORIZED = 1000
    NOT_REACHABLE = 1001
    NODE_ALREADY_JOINED = 1002

#base exception class for membase apis
class MembaseHttpException(Exception):
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

class UnauthorizedException(MembaseHttpException):
    def __init__(self,username='',password=''):
        self._message = 'user not logged in'
        self.parameters = dict()
        self.parameters['username'] = username
        self.parameters['password'] = password
        self.type = MembaseHttpExceptionTypes.UNAUTHORIZED


class ServerUnavailableException(MembaseHttpException):
    def __init__(self,ip = ''):
        self.parameters = dict()
        self.parameters['host'] = ip
        self.type = MembaseHttpExceptionTypes.NOT_REACHABLE
        self._message = 'unable to reach the host @ {0}'.format(ip)
class InvalidArgumentException(MembaseHttpException):
    def __init__(self,api,parameters):
        self.parameters = parameters
        self.api = api
        self._message = '{0} failed when invoked with parameters: {1}'\
            .format(self.api,self.parameters)

class ServerAlreadyJoinedException(MembaseHttpException):
    def __init__(self,nodeIp='',remoteIp=''):
        self._message = 'node: {0} already added to this cluster:{1}'.format(remoteIp,
                                                              nodeIp)
        self.parameters = dict()
        self.parameters['nodeIp'] = nodeIp
        self.parameters['remoteIp'] = remoteIp
        self.type = MembaseHttpExceptionTypes.NODE_ALREADY_JOINED