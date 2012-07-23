

class MembaseHttpExceptionTypes(object):

    UNAUTHORIZED = 1000
    NOT_REACHABLE = 1001
    NODE_ALREADY_JOINED = 1002
    NODE_CANT_ADD_TO_ITSELF = 1003
    BUCKET_CREATION_ERROR = 1004
    STATS_UNAVAILABLE = 1005
    REMOTE_CLUSTER_JOIN_FAILED = 1006


#base exception class for membase apis
class MembaseHttpException(Exception):
    def __init__(self):
        self._message = ""
        self.type = ""
        #you can embed the params values here
        #dictionary mostly
        self.parameters = dict()

    def __init__(self, message, type, parameters):
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
    def __init__(self, username='', password=''):
        self._message = 'user not logged in'
        self.parameters = dict()
        self.parameters['username'] = username
        self.parameters['password'] = password
        self.type = MembaseHttpExceptionTypes.UNAUTHORIZED


class BucketCreationException(MembaseHttpException):
    def __init__(self, ip='', bucket_name=''):
        self.parameters = dict()
        self.parameters['host'] = ip
        self.parameters['bucket'] = bucket_name
        self.type = MembaseHttpExceptionTypes.BUCKET_CREATION_ERROR
        self._message = 'unable to create bucket {0} on the host @ {1}'.\
            format(bucket_name, ip)


class StatsUnavailableException(MembaseHttpException):
    def __init__(self):
        self.type = MembaseHttpExceptionTypes.STATS_UNAVAILABLE
        self._message = 'unable to get stats'


class ServerUnavailableException(MembaseHttpException):
    def __init__(self, ip=''):
        self.parameters = dict()
        self.parameters['host'] = ip
        self.type = MembaseHttpExceptionTypes.NOT_REACHABLE
        self._message = 'unable to reach the host @ {0}'.format(ip)


class InvalidArgumentException(MembaseHttpException):
    def __init__(self, api, parameters):
        self.parameters = parameters
        self.api = api
        self._message = '{0} failed when invoked with parameters: {1}'.\
            format(self.api, self.parameters)


class ServerSelfJoinException(MembaseHttpException):
    def __init__(self, nodeIp='', remoteIp=''):
        self._message = 'node: {0} already added to this cluster:{1}'.\
            format(remoteIp, nodeIp)
        self.parameters = dict()
        self.parameters['nodeIp'] = nodeIp
        self.parameters['remoteIp'] = remoteIp
        self.type = MembaseHttpExceptionTypes.NODE_CANT_ADD_TO_ITSELF


class ClusterRemoteException(MembaseHttpException):
    def __init__(self, nodeIp='', remoteIp=''):
        self._message = 'unable to add remote cluster : {0}:{1}'.\
            format(remoteIp, nodeIp)
        self.parameters = dict()
        self.parameters['nodeIp'] = nodeIp
        self.parameters['remoteIp'] = remoteIp
        self.type = MembaseHttpExceptionTypes.REMOTE_CLUSTER_JOIN_FAILED


class ServerAlreadyJoinedException(MembaseHttpException):
    def __init__(self, nodeIp='', remoteIp=''):
        self._message = 'node: {0} already added to this cluster:{1}'.\
            format(remoteIp, nodeIp)
        self.parameters = dict()
        self.parameters['nodeIp'] = nodeIp
        self.parameters['remoteIp'] = remoteIp
        self.type = MembaseHttpExceptionTypes.NODE_ALREADY_JOINED


class RebalanceFailedException(MembaseHttpException):
    def __init__(self, string=''):
        self._message = 'Rebalance Failed: {0}'.format(string)

class FailoverFailedException(MembaseHttpException):
    def __init__(self, string=''):
        self._message = 'Failover Node failed :{0} '.format(string)


class DesignDocCreationException(MembaseHttpException):
    def __init__(self, design_doc_name, reason=''):
        self._message = 'Error occured design document %s: %s' % (design_doc_name, reason)

class QueryViewException(MembaseHttpException):
    def __init__(self, view_name, reason=''):
        self._message = 'Error occured querying view %s: %s' % (view_name, reason)

class ReadDocumentException(MembaseHttpException):
      def __init__(self, doc_id, reason=''):
        self._message = 'Error occured looking up document %s: %s' % (doc_id, reason)

class CompactViewFailed(MembaseHttpException):
      def __init__(self, design_doc_name, reason=''):
        self._message = 'Error occured triggering compaction for design_doc %s: %s' % \
            (design_doc_name, reason)

class SetViewInfoNotFound(MembaseHttpException):
      def __init__(self, design_doc_name, reason=''):
        self._message = 'Error occured reading set_view _info of ddoc %s: %s' % \
            (design_doc_name, reason)


class GetBucketInfoFailed(MembaseHttpException):
    def __init__(self, bucket, reason=''):
        self._message = 'Error occured getting bucket information %s: %s' % \
            (bucket, reason)

class AddNodeException(MembaseHttpException):
    def __init__(self, nodeIp='', remoteIp='', reason=''):
        self._message = 'Error adding node: {0} to the cluster:{1} - {2}'.\
            format(remoteIp, nodeIp, reason)
        self.parameters = dict()
        self.parameters['nodeIp'] = nodeIp
        self.parameters['remoteIp'] = remoteIp
