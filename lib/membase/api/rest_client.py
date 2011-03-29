import base64
import json
import urllib
import httplib2
import socket
import time
from exception import ServerAlreadyJoinedException, ServerUnavailableException, InvalidArgumentException

#helper library methods built on top of RestConnection interface
class RestHelper(object):
    def __init__(self, rest_connection):
        self.rest = rest_connection

    def is_cluster_healthy(self):
        #get the nodes and verify that all the nodes.status are healthy
        nodes = self.rest.node_statuses()
        return all(node.status == 'healthy' for node in nodes)


    def is_cluster_rebalanced(self):
        #get the nodes and verify that all the nodes.status are healthy
        return self.rest.rebalance_statuses()


class RestConnection(object):
    #port is always 8091
    def __init__(self, ip, username, password):
        #throw some error here if the ip is null ?
        self.ip = ip
        self.username = username
        self.password = password
        self.baseUrl = "http://{0}:8091/".format(self.ip)


    #authorization mut be a base64 string of username:password
    def _create_headers(self):
        authorization = base64.encodestring('%s:%s' % (self.username, self.password))
        return {'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic %s' % authorization,
                'Accept': '*/*'}

    #params serverIp : the server to add to this cluster
    #raises exceptions when
    #unauthorized user
    #server unreachable
    #can't add the node to itself ( TODO )
    #server already added
    #returns otpNode
    def add_node(self, user='', password='', remoteIp='' ):
        otpNode = None
        print 'adding remote node : {0} to this cluster @ : {1}'\
        .format(self.ip, remoteIp)
        api = self.baseUrl + 'controller/addNode'
        params = urllib.urlencode({'hostname': remoteIp,
                                   'user': user,
                                   'password': password})
        try:
            response, content = httplib2.Http().request(api, 'POST', params,
                                                        headers=self._create_headers())
            if response['status'] == '400':
                print 'error occured while adding remote node: {0}'.format(remoteIp)
                if content.find('Prepare join failed. Node is already part of cluster') >= 0:
                    raise ServerAlreadyJoinedException(nodeIp=self.ip,
                                                       remoteIp=remoteIp)
                else:
                    #todo: raise an exception here
                    print 'get_pools error', content
            elif response['status'] == '200':
                dict = json.loads(content)
                otpNodeId = dict['otpNode']
                otpNode = OtpNode(otpNodeId)
            return otpNode
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)


    def eject_node(self, user='', password='', otpNode=None ):
        if not otpNode:
            print 'otpNode parameter required'
            return False
        try:
            api = self.baseUrl + 'controller/ejectNode'
            params = urllib.urlencode({'otpNode': otpNode,
                                       'user': user,
                                       'password': password})
            response, content = httplib2.Http().request(api, 'POST', params,
                                                        headers=self._create_headers())
            if response['status'] == '400':
                if content.find('Prepare join failed. Node is already part of cluster') >= 0:
                    raise ServerAlreadyJoinedException(nodeIp=self.ip,
                                                       remoteIp=otpNode)
                else:
                    # todo : raise an exception here
                    print 'eject_node error', content
            elif response['status'] == '200':
                print 'ejectNode successful'
            return True
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)


    def rebalance(self, otpNodes):
        knownNodes = ''
        index = 0
        for node in otpNodes:
            if index == 0:
                knownNodes += node
            else:
                knownNodes += ',' + node
            index += 1

        params = urllib.urlencode({'knownNodes': knownNodes,
                                   'user': self.username,
                                   'password': self.password})

        api = self.baseUrl + "controller/rebalance"
        try:
            response, content = httplib2.Http().request(api, 'POST', params,
                                                        headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                #extract the error
                raise InvalidArgumentException('controller/rebalance',
                                               parameters=params)
            elif response['status'] == '200':
                print 'rebalance operation started'
            return True
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def monitorRebalance(self):
        start = time.time()
        progress = 0
        while progress is not -1 and progress is not 100:
            progress = self._rebalance_progress()
            #sleep for 2 seconds
            time.sleep(2)

        if progress == -1:
            return False
        else:
            duration = time.time() - start
            print 'rebalance progress took {0} seconds '.format(duration)
            return True

    def _rebalance_progress(self):
        start = time.time()
        percentage = -1
        api = self.baseUrl + "pools/default/rebalanceProgress"
        try:
            response, content = httplib2.Http().request(api, 'GET',
                                                        headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                #extract the error , how ?
                print 'unable to obtain rebalance progress ?'
            elif response['status'] == '200':
                parsed = json.loads(content)
                if parsed.has_key('status'):
                    if parsed.has_key('errorMessage'):
                        print 'rebalance failed'
                    elif parsed['status'] == 'running':
                        for key in parsed:
                            if key.find('ns_1') >= 0:
                                ns_1_dictionary = parsed[key]
                                percentage = ns_1_dictionary['progress'] * 100
                                print 'rebalance percentage : {0} %' .format(percentage)
                                break
                    else:
                        percentage = 100
            return percentage
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)
            #if status is none , is there an errorMessage
            #convoluted logic which figures out if the rebalance failed or suceeded

    def rebalance_statuses(self):
        nodes = []
        api = self.baseUrl + 'pools/rebalanceStatuses'
        try:
            response, content = httplib2.Http().request(api, 'GET', headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                #extract the error
                print 'unable to retrieve nodesStatuses'
            elif response['status'] == '200':
                parsed = json.loads(content)
                rebalanced = parsed['balanced']
                return rebalanced

        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def node_statuses(self):
        nodes = []
        api = self.baseUrl + 'nodeStatuses'
        try:
            response, content = httplib2.Http().request(api, 'GET', headers=self._create_headers())
            #if status is 200 then it was a success otherwise it was a failure
            if response['status'] == '400':
                #extract the error
                print 'unable to retrieve nodesStatuses'
            elif response['status'] == '200':
                parsed = json.loads(content)
                for key in parsed:
                    #each key contain node info
                    value = parsed[key]
                    #get otp,get status
                    node = OtpNode(id=value['otpNode'],
                                   status=value['status'])
                    nodes.append(node)
                    #let's also populate the membase_version_info
            return nodes
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def get_pools(self):
        version = None
        api = self.baseUrl + 'pools'
        try:
            response, content = httplib2.Http().request(api, 'GET', headers=self._create_headers())
            if response['status'] == '400':
                if content.find('Prepare join failed. Node is already part of cluster') >= 0:
                    raise ServerAlreadyJoinedException(nodeIp=self.ip,
                                                       remoteIp=otpNode)
                else:
                    print 'get_pools error', content
            elif response['status'] == '200':
                parsed = json.loads(content)
                version = MembaseServerVersion(parsed['implementationVersion'], parsed['componentsVersion'])
            return version
        except socket.error:
            raise ServerUnavailableException(ip=self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip=self.ip)

    def get_bucket(self,bucket ='default'):
        bucketInfo = None
        api = '{0}{1}{2}'.format(self.baseUrl,'pools/default/buckets/',bucket)
        try:
            response, content = httplib2.Http().request(api, 'GET', headers = self._create_headers())
            if response['status'] == '400':
                print 'get_pools error', content
            elif response['status'] == '200':
                bucketInfo = RestParser().parse_get_bucket_response(content)
                return bucketInfo
        except socket.error:
            raise ServerUnavailableException(ip = self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip = self.ip)
        return bucketInfo
    def delete_bucket(self,bucket = 'default'):
        api = '{0}{1}{2}'.format(self.baseUrl,'/pools/default/buckets/', bucket)
        try:
            response, content = httplib2.Http().request(api, 'DELETE', headers = self._create_headers())
            if response['status'] == '200':
                return True
            else:
                print 'get_pools error', content
        except socket.error:
            raise ServerUnavailableException(ip = self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip = self.ip)
        return False

    # figure out the proxy port
    def create_bucket(self,bucket = '',
                      ramQuotaMB = 1,
                      authType = 'none',
                      saslPassword = '',
                      replicaNumber = 1,
                      proxyPort = 0):
        api = '{0}{1}'.format(self.baseUrl,'/pools/default/buckets')
        params = urllib.urlencode({})
        if authType == 'none':
            params = urllib.urlencode({'name': bucket,
                                       'ramQuotaMB': ramQuotaMB,
                                       'authType':authType,
                                       'replicaNumber':replicaNumber,
                                       'proxyPort':proxyPort})

        elif authType == 'sasl':
            params = urllib.urlencode({'name': bucket,
                                       'ramQuotaMB': ramQuotaMB,
                                       'authType':authType,
                                       'saslPassword':saslPassword,
                                       'replicaNumber':replicaNumber,
                                       'proxyPort':proxyPort})

        try:
            response, content = httplib2.Http().request(api, 'POST',params,
                                                        headers = self._create_headers())
            print content
            print response
            if response['status'] == '200':
                return True
            else:
                print 'create_bucket error', content ,response
        except socket.error:
            raise ServerUnavailableException(ip = self.ip)
        except httplib2.ServerNotFoundError:
            raise ServerUnavailableException(ip = self.ip)
        return False


class MembaseServerVersion:
    def __init__(self, implementationVersion='', componentsVersion=''):
        self.implementationVersion = implementationVersion
        self.componentsVersion = componentsVersion

#this class will also contain more node related info
class OtpNode(object):
    def __init__(self, id = '', status = ''):
        self.id = id
        self.ip = ''
        #extract ns ip from the otpNode string
        #its normally ns_1@10.20.30.40
        if id.find('@') >= 0:
            self.ip = id[id.index('@') + 1:]
        self.status = status

class Bucket(object):
    def __init__(self):
        self.name = ''
        self.type = ''
        self.nodes = None
        self.stats = None

class Node(object):
    def __init__(self):
        self.uptime = 0
        self.memoryTotal = 0
        self.memoryFree = 0
        self.mcdMemoryReserved = 0
        self.mcdMemoryAllocated = 0
        self.status = ""
        self.hostname = ""
        self.clusterCompatibility = ""
        self.version = ""
        self.os = ""
        self.ports = None

class NodePort(object):
    def __init__(self):
        self.proxy = 0
        self.direct = 0

class BucketStats(object):
    def __init__(self):
        self.quotaPercentUsed = 0
        self.opsPerSec = 0
        self.diskFetches = 0
        self.itemCount = 0
        self.diskUsed = 0
        self.memUsed = 0
        self.ram = 0


class RestParser(object):

    def parse_get_bucket_response(self,response):
        bucket = Bucket()
        parsed = json.loads(response)
        bucket.name = parsed['name']
        bucket.type = parsed['bucketType']
        bucket.nodes = list()
        # get the 'storageTotals'
        stats = parsed['basicStats']
        bucketStats = BucketStats()
        bucketStats.quotaPercentUsed = stats['quotaPercentUsed']
        bucketStats.opsPerSec = stats['opsPerSec']
        bucketStats.diskFetches = stats['diskFetches']
        bucketStats.itemCount = stats['itemCount']
        bucketStats.diskUsed = stats['diskUsed']
        bucketStats.memUsed = stats['memUsed']
        quota = parsed['quota']
        bucketStats.ram = quota['ram']
        bucket.stats = bucketStats
        nodes = parsed['nodes']
        for nodeDictionary in nodes:
            node = Node()
            node.uptime = nodeDictionary['uptime']
            node.memoryFree = nodeDictionary['memoryFree']
            node.memoryTotal = nodeDictionary['memoryTotal']
            node.mcdMemoryAllocated = nodeDictionary['mcdMemoryAllocated']
            node.mcdMemoryReserved = nodeDictionary['mcdMemoryReserved']
            node.status = nodeDictionary['status']
            node.hostname = nodeDictionary['hostname']
            node.clusterCompatibility = nodeDictionary['clusterCompatibility']
            node.version = nodeDictionary['version']
            node.os = nodeDictionary['os']
            # todo : node.ports
            bucket.nodes.append(node)
        return bucket