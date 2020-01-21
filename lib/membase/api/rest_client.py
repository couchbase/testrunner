import base64
import json
import urllib.request, urllib.parse, urllib.error
from . import httplib2
import logger
import traceback
import socket
import time
import re
import uuid
from copy import deepcopy
from threading import Thread
from TestInput import TestInputSingleton
from TestInput import TestInputServer
from testconstants import MIN_KV_QUOTA, INDEX_QUOTA, FTS_QUOTA, CBAS_QUOTA
from testconstants import COUCHBASE_FROM_VERSION_4, IS_CONTAINER, CLUSTER_QUOTA_RATIO


try:
    from couchbase_helper.document import DesignDocument, View
except ImportError:
    from lib.couchbase_helper.document import DesignDocument, View

from memcached.helper.kvstore import KVStore
from .exception import ServerAlreadyJoinedException, ServerUnavailableException, InvalidArgumentException
from membase.api.exception import BucketCreationException, ServerSelfJoinException, ClusterRemoteException, \
    RebalanceFailedException, FailoverFailedException, DesignDocCreationException, QueryViewException, \
    ReadDocumentException, GetBucketInfoFailed, CompactViewFailed, SetViewInfoNotFound, AddNodeException, \
    BucketFlushFailed, CBRecoveryFailedException, XDCRException, SetRecoveryTypeFailed, BucketCompactionException
log = logger.Logger.get_logger()

# helper library methods built on top of RestConnection interface

class RestHelper(object):
    def __init__(self, rest_connection):
        self.rest = rest_connection

    def is_ns_server_running(self, timeout_in_seconds=360):
        end_time = time.time() + timeout_in_seconds
        while time.time() <= end_time:
            try:
                status = self.rest.get_nodes_self(5)
                if status is not None and status.status == 'healthy':
                    return True
                else:
                    if status is not None:
                        log.warn("server {0}:{1} status is {2}"\
                            .format(self.rest.ip, self.rest.port, status.status))
                    else:
                        log.warn("server {0}:{1} status is down"\
                                           .format(self.rest.ip, self.rest.port))
            except ServerUnavailableException:
                log.error("server {0}:{1} is unavailable"\
                                           .format(self.rest.ip, self.rest.port))
            time.sleep(5)
        msg = 'unable to connect to the node {0} even after waiting {1} seconds'
        log.error(msg.format(self.rest.ip, timeout_in_seconds))
        return False

    def is_cluster_healthy(self, timeout=120):
        # get the nodes and verify that all the nodes.status are healthy
        nodes = self.rest.node_statuses(timeout)
        return all(node.status == 'healthy' for node in nodes)

    def rebalance_reached(self, percentage=100,retry_count=40):
        start = time.time()
        progress = 0
        previous_progress = 0
        retry = 0
        while progress is not -1 and progress < percentage and retry < retry_count:
            # -1 is error , -100 means could not retrieve progress
            progress = self.rest._rebalance_progress()
            if progress == -100:
                log.error("unable to retrieve rebalanceProgress.try again in 2 seconds")
                retry += 1
            else:
                if previous_progress == progress:
                    retry += 0.5
                else:
                    retry = 0
                    previous_progress = progress
            # sleep for 2 seconds
            time.sleep(3)
        if progress <= 0:
            log.error("rebalance progress code : {0}".format(progress))

            return False
        elif retry >= retry_count:
            log.error("rebalance stuck on {0}%".format(progress))
            return False
        else:
            duration = time.time() - start
            log.info('rebalance reached >{0}% in {1} seconds '.format(progress, duration))
            return True

    # return true if cluster balanced, false if it needs rebalance
    def is_cluster_rebalanced(self):
        command = "ns_orchestrator:needs_rebalance()"
        status, content = self.rest.diag_eval(command)
        if status:
            return content.lower() == "false"
        log.error("can't define if cluster balanced")
        return None


    # this method will rebalance the cluster by passing the remote_node as
    # ejected node
    def remove_nodes(self, knownNodes, ejectedNodes, wait_for_rebalance=True):
        if len(ejectedNodes) == 0:
            return False
        self.rest.rebalance(knownNodes, ejectedNodes)
        if wait_for_rebalance:
            return self.rest.monitorRebalance()
        else:
            return False

    def vbucket_map_ready(self, bucket, timeout_in_seconds=360):
        end_time = time.time() + timeout_in_seconds
        while time.time() <= end_time:
            vBuckets = self.rest.get_vbuckets(bucket)
            if vBuckets:
                return True
            else:
                time.sleep(0.5)
        msg = 'vbucket map is not ready for bucket {0} after waiting {1} seconds'
        log.info(msg.format(bucket, timeout_in_seconds))
        return False

    def bucket_exists(self, bucket):
        try:
            buckets = self.rest.get_buckets()
            names = [item.name for item in buckets]
            log.info("node {1} existing buckets : {0}" \
                              .format(names, self.rest.ip))
            for item in buckets:
                if item.name == bucket:
                    log.info("node {1} found bucket {0}" \
                             .format(bucket, self.rest.ip))
                    return True
            return False
        except Exception:
            return False

    def wait_for_node_status(self, node, expected_status, timeout_in_seconds):
        status_reached = False
        end_time = time.time() + timeout_in_seconds
        while time.time() <= end_time and not status_reached:
            nodes = self.rest.node_statuses()
            for n in nodes:
                if node.id == n.id:
                    log.info('node {0} status : {1}'.format(node.id, n.status))
                    if n.status.lower() == expected_status.lower():
                        status_reached = True
                    break
            if not status_reached:
                log.info("sleep for 5 seconds before reading the node.status again")
                time.sleep(5)
        log.info('node {0} status_reached : {1}'.format(node.id, status_reached))
        return status_reached

    def _wait_for_task_pid(self, pid, end_time, ddoc_name):
        while (time.time() < end_time):
            new_pid, _ = self.rest._get_indexer_task_pid(ddoc_name)
            if pid == new_pid:
                time.sleep(5)
                continue
            else:
                return

    def _wait_for_indexer_ddoc(self, servers, ddoc_name, timeout=300):
        nodes = self.rest.get_nodes()
        servers_to_check = []
        for node in nodes:
            for server in servers:
                if node.ip == server.ip and str(node.port) == str(server.port):
                    servers_to_check.append(server)
        for server in servers_to_check:
            try:
                rest = RestConnection(server)
                log.info('Check index for ddoc %s , server %s' % (ddoc_name, server.ip))
                end_time = time.time() + timeout
                log.info('Start getting index for ddoc %s , server %s' % (ddoc_name, server.ip))
                old_pid, is_pid_blocked = rest._get_indexer_task_pid(ddoc_name)
                if not old_pid:
                    log.info('Index for ddoc %s is not going on, server %s' % (ddoc_name, server.ip))
                    continue
                while is_pid_blocked:
                    log.info('Index for ddoc %s is blocked, server %s' % (ddoc_name, server.ip))
                    self._wait_for_task_pid(old_pid, end_time, ddoc_name)
                    old_pid, is_pid_blocked = rest._get_indexer_task_pid(ddoc_name)
                    if time.time() > end_time:
                        log.error("INDEX IS STILL BLOKED node %s ddoc % pid %" % (server, ddoc_name, old_pid))
                        break
                if old_pid:
                    log.info('Index for ddoc %s is running, server %s' % (ddoc_name, server.ip))
                    self._wait_for_task_pid(old_pid, end_time, ddoc_name)
            except Exception as ex:
                log.error('unable to check index on server %s because of %s' % (server.ip, str(ex)))

    def _get_vbuckets(self, servers, bucket_name='default'):
        vbuckets_servers = {}
        for server in servers:
            buckets = RestConnection(server).get_buckets()
            if not buckets:
                return vbuckets_servers
            if bucket_name:
                bucket_to_check = [bucket for bucket in buckets
                               if bucket.name == bucket_name][0]
            else:
                bucket_to_check = [bucket for bucket in buckets][0]
            vbuckets_servers[server] = {}
            vbs_active = [vb.id for vb in bucket_to_check.vbuckets
                          if vb.master.startswith(str(server.ip))]
            vbs_replica = []
            for replica_num in range(0, bucket_to_check.numReplicas):
                vbs_replica.extend([vb.id for vb in bucket_to_check.vbuckets
                                    if replica_num in vb.replica
                                    and vb.replica[replica_num].startswith(str(server.ip))])
            vbuckets_servers[server]['active_vb'] = vbs_active
            vbuckets_servers[server]['replica_vb'] = vbs_replica
        return vbuckets_servers

class RestConnection(object):
    def __new__(cls, serverInfo={}):
        # allow port to determine
        # behavior of restconnection
        port = None
        if isinstance(serverInfo, dict):
            if 'port' in serverInfo:
                port = serverInfo['port']
        else:
            port = serverInfo.port

        if not port:
            port = 8091

        #log.info("port is {} and serverInfo object type is {}".format(port,type(serverInfo)))
        #log.info("serverInfo={}".format(vars(serverInfo)))
        if int(port) in range(9091, 9100):
            # return elastic search rest connection
            from membase.api.esrest_client import EsRestConnection
            #log.info("--> calling object.__new__(EsRestConnection,serverInfo)")
            obj = super(EsRestConnection,cls).__new__(cls)
        else:
            # default
            #log.info("-->default")
            obj = object.__new__(cls)
              
        #log.info("obj={}".format(vars(obj)))
        return obj

    def __init__(self, serverInfo):
        # serverInfo can be a json object/dictionary
        if isinstance(serverInfo, dict):
            self.ip = serverInfo["ip"]
            self.username = serverInfo["username"]
            self.password = serverInfo["password"]
            self.port = serverInfo["port"]
            self.index_port = 9102
            self.fts_port = 8094
            self.query_port = 8093
            self.eventing_port = 8096
            if "index_port" in list(serverInfo.keys()):
                self.index_port = serverInfo["index_port"]
            if "fts_port" in list(serverInfo.keys()):
                if serverInfo['fts_port']:
                    self.fts_port = serverInfo["fts_port"]
            if "eventing_port" in list(serverInfo.keys()):
                if serverInfo['eventing_port']:
                    self.eventing_port = serverInfo["eventing_port"]
            self.hostname = ''
            self.services = ''
            if "hostname" in serverInfo:
                self.hostname = serverInfo["hostname"]
            if "services" in serverInfo:
                self.services = serverInfo["services"]
        else:
            self.ip = serverInfo.ip
            self.username = serverInfo.rest_username
            self.password = serverInfo.rest_password
            self.port = serverInfo.port
            self.hostname = ''
            self.index_port = 9102
            self.fts_port = 8094
            self.query_port = 8093
            self.eventing_port = 8096
            self.services = "kv"
            self.debug_logs = False
            if hasattr(serverInfo, "services"):
                self.services = serverInfo.services
            if hasattr(serverInfo, 'index_port'):
                self.index_port = serverInfo.index_port
            if hasattr(serverInfo, 'query_port'):
                self.query_port = serverInfo.query_port
            if hasattr(serverInfo, 'fts_port'):
                if serverInfo.fts_port:
                    self.fts_port = serverInfo.fts_port
            if hasattr(serverInfo, 'eventing_port'):
                if serverInfo.eventing_port:
                    self.eventing_port = serverInfo.eventing_port
            if hasattr(serverInfo, 'hostname') and serverInfo.hostname and\
               serverInfo.hostname.find(self.ip) == -1:
                self.hostname = serverInfo.hostname
            if hasattr(serverInfo, 'services'):
                self.services = serverInfo.services
        self.input = TestInputSingleton.input
        if self.input is not None:
            """ from watson, services param order and format:
                new_services=fts-kv-index-n1ql """
            self.services_node_init = self.input.param("new_services", None)
            self.debug_logs = self.input.param("debug-logs", False)
        self.baseUrl = "http://{0}:{1}/".format(self.ip, self.port)
        self.fts_baseUrl = "http://{0}:{1}/".format(self.ip, self.fts_port)
        self.index_baseUrl = "http://{0}:{1}/".format(self.ip, self.index_port)
        self.query_baseUrl = "http://{0}:{1}/".format(self.ip, self.query_port)
        self.capiBaseUrl = "http://{0}:{1}/".format(self.ip, 8092)
        self.eventing_baseUrl = "http://{0}:{1}/".format(self.ip, self.eventing_port)
        if self.hostname:
            self.baseUrl = "http://{0}:{1}/".format(self.hostname, self.port)
            self.capiBaseUrl = "http://{0}:{1}/".format(self.hostname, 8092)
            self.query_baseUrl = "http://{0}:{1}/".format(self.hostname, 8093)
            self.eventing_baseUrl = "http://{0}:{1}/".format(self.hostname, self.eventing_port)

        # Initialization of CBAS related params
        self.cbas_base_url = "http://{0}:{1}".format(self.ip, 8095)
        if hasattr(self.input, 'cbas'):
            if self.input.cbas:
                self.cbas_node = self.input.cbas
                self.cbas_port = 8095
                if hasattr(self.cbas_node, 'port'):
                    self.cbas_port = self.cbas_node.port
                self.cbas_base_url = "http://{0}:{1}".format(
                    self.cbas_node.ip,
                    self.cbas_port)
            elif "cbas" in self.services:
                self.cbas_base_url = "http://{0}:{1}".format(self.ip, 8095)

        # for Node is unknown to this cluster error
        for iteration in range(5):
            #log.info("--> api baseurl is {},{},{}".format(type(self.baseUrl),type('nodes/selfr'),self.baseUrl + 'nodes/self'))
            http_res, success = self.init_http_request(api=self.baseUrl + "nodes/self")
            if not success and isinstance(http_res, str) and\
               (http_res.find('Node is unknown to this cluster') > -1 or \
                http_res.find('Unexpected server error, request logged') > -1):
                log.error("Error {0} was gotten, 5 seconds sleep before retry"\
                                                             .format(http_res))
                time.sleep(5)
                if iteration == 2:
                    log.error("node {0}:{1} is in a broken state!"\
                                        .format(self.ip, self.port))
                    raise ServerUnavailableException(self.ip)
                continue
            else:
                break
        # determine the real couchApiBase for cluster_run
        # couchApiBase appeared in version 2.*
        if not http_res or http_res["version"][0:2] == "1.":
            self.capiBaseUrl = self.baseUrl + "/couchBase"
        else:
            for iteration in range(5):
                if "couchApiBase" not in list(http_res.keys()):
                    if self.is_cluster_mixed():
                        self.capiBaseUrl = self.baseUrl + "/couchBase"
                        return
                    time.sleep(0.2)
                    http_res, success = self.init_http_request(self.baseUrl + 'nodes/self')
                else:
                    self.capiBaseUrl = http_res["couchApiBase"]
                    return
            raise ServerUnavailableException("couchApiBase doesn't exist in nodes/self: %s " % http_res)

    def sasl_streaming_rq(self, bucket, timeout=120):
        api = self.baseUrl + 'pools/default/bucketsStreaming/{0}'.format(bucket)
        if isinstance(bucket, Bucket):
            api = self.baseUrl + 'pools/default/bucketsStreaming/{0}'.format(bucket.name)
        try:
            httplib2.Http(timeout=timeout).request(api, 'GET', '',
                                                   headers=self._create_capi_headers())
        except Exception as ex:
            log.warn('Exception while streaming: %s' % str(ex))

    def open_sasl_streaming_connection(self, bucket, timeout=1000):
        if self.debug_logs:
            log.info("Opening sasl streaming connection for bucket {0}"\
                 .format((bucket, bucket.name)[isinstance(bucket, Bucket)]))
        t = Thread(target=self.sasl_streaming_rq,
                          name="streaming_" + str(uuid.uuid4())[:4],
                          args=(bucket, timeout))
        try:
            t.start()
        except:
            log.warn("thread is not started")
            return None
        return t

    def is_cluster_mixed(self):
            http_res, success = self.init_http_request(self.baseUrl + 'pools/default')
            if http_res == 'unknown pool':
                return False
            try:
                versions = list({node["version"][:1] for node in http_res["nodes"]})
            except:
                log.error('Error while processing cluster info {0}'.format(http_res))
                # not really clear what to return but False see to be a good start until we figure what is happening
                return False


            if '1' in versions and '2' in versions:
                 return True
            return False

    def is_cluster_compat_mode_greater_than(self, version):
        """
        curl -v -X POST -u Administrator:welcome http://10.3.4.186:8091/diag/eval
        -d 'cluster_compat_mode:get_compat_version().'
        Returns : [3,2] if version = 3.2.0
        """
        status, content = self.diag_eval('cluster_compat_mode:get_compat_version().')
        if status:
            json_parsed = json.loads(content)
            cluster_ver = float("%s.%s" % (json_parsed[0], json_parsed[1]))
            if cluster_ver > version:
                return True
        return False

    def is_enterprise_edition(self):
        http_res, success = self.init_http_request(self.baseUrl + 'pools/default')
        if http_res == 'unknown pool':
            return False
        editions = []
        community_nodes = []
        """ get the last word in node["version"] as in "version": "2.5.1-1073-rel-enterprise" """
        for node in http_res["nodes"]:
            editions.extend(node["version"].split("-")[-1:])
            if "community" in node["version"].split("-")[-1:]:
                community_nodes.extend(node["hostname"].split(":")[:1])
        if "community" in editions:
            log.error("IP(s) for node(s) with community edition {0}".format(community_nodes))
            return False
        return True

    def init_http_request(self, api):
        content = None
        try:
            #log.info("--> api: {}".format(api))
            headers = self._create_capi_headers()
            #log.info("--> headers: {}".format(headers))
            status, content, header = self._http_request(api, 'GET', headers)
            json_parsed = json.loads(content)
            if status:
                return json_parsed, True
            else:
                print(("{0} with status {1}: {2}".format(api, status, json_parsed)))
                return json_parsed, False
        except ValueError as e:
            if content is not None:
                print(("{0}: {1}".format(api, content)))
            else:
                print(e)
            return content, False

    def rename_node(self, hostname, username='Administrator', password='password'):
        params = urllib.parse.urlencode({'username': username,
                                   'password': password,
                                   'hostname': hostname})

        api = "%snode/controller/rename" % (self.baseUrl)
        status, content, header = self._http_request(api, 'POST', params)
        return status, content

    def active_tasks(self):
        api = 'http://{0}:{1}/pools/default/tasks'.format(self.ip, self.port)
        try:
            status, content, header = self._http_request(api, 'GET',
                                                         headers=self._create_capi_headers())
            json_parsed = json.loads(content)
        except ValueError as e:
            print(e)
            return ""
        return json_parsed

    def ns_server_tasks(self):
        api = self.baseUrl + 'pools/default/tasks'
        try:
            status, content, header = self._http_request(api, 'GET', headers=self._create_headers())
            return json.loads(content)
        except ValueError:
            return ""

    # DEPRECATED: use create_ddoc() instead.
    def create_view(self, design_doc_name, bucket_name, views, options=None):
        return self.create_ddoc(design_doc_name, bucket_name, views, options)

    def create_ddoc(self, design_doc_name, bucket, views, options=None):
        design_doc = DesignDocument(design_doc_name, views, options=options)
        if design_doc.name.find('/') != -1:
            design_doc.name = design_doc.name.replace('/', '%2f')
            design_doc.id = '_design/{0}'.format(design_doc.name)
        return self.create_design_document(bucket, design_doc)

    def create_design_document(self, bucket, design_doc):
        log.info("-->create_design_document")
        try:
          design_doc_name = design_doc.id
          api = '%s/%s/%s' % (self.capiBaseUrl, bucket, design_doc_name)
          if isinstance(bucket, Bucket):
            api = '%s/%s/%s' % (self.capiBaseUrl, bucket.name, design_doc_name)

          #log.info("--> calling _http_request({},{},{},{}".format(api,'PUT',str(design_doc),self._create_capi_headers()))
          status, content, header = self._http_request(api, 'PUT', str(design_doc),
                                                     headers=self._create_capi_headers())
          #log.info("-->status={},content={},header={}".format(status,content,header))
          #log.info("-->FIX ME.Sleeping for 60 secs..")
          #time.sleep(60)
        except Exception as e:
          traceback.print_exc()

        if not status:
            raise DesignDocCreationException(design_doc_name, content)
        return json.loads(content.decode())

    def is_index_triggered(self, ddoc_name, index_type='main'):
        run, block = self._get_indexer_task_pid(ddoc_name, index_type=index_type)
        if run or block:
            return True
        else:
            return False

    def _get_indexer_task_pid(self, ddoc_name, index_type='main'):
        active_tasks = self.active_tasks()
        if 'error' in active_tasks:
            return None
        if active_tasks:
            for task in active_tasks:
                if task['type'] == 'indexer' and task['indexer_type'] == index_type:
                    for ddoc in task['design_documents']:
                        if ddoc == ('_design/%s' % ddoc_name):
                            return task['pid'], False
                if task['type'] == 'blocked_indexer' and task['indexer_type'] == index_type:
                    for ddoc in task['design_documents']:
                        if ddoc == ('_design/%s' % ddoc_name):
                            return task['pid'], True
        return None, None

    def query_view(self, design_doc_name, view_name, bucket, query, timeout=120, invalid_query=False, type="view"):
        status, content, header = self._query(design_doc_name, view_name, bucket, type, query, timeout)
        if not status and not invalid_query:
            stat = 0
            if 'status' in header:
                stat = int(header['status'])
            raise QueryViewException(view_name, content, status=stat)
        return json.loads(content)

    def _query(self, design_doc_name, view_name, bucket, view_type, query, timeout):
        if design_doc_name.find('/') != -1:
            design_doc_name = design_doc_name.replace('/', '%2f')
        if view_name.find('/') != -1:
            view_name = view_name.replace('/', '%2f')
        api = self.capiBaseUrl + '%s/_design/%s/_%s/%s?%s' % (bucket,
                                               design_doc_name, view_type,
                                               view_name,
                                               urllib.parse.urlencode(query))
        if isinstance(bucket, Bucket):
            api = self.capiBaseUrl + '%s/_design/%s/_%s/%s?%s' % (bucket.name,
                                                  design_doc_name, view_type,
                                                  view_name,
                                                  urllib.parse.urlencode(query))
        log.info("index query url: {0}".format(api))
        status, content, header = self._http_request(api, headers=self._create_capi_headers(),
                                                     timeout=timeout)
        #log.info("-->status={},content={},header={}".format(status,content,header))
        return status, content, header

    def view_results(self, bucket, ddoc_name, params, limit=100, timeout=120,
                     view_name=None):
        status, json = self._index_results(bucket, "view", ddoc_name, params, limit, timeout=timeout, view_name=view_name)
        if not status:
            raise Exception("unable to obtain view results")
        return json


    # DEPRECATED: Incorrectly named function kept for backwards compatibility.
    def get_view(self, bucket, view):
        log.info("DEPRECATED function get_view(" + view + "). use get_ddoc()")
        return self.get_ddoc(bucket, view)

    def get_data_path(self):
        node_info = self.get_nodes_self()
        data_path = node_info.storage[0].get_data_path()
        return data_path

    def get_memcached_port(self):
        node_info = self.get_nodes_self()
        return node_info.memcached

    def get_ddoc(self, bucket, ddoc_name):
        #log.info("-->get_ddoc..") 
        status, json, meta = self._get_design_doc(bucket, ddoc_name)
        if not status:
            raise ReadDocumentException(ddoc_name, json)
        return json, meta

    # the same as Preview a Random Document on UI
    def get_random_key(self, bucket):
        api = self.baseUrl + 'pools/default/buckets/%s/localRandomKey' % (bucket)
        status, content, header = self._http_request(api, headers=self._create_capi_headers())
        json_parsed = json.loads(content)
        if not status:
            raise Exception("unable to get random document/key for bucket %s" % (bucket))
        return json_parsed

    def create_collection(self, bucket, scope, collection):
        api = self.baseUrl + 'pools/default/buckets/%s/collections/%s' % (bucket, scope)
        body = {'name': collection}
        params = urllib.parse.urlencode(body)
        headers = self._create_headers()
        status, content, header = self._http_request(api, 'POST', params=params, headers=headers)
        return status

    def create_scope(self, bucket, scope):
        api = self.baseUrl + 'pools/default/buckets/%s/collections' % (bucket)
        body = {'name': scope}
        params = urllib.parse.urlencode(body)
        headers = self._create_headers()
        status, content, header = self._http_request(api, 'POST', params=params, headers=headers)
        return status

    def delete_scope(self, bucket, scope):
        api = self.baseUrl + 'pools/default/buckets/%s/collections/%s' % (bucket, scope)
        headers = self._create_headers()
        status, content, header = self._http_request(api, 'DELETE', headers=headers)
        return status

    def delete_collection(self, bucket, scope, collection):
        api = self.baseUrl + 'pools/default/buckets/%s/collections/%s/%s' % (bucket, scope, collection)
        headers = self._create_headers()
        status, content, header = self._http_request(api, 'DELETE', headers=headers)
        return status

    def run_view(self, bucket, view, name):
        api = self.capiBaseUrl + '/%s/_design/%s/_view/%s' % (bucket, view, name)
        status, content, header = self._http_request(api, headers=self._create_capi_headers())
        json_parsed = json.loads(content)
        if not status:
            raise Exception("unable to create view")
        return json_parsed

    def delete_view(self, bucket, view):
        status, json = self._delete_design_doc(bucket, view)
        if not status:
            raise Exception("unable to delete the view")
        return json

    def spatial_results(self, bucket, spatial, params, limit=100):
        status, json = self._index_results(bucket, "spatial", spatial,
                                           params, limit)
        if not status:
            raise Exception("unable to obtain spatial view results")
        return json

    def create_spatial(self, bucket, spatial, function):
        status, json = self._create_design_doc(bucket, spatial, function)
        if status == False:
            raise Exception("unable to create spatial view")
        return json

    def get_spatial(self, bucket, spatial):
        status, json, meta = self._get_design_doc(bucket, spatial)
        if not status:
            raise Exception("unable to get the spatial view definition")
        return json, meta

    def delete_spatial(self, bucket, spatial):
        status, json = self._delete_design_doc(bucket, spatial)
        if not status:
            raise Exception("unable to delete the spatial view")
        return json

    # type_ is "view" or "spatial"
    def _index_results(self, bucket, type_, ddoc_name, params, limit, timeout=120,
                       view_name=None):
        if view_name is None:
            view_name = ddoc_name
        query = '/{0}/_design/{1}/_{2}/{3}'
        api = self.capiBaseUrl + query.format(bucket, ddoc_name, type_, view_name)

        num_params = 0
        if limit != None:
            num_params = 1
            api += "?limit={0}".format(limit)
        for param in params:
            if num_params > 0:
                api += "&"
            else:
                api += "?"
            num_params += 1

            if param in ["key", "startkey", "endkey", "start_range",
                         "end_range"] or isinstance(params[param], bool):
                api += "{0}={1}".format(param,
                                        json.dumps(params[param],
                                                   separators=(',', ':')))
            else:
                api += "{0}={1}".format(param, params[param])

        log.info("index query url: {0}".format(api))
        status, content, header = self._http_request(api, headers=self._create_capi_headers(), timeout=timeout)
        json_parsed = json.loads(content)
        return status, json_parsed

    def get_couch_doc(self, doc_id, bucket="default", timeout=120):
        """ use couchBase uri to retrieve document from a bucket """
        api = self.capiBaseUrl + '/%s/%s' % (bucket, doc_id)
        status, content, header = self._http_request(api, headers=self._create_capi_headers(),
                                             timeout=timeout)
        if not status:
            raise ReadDocumentException(doc_id, content)
        return  json.loads(content)

    def _create_design_doc(self, bucket, name, function):
        api = self.capiBaseUrl + '/%s/_design/%s' % (bucket, name)
        status, content, header = self._http_request(
            api, 'PUT', function, headers=self._create_capi_headers())
        json_parsed = json.loads(content)
        #print("-->create_design_doc: {},{}".format(status,json_parsed))
        return status, json_parsed

    def _get_design_doc(self, bucket, name):
        api = self.capiBaseUrl + '/%s/_design/%s' % (bucket, name)
        if isinstance(bucket, Bucket):
            api = self.capiBaseUrl + '/%s/_design/%s' % (bucket.name, name)

        status, content, header = self._http_request(api, headers=self._create_capi_headers())
        #log.info("-->status={},content={},header={}".format(status,content,header))
        json_parsed = json.loads(content.decode())
        meta_parsed = ""
        if status:
            # in dp4 builds meta data is in content, not in header
            #log.info("-->X-Couchbase-Meta header value={}".format(header['X-Couchbase-Meta']))
            #log.info("-->x-couchbase-meta header value={}".format(header['x-couchbase-meta']))
            if 'X-Couchbase-Meta' in header:
                meta = header['X-Couchbase-Meta']
                meta_parsed = json.loads(meta)
            else:
                meta_parsed = {}
                try:
                  meta_parsed["_rev"] = json_parsed["_rev"]
                  meta_parsed["_id"] = json_parsed["_id"]
                except KeyError:
                  pass
        return status, json_parsed, meta_parsed

    def _delete_design_doc(self, bucket, name):
        status, design_doc, meta = self._get_design_doc(bucket, name)
        if not status:
            raise Exception("unable to find for deletion design document")
        api = self.capiBaseUrl + '/%s/_design/%s' % (bucket, name)
        if isinstance(bucket, Bucket):
            api = self.capiBaseUrl + '/%s/_design/%s' % (bucket.name, name)
        status, content, header = self._http_request(api, 'DELETE',
                                                     headers=self._create_capi_headers())
        json_parsed = json.loads(content)
        return status, json_parsed

    def spatial_compaction(self, bucket, design_name):
        api = self.capiBaseUrl + '/%s/_design/%s/_spatial/_compact' % (bucket, design_name)
        if isinstance(bucket, Bucket):
            api = self.capiBaseUrl + \
            '/%s/_design/%s/_spatial/_compact' % (bucket.name, design_name)

        status, content, header = self._http_request(api, 'POST',
                                                     headers=self._create_capi_headers())
        json_parsed = json.loads(content)
        return status, json_parsed

    # Make a _design/_info request
    def set_view_info(self, bucket, design_name):
        """Get view diagnostic info (node specific)"""
        api = self.capiBaseUrl
        if isinstance(bucket, Bucket):
            api += '/_set_view/{0}/_design/{1}/_info'.format(bucket.name, design_name)
        else:
            api += '_set_view/{0}/_design/{1}/_info'.format(bucket, design_name)

        status, content, header = self._http_request(api, 'GET',
                                                     headers=self._create_capi_headers())
        if not status:
            raise SetViewInfoNotFound(design_name, content)
        json_parsed = json.loads(content)
        return status, json_parsed

    # Make a _spatial/_info request
    def spatial_info(self, bucket, design_name):
        api = self.capiBaseUrl + \
            '/%s/_design/%s/_spatial/_info' % (bucket, design_name)
        status, content, header = self._http_request(
            api, 'GET', headers=self._create_capi_headers())
        json_parsed = json.loads(content)
        return status, json_parsed

    def _create_capi_headers(self):
        authorization = self.get_authorization(self.username, self.password)
        #log.info("-->authorization:{}:{}".format(type(authorization),authorization))
        return {'Content-Type': 'application/json',
                'Authorization': 'Basic %s' % authorization,
                'Accept': '*/*'}

    def _create_capi_headers_with_auth(self, username, password):
        authorization = self.get_authorization(username, password)
        return {'Content-Type': 'application/json',
                'Authorization': 'Basic %s' % authorization,
                'Accept': '*/*'}

    def _create_headers_with_auth(self, username, password):
        authorization = self.get_authorization(username, password)
        return {'Authorization': 'Basic %s' % authorization}

    # authorization must be a base64 string of username:password
    def _create_headers(self):
        authorization = self.get_authorization(self.username, self.password)
        return {'Content-Type': 'application/x-www-form-urlencoded',
                'Authorization': 'Basic %s' % authorization,
                'Accept': '*/*'}

    # authorization must be a base64 string of username:password
    def _create_headers_encoded_prepared(self):
        authorization = self.get_authorization(self.username, self.password)
        return {'Content-Type': 'application/json',
                'Authorization': 'Basic %s' % authorization}

    def _get_auth(self, headers):
        #log.info("-->_get_auth {}:{}".format(type(headers),headers))
        key = 'Authorization'
        if key in headers:
            val = headers[key]
            if val.startswith("Basic "):
              try:
                #log.info("-->val: {}:{}".format(type(val),val))
                val = val.encode()
                return str("auth: " + base64.decodebytes(val[6:]).decode())
              except Exception as e:
                print(e)
        return ""

    def _http_request(self, api, method='GET', params='', headers=None, timeout=120):
        if not headers:
            headers = self._create_headers()
        end_time = time.time() + timeout
        log.debug("Executing {0} request for following api {1} with Params: {2}  and Headers: {3}"\
                                                                .format(method, api, params, headers))
        count = 1
        while True:
            try:
                try:
                    if TestInputSingleton.input.param("debug.api.calls", False):
                        log.info("--->Start calling httplib2.Http({}).request({},{},{},{})".format(timeout,api,headers,method,params))
                except AttributeError:
                    pass
                if method == "POST" or method == "PUT":
                    response, content = httplib2.Http(timeout=timeout).request(api, method, params, headers=headers)
                else:
                    response, content = httplib2.Http(timeout=timeout).request(api, method, headers=headers)
                try:
                    if TestInputSingleton.input.param("debug.api.calls", False):
                        log.info(
                            "--->End calling httplib2.Http({}).request({},{},{},{})".format(timeout, api, headers,
                                                                                              method, params))
                except AttributeError:
                    pass

                if response['status'] in ['200', '201', '202']:
                    return True, content, response
                else:
                    try:
                        json_parsed = json.loads(content)
                    except ValueError as e:
                        json_parsed = {}
                        json_parsed["error"] = "status: {0}, content: {1}"\
                                                           .format(response['status'], content)
                    reason = "unknown"
                    if "error" in json_parsed:
                        reason = json_parsed["error"]
                    #log.info("--->content {}:{}".format(type(content),content))
                    message = '{0} {1} body: {2} headers: {3} error: {4} reason: {5} {6} {7}'.\
                              format(method, api, params, headers, response['status'], reason,
                                     str(str(content).rstrip('\n')), self._get_auth(headers))
                    log.error(message)
                    log.debug(''.join(traceback.format_stack()))
                    return False, content, response
            except socket.error as e:
                if count < 4:
                    log.error("socket error while connecting to {0} error {1} ".format(api, e))
                if time.time() > end_time:
                    log.error("Tried ta connect {0} times".format(count))
                    raise ServerUnavailableException(ip=self.ip)
            except httplib2.ServerNotFoundError as e:
                if count < 4:
                    log.error("ServerNotFoundError error while connecting to {0} error {1} "\
                                                                              .format(api, e))
                if time.time() > end_time:
                    log.error("Tried ta connect {0} times".format(count))
                    raise ServerUnavailableException(ip=self.ip)
            time.sleep(3)
            count += 1

    def init_cluster(self, username='Administrator', password='password', port='8091'):
        log.info("--> in init_cluster...{},{},{}".format(username,password,port))
        api = self.baseUrl + 'settings/web'
        params = urllib.parse.urlencode({'port': port,
                                   'username': username,
                                   'password': password})
        log.info('settings/web params on {0}:{1}:{2}'.format(self.ip, self.port, params))
        status, content, header = self._http_request(api, 'POST', params=params)
        log.info("--> status:{}".format(status))
        return status

    def init_node(self):
        """ need a standalone method to initialize a node that could call
            anywhere with quota from testconstant """
        log.info("--> init_node()...")
        try:
          self.node_services = []
          if self.services_node_init is None and self.services == "":
            self.node_services = ["kv"]
          elif self.services_node_init is None and self.services != "":
            self.node_services = self.services.split(",")
          elif self.services_node_init is not None:
            self.node_services = self.services_node_init.split("-")
          kv_quota = 0
          while kv_quota == 0:
            time.sleep(1)
            kv_quota = int(self.get_nodes_self().mcdMemoryReserved)
          info = self.get_nodes_self()
          kv_quota = int(info.mcdMemoryReserved * CLUSTER_QUOTA_RATIO)

          cb_version = info.version[:5]
          if cb_version in COUCHBASE_FROM_VERSION_4:
            if "index" in self.node_services:
                log.info("quota for index service will be %s MB" % (INDEX_QUOTA))
                kv_quota -= INDEX_QUOTA
                log.info("set index quota to node %s " % self.ip)
                self.set_service_memoryQuota(service='indexMemoryQuota', memoryQuota=INDEX_QUOTA)
            if "fts" in self.node_services:
                log.info("quota for fts service will be %s MB" % (FTS_QUOTA))
                kv_quota -= FTS_QUOTA
                log.info("set both index and fts quota at node %s "% self.ip)
                self.set_service_memoryQuota(service='ftsMemoryQuota', memoryQuota=FTS_QUOTA)
            if "cbas" in self.node_services:
                log.info("quota for cbas service will be %s MB" % (CBAS_QUOTA))
                kv_quota -= CBAS_QUOTA
                self.set_service_memoryQuota(service = "cbasMemoryQuota", memoryQuota=CBAS_QUOTA)
            kv_quota -= 1
            if kv_quota < MIN_KV_QUOTA:
                    raise Exception("KV RAM needs to be more than %s MB"
                            " at node  %s"  % (MIN_KV_QUOTA, self.ip))

          log.info("quota for kv: %s MB" % kv_quota)
          self.init_cluster_memoryQuota(self.username, self.password, kv_quota)
          if cb_version in COUCHBASE_FROM_VERSION_4:
            self.init_node_services(username=self.username, password=self.password,
                                                       services=self.node_services)
          self.init_cluster(username=self.username, password=self.password)
        except Exception as e:
          traceback.print_exc()

    def init_node_services(self, username='Administrator', password='password', hostname='127.0.0.1', port='8091', services=None):
        log.info("--> init_node_services({},{},{},{},{})".format(username,password,hostname,port,services))
        api = self.baseUrl + '/node/controller/setupServices'
        if services == None:
            log.info(" services are marked as None, will not work")
            return False
        if hostname == "127.0.0.1":
            hostname = "{0}:{1}".format(hostname, port)
        params = urllib.parse.urlencode({ 'hostname': hostname,
                                    'user': username,
                                    'password': password,
                                    'services': ",".join(services)})
        log.info('/node/controller/setupServices params on {0}: {1}:{2}'.format(self.ip, self.port, params))
        status, content, header = self._http_request(api, 'POST', params)
        error_message = "cannot change node services after cluster is provisioned"
        #log.info("--->{}:{},{}:{},{}:{}".format(type(status),status,type(error_message),error_message,type(content),content))
        if not status and error_message in str(content):
            status = True
            log.info("This node is already provisioned with services, we do not consider this as failure for test case")
        return status

    def get_cluster_settings(self):
        settings = {}
        api = self.baseUrl + 'settings/web'
        status, content, header = self._http_request(api, 'GET')
        if status:
            settings = json.loads(content)
        log.info('settings/web params on {0}:{1}:{2}'.format(self.ip, self.port, settings))
        return settings

    def init_cluster_memoryQuota(self, username='Administrator',
                                 password='password',
                                 memoryQuota=256):
        api = self.baseUrl + 'pools/default'
        params = urllib.parse.urlencode({'memoryQuota': memoryQuota})
        log.info('pools/default params : {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def set_service_memoryQuota(self, service, username='Administrator',
                                 password='password',
                                 memoryQuota=256):
        ''' cbasMemoryQuota for cbas service.
            ftsMemoryQuota for fts service.
            indexMemoryQuota for index service.'''
        api = self.baseUrl + 'pools/default'
        params = urllib.parse.urlencode({service: memoryQuota})
        log.info('pools/default params : {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def set_cluster_name(self, name):
        api = self.baseUrl + 'pools/default'
        if name is None:
            name = ""
        params = urllib.parse.urlencode({'clusterName': name})
        log.info('pools/default params : {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def set_indexer_storage_mode(self, username='Administrator',
                                 password='password',
                                 storageMode='plasma'):
        """
           StorageMode could be plasma or memopt
           From spock, we replace forestdb with plasma
        """
        api = self.baseUrl + 'settings/indexes'
        params = urllib.parse.urlencode({'storageMode': storageMode})
        error_message = "storageMode must be one of plasma, memory_optimized"
        log.info('settings/indexes params : {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        if not status and error_message in content.decode():
            #TODO: Currently it just acknowledges if there is an error.
            #And proceeds with further initialization.
            log.info(content)
        return status

    def set_indexer_num_replica(self,
                                 num_replica=0):
        api = self.index_baseUrl + 'settings'
        params = {'indexer.settings.num_replica': num_replica}
        params = json.dumps(params)
        status, content, header = self._http_request(api, 'POST',
                                                     params=params,
                                                     timeout=60)
        error_message = ""
        log.info('settings params : {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        if not status and error_message in content:
            # TODO: Currently it just acknowledges if there is an error.
            # And proceeds with further initialization.
            log.info(content)
        return status

    def cleanup_indexer_rebalance(self, server):
        if server:
            api = "http://{0}:{1}/".format(server.ip, self.index_port) + 'cleanupRebalance'
        else:
            api = self.baseUrl + 'cleanupRebalance'
        status, content, _ = self._http_request(api, 'GET')
        if status:
            return content
        else:
            log.error("cleanupRebalance:{0},content:{1}".format(status, content))
            raise Exception("indexer rebalance cleanup failed")

    def list_indexer_rebalance_tokens(self, server):
        if server:
            api = "http://{0}:{1}/".format(server.ip, self.index_port) + 'listRebalanceTokens'
        else:
            api = self.baseUrl + 'listRebalanceTokens'
        print(api)
        status, content, _ = self._http_request(api, 'GET')
        if status:
            return content
        else:
            log.error("listRebalanceTokens:{0},content:{1}".format(status, content))
            raise Exception("list rebalance tokens failed")

    def execute_statement_on_cbas(self, statement, mode, pretty=True,
                                  timeout=70, client_context_id=None,
                                  username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password
        api = self.cbas_base_url + "/analytics/service"
        headers = self._create_capi_headers_with_auth(username, password)

        params = {'statement': statement, 'mode': mode, 'pretty': pretty,
                  'client_context_id': client_context_id}
        params = json.dumps(params)
        status, content, header = self._http_request(api, 'POST',
                                                     headers=headers,
                                                     params=params,
                                                     timeout=timeout)
        if status:
            return content
        elif str(header['status']) == '503':
            log.info("Request Rejected")
            raise Exception("Request Rejected")
        elif str(header['status']) in ['500', '400']:
            json_content = json.loads(content)
            msg = json_content['errors'][0]['msg']
            if "Job requirement" in  msg and "exceeds capacity" in msg:
                raise Exception("Capacity cannot meet job requirement")
            else:
                return content
        else:
            log.error("/analytics/service status:{0},content:{1}".format(
                status, content))
            raise Exception("Analytics Service API failed")

    def delete_active_request_on_cbas(self, client_context_id, username=None, password=None):
        if not username:
            username = self.username
        if not password:
            password = self.password

        api = self.cbas_base_url + "/analytics/admin/active_requests?client_context_id={0}".format(
            client_context_id)
        headers = self._create_capi_headers_with_auth(username, password)

        status, content, header = self._http_request(api, 'DELETE',
                                                     headers=headers,
                                                     timeout=60)
        if status:
            return header['status']
        elif str(header['status']) == '404':
            log.info("Request Not Found")
            return header['status']
        else:
            log.error(
                "/analytics/admin/active_requests status:{0},content:{1}".format(
                    status, content))
            raise Exception("Analytics Admin API failed")

    def get_cluster_ceritificate(self):
        api = self.baseUrl + 'pools/default/certificate'
        status, content, _ = self._http_request(api, 'GET')
        if status:
            return content
        else:
            log.error("/poos/default/certificate status:{0},content:{1}".format(status, content))
            raise Exception("certificate API failed")

    def regenerate_cluster_certificate(self):
        api = self.baseUrl + 'controller/regenerateCertificate'
        status, content, _ = self._http_request(api, 'POST')
        if status:
            return content
        else:
            log.error("controller/regenerateCertificate status:{0},content:{1}".format(status, content))
            raise Exception("regenerateCertificate API failed")

    def __remote_clusters(self, api, op, remoteIp, remotePort, username, password, name, demandEncryption=0,
                          certificate='', encryptionType="half"):
        param_map = {'hostname': "{0}:{1}".format(remoteIp, remotePort),
                        'username': username,
                        'password': password,
                        'name':name}
        from TestInput import TestInputServer
        remote = TestInputServer()
        remote.ip = remoteIp
        remote.rest_username = username
        remote.rest_password = password
        remote.port = remotePort
        if demandEncryption:
            param_map ['demandEncryption'] = 'on'
            if certificate != '':
                param_map['certificate'] = certificate
            if self.check_node_versions("5.5") and RestConnection(remote).check_node_versions("5.5"):
                # 5.5.0 and above
                param_map['secureType'] = encryptionType
            elif self.check_node_versions("5.0") and RestConnection(remote).check_node_versions("5.0"):
                param_map['encryptionType'] = encryptionType
        params = urllib.parse.urlencode(param_map)
        status, content, _ = self._http_request(api, 'POST', params)
        # sample response :
        # [{"name":"two","uri":"/pools/default/remoteClusters/two","validateURI":"/pools/default/remoteClusters/two?just_validate=1","hostname":"127.0.0.1:9002","username":"Administrator"}]
        if status:
            remoteCluster = json.loads(content)
        else:
            log.error("/remoteCluster failed : status:{0},content:{1}".format(status, content))
            raise Exception("remoteCluster API '{0} remote cluster' failed".format(op))
        return remoteCluster

    def add_remote_cluster(self, remoteIp, remotePort, username, password, name, demandEncryption=0, certificate='',
                           encryptionType="full"):
        # example : password:password username:Administrator hostname:127.0.0.1:9002 name:two
        msg = "adding remote cluster hostname:{0}:{1} with username:password {2}:{3} name:{4} to source node: {5}:{6}"
        log.info(msg.format(remoteIp, remotePort, username, password, name, self.ip, self.port))
        api = self.baseUrl + 'pools/default/remoteClusters'
        return self.__remote_clusters(api, 'add', remoteIp, remotePort, username, password, name, demandEncryption, certificate, encryptionType)

    def add_remote_cluster_new(self, remoteIp, remotePort, username, password, name, demandEncryption=0, certificate=''):
        # example : password:password username:Administrator hostname:127.0.0.1:9002 name:two
        msg = "adding remote cluster hostname:{0}:{1} with username:password {2}:{3} name:{4} to source node: {5}:{6}"
        log.info(msg.format(remoteIp, remotePort, username, password, name, self.ip, self.port))
        api = self.baseUrl + 'pools/default/remoteClusters'
        return self.__remote_clusters(api, 'add', remoteIp, remotePort, username, password, name, demandEncryption, certificate)

    def modify_remote_cluster(self, remoteIp, remotePort, username, password, name, demandEncryption=0, certificate='', encryptionType="half"):
        log.info("modifying remote cluster name:{0}".format(name))
        api = self.baseUrl + 'pools/default/remoteClusters/' + urllib.parse.quote(name)
        return self.__remote_clusters(api, 'modify', remoteIp, remotePort, username, password, name, demandEncryption, certificate, encryptionType)

    def get_remote_clusters(self):
        remote_clusters = []
        api = self.baseUrl + 'pools/default/remoteClusters/'
        params = urllib.parse.urlencode({})
        status, content, header = self._http_request(api, 'GET', params)
        if status:
            remote_clusters = json.loads(content)
        return remote_clusters

    def remove_all_remote_clusters(self):
        remote_clusters = self.get_remote_clusters()
        for remote_cluster in remote_clusters:
            try:
                if remote_cluster["deleted"] == False:
                    self.remove_remote_cluster(remote_cluster["name"])
            except KeyError:
                # goxdcr cluster references will not contain "deleted" field
                self.remove_remote_cluster(remote_cluster["name"])

    def remove_remote_cluster(self, name):
        # example : name:two
        msg = "removing remote cluster name:{0}".format(urllib.parse.quote(name))
        log.info(msg)
        api = self.baseUrl + 'pools/default/remoteClusters/{0}?'.format(urllib.parse.quote(name))
        params = urllib.parse.urlencode({})
        status, content, header = self._http_request(api, 'DELETE', params)
        #sample response : "ok"
        if not status:
            log.error("failed to remove remote cluster: status:{0},content:{1}".format(status, content))
            raise Exception("remoteCluster API 'remove cluster' failed")

    # replicationType:continuous toBucket:default toCluster:two fromBucket:default
    # defaults at https://github.com/couchbase/goxdcr/metadata/replication_settings.go#L20-L33
    def start_replication(self, replicationType, fromBucket, toCluster, rep_type="xmem", toBucket=None, xdcr_params={}):
        toBucket = toBucket or fromBucket


        msg = "starting {0} replication type:{1} from {2} to {3} in the remote" \
              " cluster {4} with settings {5}"
        log.info(msg.format(replicationType, rep_type, fromBucket, toBucket,
                            toCluster, xdcr_params))
        api = self.baseUrl + 'controller/createReplication'
        param_map = {'replicationType': replicationType,
                     'toBucket': toBucket,
                     'fromBucket': fromBucket,
                     'toCluster': toCluster,
                     'type': rep_type}
        param_map.update(xdcr_params)
        params = urllib.parse.urlencode(param_map)
        status, content, _ = self._http_request(api, 'POST', params)
        # response : {"id": "replication_id"}
        if status:
            json_parsed = json.loads(content)
            log.info("Replication created with id: {0}".format(json_parsed['id']))
            return json_parsed['id']
        else:
            log.error("/controller/createReplication failed : status:{0},content:{1}".format(status, content))
            raise Exception("create replication failed : status:{0},content:{1}".format(status, content))

    def get_replications(self):
        replications = []
        content = self.ns_server_tasks()
        for item in content:
            if not isinstance(item, dict):
                log.error("Unexpected error while retrieving pools/default/tasks : {0}".format(content))
                raise Exception("Unexpected error while retrieving pools/default/tasks : {0}".format(content))
            if item["type"] == "xdcr":
                replications.append(item)
        return replications

    def remove_all_replications(self):
        replications = self.get_replications()
        for replication in replications:
            self.stop_replication(replication["cancelURI"])

    def stop_replication(self, uri):
        log.info("Deleting replication {0}".format(uri))
        api = self.baseUrl[:-1] + uri
        status, content, _ = self._http_request(api, 'DELETE')
        if status:
            log.info("Replication deleted successfully")
        else:
            log.error("/controller/cancelXDCR failed: status:{0}, content:{1}".format(status, content))
            raise Exception("delete replication failed : status:{0}, content:{1}".format(status, content))

    def remove_all_recoveries(self):
        recoveries = []
        content = self.ns_server_tasks()
        for item in content:
            if item["type"] == "recovery":
                recoveries.append(item)
        for recovery in recoveries:
            api = self.baseUrl + recovery["stopURI"]
            status, content, header = self._http_request(api, 'POST')
            if not status:
                raise CBRecoveryFailedException("impossible to stop cbrecovery by {0}".format(api))
            log.info("recovery stopped by {0}".format(api))

    # params serverIp : the server to add to this cluster
    # raises exceptions when
    # unauthorized user
    # server unreachable
    # can't add the node to itself ( TODO )
    # server already added
    # returns otpNode
    def add_node(self, user='', password='', remoteIp='', port='8091', zone_name='', services=None):
        otpNode = None

        # if ip format is ipv6 and enclosing brackets are not found,
        # enclose self.ip and remoteIp
        if self.ip.count(':') and self.ip[0] != '[':
            self.ip = '[' + self.ip + ']'
        if remoteIp.count(':') and remoteIp[0] != '[':
            remoteIp = '[' + remoteIp + ']'

        log.info('adding remote node @{0}:{1} to this cluster @{2}:{3}'\
                          .format(remoteIp, port, self.ip, self.port))
        if zone_name == '':
            api = self.baseUrl + 'controller/addNode'
        else:
            api = self.baseUrl + 'pools/default/serverGroups'
            if self.is_zone_exist(zone_name):
                zones = self.get_zone_names()
                api = "/".join((api, zones[zone_name], "addNode"))
                log.info("node {0} will be added to zone {1}".format(remoteIp, zone_name))
            else:
                raise Exception("There is not zone with name: %s in cluster" % zone_name)

        params = urllib.parse.urlencode({'hostname': "http://{0}:{1}".format(remoteIp, port),
                                   'user': user,
                                   'password': password})
        if services != None:
            services = ','.join(services)
            params = urllib.parse.urlencode({'hostname': "http://{0}:{1}".format(remoteIp, port),
                                   'user': user,
                                   'password': password,
                                   'services': services})
        status, content, header = self._http_request(api, 'POST', params)
        if status:
            json_parsed = json.loads(content)
            otpNodeId = json_parsed['otpNode']
            otpNode = OtpNode(otpNodeId)
            if otpNode.ip == '127.0.0.1':
                otpNode.ip = self.ip
        else:
            self.print_UI_logs()
            try:
                # print logs from node that we want to add
                wanted_node = deepcopy(self)
                wanted_node.ip = remoteIp
                wanted_node.print_UI_logs()
            except Exception as ex:
                self.log(ex)
            if content.find(b'Prepare join failed. Node is already part of cluster') >= 0:
                raise ServerAlreadyJoinedException(nodeIp=self.ip,
                                                   remoteIp=remoteIp)
            elif content.find(b'Prepare join failed. Joining node to itself is not allowed') >= 0:
                raise ServerSelfJoinException(nodeIp=self.ip,
                                          remoteIp=remoteIp)
            else:
                log.error('add_node error : {0}'.format(content))
                raise AddNodeException(nodeIp=self.ip,
                                          remoteIp=remoteIp,
                                          reason=content)
        return otpNode

        # params serverIp : the server to add to this cluster
    # raises exceptions when
    # unauthorized user
    # server unreachable
    # can't add the node to itself ( TODO )
    # server already added
    # returns otpNode
    def do_join_cluster(self, user='', password='', remoteIp='', port='8091', zone_name='', services=None):
        otpNode = None
        log.info('adding remote node @{0}:{1} to this cluster @{2}:{3}'\
                          .format(remoteIp, port, self.ip, self.port))
        api = self.baseUrl + '/node/controller/doJoinCluster'
        params = urllib.parse.urlencode({'hostname': "{0}:{1}".format(remoteIp, port),
                                   'user': user,
                                   'password': password})
        if services != None:
            services = ','.join(services)
            params = urllib.parse.urlencode({'hostname': "{0}:{1}".format(remoteIp, port),
                                   'user': user,
                                   'password': password,
                                   'services': services})
        status, content, header = self._http_request(api, 'POST', params)
        if status:
            json_parsed = json.loads(content)
            otpNodeId = json_parsed['otpNode']
            otpNode = OtpNode(otpNodeId)
            if otpNode.ip == '127.0.0.1':
                otpNode.ip = self.ip
        else:
            self.print_UI_logs()
            try:
                # print logs from node that we want to add
                wanted_node = deepcopy(self)
                wanted_node.ip = remoteIp
                wanted_node.print_UI_logs()
            except Exception as ex:
                self.log(ex)
            if content.find('Prepare join failed. Node is already part of cluster') >= 0:
                raise ServerAlreadyJoinedException(nodeIp=self.ip,
                                                   remoteIp=remoteIp)
            elif content.find('Prepare join failed. Joining node to itself is not allowed') >= 0:
                raise ServerSelfJoinException(nodeIp=self.ip,
                                          remoteIp=remoteIp)
            else:
                log.error('add_node error : {0}'.format(content))
                raise AddNodeException(nodeIp=self.ip,
                                          remoteIp=remoteIp,
                                          reason=content)
        return otpNode


    def eject_node(self, user='', password='', otpNode=None):
        if not otpNode:
            log.error('otpNode parameter required')
            return False
        api = self.baseUrl + 'controller/ejectNode'
        params = urllib.parse.urlencode({'otpNode': otpNode,
                                   'user': user,
                                   'password': password})
        status, content, header = self._http_request(api, 'POST', params)
        if status:
            log.info('ejectNode successful')
        else:
            if content.find('Prepare join failed. Node is already part of cluster') >= 0:
                raise ServerAlreadyJoinedException(nodeIp=self.ip,
                                                   remoteIp=otpNode)
            else:
                # TODO : raise an exception here
                log.error('eject_node error {0}'.format(content))
        return True

    def force_eject_node(self):
        self.diag_eval("gen_server:cast(ns_cluster, leave).")
        self.check_delay_restart_coucbase_server()

    """ when we do reset couchbase server by force reject, couchbase server will not
        down right away but delay few seconds to be down depend on server spec.
        This fx will detect that delay and return true when couchbase server down and
        up again after force reject """
    def check_delay_restart_coucbase_server(self):
        api = self.baseUrl + 'nodes/self'
        headers = self._create_headers()
        break_out = 0
        count_cbserver_up = 0
        while break_out < 60 and count_cbserver_up < 2:
            try:
                response, content = httplib2.Http(timeout=120).request(api, 'GET', '', headers)
                if response['status'] in ['200', '201', '202'] and count_cbserver_up == 0:
                    log.info("couchbase server is up but down soon.")
                    time.sleep(1)
                    break_out += 1  # time needed for couchbase server reload after reset config
                elif response['status'] in ['200', '201', '202']:
                    count_cbserver_up = 2
                    log.info("couchbase server is up again")
            except (socket.error, AttributeError) as e:
                log.info("couchbase server is down.  Waiting for couchbase server up")
                time.sleep(2)
                break_out += 1
                count_cbserver_up = 1
                pass
        if break_out >= 60:
            raise Exception("Couchbase server did not start after 60 seconds")

    def fail_over(self, otpNode=None, graceful=False):
        if otpNode is None:
            log.error('otpNode parameter required')
            return False
        api = self.baseUrl + 'controller/failOver'
        if graceful:
            api = self.baseUrl + 'controller/startGracefulFailover'
        params = urllib.parse.urlencode({'otpNode': otpNode})
        status, content, header = self._http_request(api, 'POST', params)
        if status:
            log.info('fail_over node {0} successful'.format(otpNode))
        else:
            log.error('fail_over node {0} error : {1}'.format(otpNode, content))
            raise FailoverFailedException(content)
        return status

    def set_recovery_type(self, otpNode=None, recoveryType=None):
        log.info("Going to set recoveryType={0} for node :: {1}".format(recoveryType, otpNode))
        if otpNode is None:
            log.error('otpNode parameter required')
            return False
        if recoveryType is None:
            log.error('recoveryType is not set')
            return False
        api = self.baseUrl + 'controller/setRecoveryType'
        params = urllib.parse.urlencode({'otpNode': otpNode,
                                   'recoveryType': recoveryType})
        status, content, header = self._http_request(api, 'POST', params)
        if status:
            log.info('recoveryType for node {0} set successful'.format(otpNode))
        else:
            log.error('recoveryType node {0} not set with error : {1}'.format(otpNode, content))
            raise SetRecoveryTypeFailed(content)
        return status

    def add_back_node(self, otpNode=None):
        if otpNode is None:
            log.error('otpNode parameter required')
            return False
        api = self.baseUrl + 'controller/reAddNode'
        params = urllib.parse.urlencode({'otpNode': otpNode})
        status, content, header = self._http_request(api, 'POST', params)
        if status:
            log.info('add_back_node {0} successful'.format(otpNode))
        else:
            log.error('add_back_node {0} error : {1}'.format(otpNode, content))
            raise InvalidArgumentException('controller/reAddNode',
                                           parameters=params)
        return status

    def rebalance(self, otpNodes=[], ejectedNodes=[], deltaRecoveryBuckets=None):
        knownNodes = ','.join(otpNodes)
        ejectedNodesString = ','.join(ejectedNodes)
        if deltaRecoveryBuckets == None:
            params = {'knownNodes': knownNodes,
                                    'ejectedNodes': ejectedNodesString,
                                    'user': self.username,
                                    'password': self.password}
        else:
            deltaRecoveryBuckets = ",".join(deltaRecoveryBuckets)
            params = {'knownNodes': knownNodes,
                      'ejectedNodes': ejectedNodesString,
                      'deltaRecoveryBuckets': deltaRecoveryBuckets,
                      'user': self.username,
                      'password': self.password}
        log.info('rebalance params : {0}'.format(params))
        params = urllib.parse.urlencode(params)
        api = self.baseUrl + "controller/rebalance"
        status, content, header = self._http_request(api, 'POST', params)
        if status:
            log.info('rebalance operation started')
        else:
            log.error('rebalance operation failed: {0}'.format(content))
            # extract the error
            raise InvalidArgumentException('controller/rebalance with error message {0}'.format(content),
                                           parameters=params)
        return status

    def diag_eval(self, code, print_log=True):
        api = '{0}{1}'.format(self.baseUrl, 'diag/eval/')
        status, content, header = self._http_request(api, "POST", code)
        if print_log:
            log.info("/diag/eval status on {0}:{1}: {2} content: {3} command: {4}".
                     format(self.ip, self.port, status, content, code))
        return status, content

    def set_chk_max_items(self, max_items):
        status, content = self.diag_eval("ns_config:set(chk_max_items, " + str(max_items) + ")")
        return status, content

    def set_chk_period(self, period):
        status, content = self.diag_eval("ns_config:set(chk_period, " + str(period) + ")")
        return status, content

    def set_enable_flow_control(self, flow=True, bucket='default'):
        flow_control = "false"
        if flow:
            flow_control = "true"
        code = "ns_bucket:update_bucket_props(\"" + bucket + "\", [{extra_config_string, \"upr_enable_flow_control=" + flow_control + "\"}])"
        status, content = self.diag_eval(code)
        return status, content

    def change_flusher_batch_split_trigger(self, flusher_batch_split_trigger=3,
                                           bucket='default'):
        code = "ns_bucket:update_bucket_props(\"" + bucket \
               + "\", [{extra_config_string, " \
               + "\"flusher_batch_split_trigger=" \
               + str(flusher_batch_split_trigger) + "\"}])."
        status, content = self.diag_eval(code)
        return status, content

    def diag_master_events(self):
        api = '{0}{1}'.format(self.baseUrl, 'diag/masterEvents?o=1')
        status, content, header = self._http_request(api, "GET")
        log.info("diag/masterEvents?o=1 status: {0} content: {1}".format(status, content))
        return status, content


    def get_admin_credentials(self):

        code = 'ns_config:search_node_prop(node(), ns_config:latest(), memcached, admin_user)'
        status, id = self.diag_eval(code)

        code = 'ns_config:search_node_prop(node(), ns_config:latest(), memcached, admin_pass)'
        status, password = self.diag_eval(code)
        return id.strip('"'), password.strip('"')

    def monitorRebalance(self, stop_if_loop=True):
        start = time.time()
        progress = 0
        retry = 0
        same_progress_count = 0
        previous_progress = 0
        while progress != -1 and (progress != 100 or \
                    self._rebalance_progress_status() == 'running') and retry < 20:
            # -1 is error , -100 means could not retrieve progress
            progress = self._rebalance_progress()
            if progress == -100:
                log.error("unable to retrieve rebalanceProgress.try again in 1 second")
                retry += 1
            else:
                retry = 0
            if stop_if_loop:
                # reset same_progress_count if get a different result,
                # or progress is still O
                # (it may take a long time until the results are different from 0)
                if previous_progress != progress or progress == 0:
                    previous_progress = progress
                    same_progress_count = 0
                else:
                    same_progress_count += 1
                if same_progress_count > 50:
                    log.error("apparently rebalance progress code in infinite loop:"
                                                             " {0}".format(progress))
                    return False
            # sleep 10 seconds to printout less log
            time.sleep(10)
        if progress < 0:
            log.error("rebalance progress code : {0}".format(progress))
            return False
        else:
            duration = time.time() - start
            if duration > 10:
                sleep = 10
            else:
                sleep = duration
            log.info('rebalance progress took {:.02f} seconds '.format(duration))
            log.info("sleep for {0} seconds after rebalance...".format(sleep))
            time.sleep(sleep)
            return True

    def _rebalance_progress_status(self):
        api = self.baseUrl + "pools/default/rebalanceProgress"
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            if "status" in json_parsed:
                return json_parsed['status']
        else:
            return None

    def _rebalance_status_and_progress(self):
        """
        Returns a 2-tuple capturing the rebalance status and progress, as follows:
            ('running', progress) - if rebalance is running
            ('none', 100)         - if rebalance is not running (i.e. assumed done)
            (None, -100)          - if there's an error getting the rebalance progress
                                    from the server
            (None, -1)            - if the server responds but there's no information on
                                    what the status of rebalance is

        The progress is computed as a average of the progress of each node
        rounded to 2 decimal places.

        Throws RebalanceFailedException if rebalance progress returns an error message
        """
        avg_percentage = -1
        rebalance_status = None
        api = self.baseUrl + "pools/default/rebalanceProgress"
        try:
            status, content, header = self._http_request(api)
        except ServerUnavailableException as e:
            log.error(e)
            return None, -100
        json_parsed = json.loads(content)
        if status:
            if "status" in json_parsed:
                rebalance_status = json_parsed["status"]
                if "errorMessage" in json_parsed:
                    msg = '{0} - rebalance failed'.format(json_parsed)
                    log.error(msg)
                    self.print_UI_logs()
                    raise RebalanceFailedException(msg)
                elif rebalance_status == "running":
                    total_percentage = 0
                    count = 0
                    for key in json_parsed:
                        if key.find('@') >= 0:
                            ns_1_dictionary = json_parsed[key]
                            percentage = ns_1_dictionary['progress'] * 100
                            count += 1
                            total_percentage += percentage
                    if count:
                        avg_percentage = (total_percentage // count)
                    else:
                        avg_percentage = 0
                    log.info('rebalance percentage : {0:.02f} %'.
                             format(round(avg_percentage, 2)))
                else:
                    avg_percentage = 100
        else:
            avg_percentage = -100
        return rebalance_status, avg_percentage

    def _rebalance_progress(self):
        return self._rebalance_status_and_progress()[1]

    def log_client_error(self, post):
        api = self.baseUrl + 'logClientError'
        status, content, header = self._http_request(api, 'POST', post)
        if not status:
            log.error('unable to logClientError')
        return status, content, header

    def trigger_index_compaction(self, timeout=120):
        node = None
        api = self.index_baseUrl + 'triggerCompaction'
        status, content, header = self._http_request(api, timeout=timeout)
        if not status:
            raise Exception(content)

    def set_index_settings(self, setting_json, timeout=120):
        api = self.index_baseUrl + 'settings'
        status, content, header = self._http_request(api, 'POST', json.dumps(setting_json))
        if not status:
            raise Exception(content)
        log.info("{0} set".format(setting_json))

    def set_index_settings_internal(self, setting_json, timeout=120):
        api = self.index_baseUrl + 'internal/settings'
        status, content, header = self._http_request(api, 'POST',
                                                     json.dumps(setting_json))
        if not status:
            if header['status']=='404':
                log.info("This endpoint is introduced only in 5.5.0, hence not found. Redirecting the request to the old endpoint")
                self.set_index_settings(setting_json, timeout)
            else:
                raise Exception(content)
        log.info("{0} set".format(setting_json))

    def get_index_settings(self, timeout=120):
        node = None
        api = self.index_baseUrl + 'settings'
        status, content, header = self._http_request(api, timeout=timeout)
        if not status:
            raise Exception(content)
        return json.loads(content)

    def set_index_planner_settings(self, setting, timeout=120):
        api = self.index_baseUrl + 'settings/planner?{0}'.format(setting)
        status, content, header = self._http_request(api, timeout=timeout)
        if not status:
            raise Exception(content)
        return json.loads(content)

    def get_index_stats(self, timeout=120, index_map=None):
        api = self.index_baseUrl + 'stats'
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            index_map = RestParser().parse_index_stats_response(json_parsed, index_map=index_map)
        return index_map

    def get_index_official_stats(self, timeout=120, index_map=None):
        api = self.index_baseUrl + 'api/v1/stats'
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
        return json_parsed

    def get_index_storage_stats(self, timeout=120, index_map=None):
        api = self.index_baseUrl + 'stats/storage'
        status, content, header = self._http_request(api, timeout=timeout)
        if not status:
            raise Exception(content)
        json_parsed = json.loads(content)
        index_storage_stats = {}
        for index_stats in json_parsed:
            bucket = index_stats["Index"].split(":")[0]
            index_name = index_stats["Index"].split(":")[1]
            if not bucket in list(index_storage_stats.keys()):
                index_storage_stats[bucket] = {}
            index_storage_stats[bucket][index_name] = index_stats["Stats"]
        return index_storage_stats

    def get_indexer_stats(self, timeout=120, index_map=None, baseUrl=None):
        if baseUrl is None:
            api = self.index_baseUrl + 'stats'
        else:
            api = baseUrl + 'stats'
        index_map = {}
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            for key in list(json_parsed.keys()):
                tokens = key.split(":")
                val = json_parsed[key]
                if len(tokens) == 1:
                    field = tokens[0]
                    index_map[field] = val
        return index_map

    def get_indexer_metadata(self, timeout=120, index_map=None):
        api = self.index_baseUrl + 'getIndexStatus'
        index_map = {}
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            for key in list(json_parsed.keys()):
                tokens = key.split(":")
                val = json_parsed[key]
                if len(tokens) == 1:
                    field = tokens[0]
                    index_map[field] = val
        return index_map

    def get_indexer_internal_stats(self, timeout=120, index_map=None):
        api = self.index_baseUrl + 'settings?internal=ok'
        index_map = {}
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            for key in list(json_parsed.keys()):
                tokens = key.split(":")
                val = json_parsed[key]
                if len(tokens) == 1:
                    field = tokens[0]
                    index_map[field] = val
        return index_map

    def get_index_status(self, timeout=120, index_map=None):
        api = self.baseUrl + 'indexStatus'
        index_map = {}
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            index_map = RestParser().parse_index_status_response(json_parsed)
        return index_map

    def get_index_id_map(self, timeout=120):
        api = self.baseUrl + 'indexStatus'
        index_map = {}
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            for map in json_parsed["indexes"]:
                bucket_name = map['bucket'].encode('ascii', 'ignore')
                if bucket_name not in list(index_map.keys()):
                    index_map[bucket_name] = {}
                index_name = map['index'].encode('ascii', 'ignore')
                index_map[bucket_name][index_name] = {}
                index_map[bucket_name][index_name]['id'] = map['id']
        return index_map

    def get_index_statements(self, timeout=120):
        api = self.index_baseUrl + 'getIndexStatement'
        index_map = {}
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
        return json_parsed

    # returns node data for this host
    def get_nodes_self(self, timeout=120):
        node = None
        api = self.baseUrl + 'nodes/self'
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            node = RestParser().parse_get_nodes_response(json_parsed)
        return node

    def node_statuses(self, timeout=120):
        nodes = []
        api = self.baseUrl + 'nodeStatuses'
        status, content, header = self._http_request(api, timeout=timeout)
        json_parsed = json.loads(content)
        if status:
            for key in json_parsed:
                # each key contain node info
                value = json_parsed[key]
                # get otp,get status
                node = OtpNode(id=value['otpNode'],
                               status=value['status'])
                if node.ip == 'cb.local':
                    node.ip = self.ip
                    node.id = node.id.replace('cb.local',
                                              self.ip.__str__())
                if node.ip == '127.0.0.1':
                    node.ip = self.ip
                node.port = int(key[key.rfind(":") + 1:])
                node.replication = value['replication']
                if 'gracefulFailoverPossible' in list(value.keys()):
                    node.gracefulFailoverPossible = value['gracefulFailoverPossible']
                else:
                    node.gracefulFailoverPossible = False
                nodes.append(node)
        return nodes

    def cluster_status(self):
        parsed = {}
        api = self.baseUrl + 'pools/default'
        status, content, header = self._http_request(api)
        if status:
            parsed = json.loads(content)
        return parsed

    def fetch_vbucket_map(self, bucket="default"):
        """Return vbucket map for bucket
        Keyword argument:
        bucket -- bucket name
        """
        api = self.baseUrl + 'pools/default/buckets/' + bucket
        status, content, header = self._http_request(api)
        _stats = json.loads(content)
        return _stats['vBucketServerMap']['vBucketMap']

    def get_vbucket_map_and_server_list(self, bucket="default"):
        """ Return server list, replica and vbuckets map
        that matches to server list """
        vbucket_map = self.fetch_vbucket_map(bucket)
        api = self.baseUrl + 'pools/default/buckets/' + bucket
        status, content, header = self._http_request(api)
        _stats = json.loads(content)
        num_replica = _stats['vBucketServerMap']['numReplicas']
        vbucket_map = _stats['vBucketServerMap']['vBucketMap']
        servers = _stats['vBucketServerMap']['serverList']
        server_list = []
        for node in servers:
            node = node.split(":")
            server_list.append(node[0])
        return vbucket_map, server_list, num_replica

    def get_pools_info(self):
        parsed = {}
        api = self.baseUrl + 'pools'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            parsed = json_parsed
        return parsed

    def get_pools_default(self, query='', timeout=30):
        parsed = {}
        api = self.baseUrl + 'pools/default'
        if query:
            api += "?" + query

        status, content, header = self._http_request(api, timeout=timeout)
        json_parsed = json.loads(content)
        if status:
            parsed = json_parsed
        return parsed

    def get_cluster_stats(self):
        """
        Reads cluster nodes statistics using `pools/default` rest GET method
        :return stat_dict - Dictionary of CPU & Memory status each cluster node:
        """
        stat_dict = dict()
        json_output = self.get_pools_default()
        if 'nodes' in json_output:
            for node_stat in json_output['nodes']:
                stat_dict[node_stat['hostname']] = dict()
                stat_dict[node_stat['hostname']]['services'] = node_stat['services']
                stat_dict[node_stat['hostname']]['cpu_utilization'] = node_stat['systemStats']['cpu_utilization_rate']
                stat_dict[node_stat['hostname']]['mem_free'] = node_stat['systemStats']['mem_free']
                stat_dict[node_stat['hostname']]['mem_total'] = node_stat['systemStats']['mem_total']
                stat_dict[node_stat['hostname']]['swap_mem_used'] = node_stat['systemStats']['swap_used']
                stat_dict[node_stat['hostname']]['swap_mem_total'] = node_stat['systemStats']['swap_total']
        return stat_dict

    def get_pools(self):
        version = None
        api = self.baseUrl + 'pools'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            version = MembaseServerVersion(json_parsed['implementationVersion'], json_parsed['componentsVersion'])
        return version

    def get_buckets(self):
        # get all the buckets
        buckets = []
        api = '{0}{1}'.format(self.baseUrl, 'pools/default/buckets?basic_stats=true')
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            for item in json_parsed:
                bucketInfo = RestParser().parse_get_bucket_json(item)
                buckets.append(bucketInfo)
        return buckets

    def get_buckets_itemCount(self):
        # get all the buckets
        bucket_map = {}
        api = '{0}{1}'.format(self.baseUrl, 'pools/default/buckets?basic_stats=true')
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            for item in json_parsed:
                bucketInfo = RestParser().parse_get_bucket_json(item)
                bucket_map[bucketInfo.name] = bucketInfo.stats.itemCount
        log.info(bucket_map)
        return bucket_map

    def get_bucket_stats_for_node(self, bucket='default', node=None):
        if not node:
            log.error('node_ip not specified')
            return None
        stats = {}
        api = "{0}{1}{2}{3}{4}:{5}{6}".format(self.baseUrl, 'pools/default/buckets/',
                                     bucket, "/nodes/", node.ip, node.port, "/stats")
        status, content, header = self._http_request(api)
        if status:
            json_parsed = json.loads(content)
            op = json_parsed["op"]
            samples = op["samples"]
            for stat_name in samples:
                if stat_name not in stats:
                    if len(samples[stat_name]) == 0:
                        stats[stat_name] = []
                    else:
                        stats[stat_name] = samples[stat_name][-1]
                else:
                    raise Exception("Duplicate entry in the stats command {0}".format(stat_name))
        return stats



    def get_bucket_status(self, bucket):
        if not bucket:
            log.error("Bucket Name not Specified")
            return None
        api = self.baseUrl + 'pools/default/buckets'
        status, content, header = self._http_request(api)
        if status:
            json_parsed = json.loads(content)
            for item in json_parsed:
                if item["name"] == bucket:
                    return item["nodes"][0]["status"]
            log.error("Bucket {} doesn't exist".format(bucket))
            return None

    def fetch_bucket_stats(self, bucket='default', zoom='minute'):
        """Return deserialized buckets stats.
        Keyword argument:
        bucket -- bucket name
        zoom -- stats zoom level (minute | hour | day | week | month | year)
        """
        api = self.baseUrl + 'pools/default/buckets/{0}/stats?zoom={1}'.format(bucket, zoom)
        log.info(api)
        status, content, header = self._http_request(api)
        return json.loads(content)

    def set_query_index_api_mode(self, index_api_mode=3):
        api = self.query_baseUrl + 'admin/settings'
        query_api_setting = {"max-index-api": index_api_mode}
        status, content, header = self._http_request(api, 'POST', json.dumps(query_api_setting))
        if not status:
            raise Exception(content)
        log.info("{0} set".format(query_api_setting))

    def fetch_bucket_xdcr_stats(self, bucket='default', zoom='minute'):
        """Return deserialized bucket xdcr stats.
        Keyword argument:
        bucket -- bucket name
        zoom -- stats zoom level (minute | hour | day | week | month | year)
        """
        api = self.baseUrl + 'pools/default/buckets/@xdcr-{0}/stats?zoom={1}'.format(bucket, zoom)
        status, content, header = self._http_request(api)
        return json.loads(content)

    def fetch_system_stats(self):
        """Return deserialized system stats."""
        api = self.baseUrl + 'pools/default/'
        status, content, header = self._http_request(api)
        return json.loads(content)

    def get_xdc_queue_size(self, bucket):
        """Fetch bucket stats and return the latest value of XDC replication
        queue size"""
        bucket_stats = self.fetch_bucket_xdcr_stats(bucket)
        return bucket_stats['op']['samples']['replication_changes_left'][-1]

    def get_dcp_queue_size(self, bucket):
        """Fetch bucket stats and return the latest value of DCP
        queue size"""
        bucket_stats = self.fetch_bucket_stats(bucket)
        return bucket_stats['op']['samples']['ep_dcp_xdcr_items_remaining'][-1]

    def get_active_key_count(self, bucket):
        """Fetch bucket stats and return the bucket's curr_items count"""
        bucket_stats = self.fetch_bucket_stats(bucket)
        return bucket_stats['op']['samples']['curr_items'][-1]

    def get_replica_key_count(self, bucket):
        """Fetch bucket stats and return the bucket's replica count"""
        bucket_stats = self.fetch_bucket_stats(bucket)
        return bucket_stats['op']['samples']['vb_replica_curr_items'][-1]

    def get_nodes(self):
        nodes = []
        api = self.baseUrl + 'pools/default'
        status, content, header = self._http_request(api)
        count = 0
        while not content and count < 7:
            log.info("sleep 5 seconds and retry")
            time.sleep(5)
            status, content, header = self._http_request(api)
            count += 1
        if count == 7:
            raise Exception("could not get node info after 30 seconds")
        json_parsed = json.loads(content)
        if status:
            if "nodes" in json_parsed:
                for json_node in json_parsed["nodes"]:
                    node = RestParser().parse_get_nodes_response(json_node)
                    node.rest_username = self.username
                    node.rest_password = self.password
                    if node.ip == "127.0.0.1":
                        node.ip = self.ip
                    # Only add nodes which are active on cluster
                    if node.clusterMembership == 'active':
                        nodes.append(node)
                    else:
                        log.info("Node {0} not part of cluster {1}".format(node.ip, node.clusterMembership))
        return nodes

    # this method returns the number of node in cluster
    def get_cluster_size(self):
        nodes = self.get_nodes()
        node_ip = []
        for node in nodes:
            node_ip.append(node.ip)
        log.info("Number of node(s) in cluster is {0} node(s)".format(len(node_ip)))
        return len(node_ip)

    """ this medthod return version on node that is not initialized yet """
    def get_nodes_version(self):
        node = self.get_nodes_self()
        version = node.version
        log.info("Node version in cluster {0}".format(version))
        return version

    # this method returns the versions of nodes in cluster
    def get_nodes_versions(self, logging=True):
        nodes = self.get_nodes()
        versions = []
        for node in nodes:
            versions.append(node.version)
        if logging:
            log.info("Node versions in cluster {0}".format(versions))
        return versions

    def check_cluster_compatibility(self, version):
        """
        Check if all nodes in cluster are of versions equal or above the version required.
        :param version: Version to check the cluster compatibility for. Should be of format major_ver.minor_ver.
                        For example: 5.0, 4.5, 5.1
        :return: True if cluster is compatible with the version specified, False otherwise. Return None if cluster is
        uninitialized.
        """
        nodes = self.get_nodes()
        if not nodes:
            # If nodes returned is None, it means that the cluster is not initialized yet and hence cluster
            # compatibility cannot be found. Return None
            return None
        major_ver, minor_ver = version.split(".")
        compatibility = int(major_ver) * 65536 + int(minor_ver)
        is_compatible = True
        for node in nodes:
            clusterCompatibility = int(node.clusterCompatibility)
            if clusterCompatibility < compatibility:
                is_compatible = False
        return is_compatible


    # this method returns the services of nodes in cluster - implemented for Sherlock
    def get_nodes_services(self):
        nodes = self.get_nodes()
        map = {}
        for node in nodes:
            key = "{0}:{1}".format(node.ip, node.port)
            map[key] = node.services
        return map

    # Check node version
    def check_node_versions(self, check_version="4.0"):
        versions = self.get_nodes_versions()
        if versions[0] < check_version:
            return False
        return True

    def get_bucket_stats(self, bucket='default'):
        stats = {}
        status, json_parsed = self.get_bucket_stats_json(bucket)
        if status:
            op = json_parsed["op"]
            samples = op["samples"]
            for stat_name in samples:
                if samples[stat_name]:
                    last_sample = len(samples[stat_name]) - 1
                    if last_sample:
                        stats[stat_name] = samples[stat_name][last_sample]
        return stats

    def get_fts_stats(self, index_name, bucket_name, stat_name):
        """
        List of fts stats available as of 03/16/2017 -
        default:default_idx3:avg_queries_latency: 0,
        default:default_idx3:batch_merge_count: 0,
        default:default_idx3:doc_count: 0,
        default:default_idx3:iterator_next_count: 0,
        default:default_idx3:iterator_seek_count: 0,
        default:default_idx3:num_bytes_live_data: 0,
        default:default_idx3:num_bytes_used_disk: 0,
        default:default_idx3:num_mutations_to_index: 0,
        default:default_idx3:num_pindexes: 0,
        default:default_idx3:num_pindexes_actual: 0,
        default:default_idx3:num_pindexes_target: 0,
        default:default_idx3:num_recs_to_persist: 0,
        default:default_idx3:reader_get_count: 0,
        default:default_idx3:reader_multi_get_count: 0,
        default:default_idx3:reader_prefix_iterator_count: 0,
        default:default_idx3:reader_range_iterator_count: 0,
        default:default_idx3:timer_batch_store_count: 0,
        default:default_idx3:timer_data_delete_count: 0,
        default:default_idx3:timer_data_update_count: 0,
        default:default_idx3:timer_opaque_get_count: 0,
        default:default_idx3:timer_opaque_set_count: 0,
        default:default_idx3:timer_rollback_count: 0,
        default:default_idx3:timer_snapshot_start_count: 0,
        default:default_idx3:total_bytes_indexed: 0,
        default:default_idx3:total_bytes_query_results: 0,
        default:default_idx3:total_compactions: 0,
        default:default_idx3:total_queries: 0,
        default:default_idx3:total_queries_error: 0,
        default:default_idx3:total_queries_slow: 0,
        default:default_idx3:total_queries_timeout: 0,
        default:default_idx3:total_request_time: 0,
        default:default_idx3:total_term_searchers: 0,
        default:default_idx3:writer_execute_batch_count: 0,
        :param index_name: name of the index
        :param bucket_name: source bucket
        :param stat_name: any of the above
        :return:
        """
        api = "{0}{1}".format(self.fts_baseUrl, 'api/nsstats')
        attempts = 0
        while attempts < 5:
            status, content, header = self._http_request(api)
            json_parsed = json.loads(content)
            key = bucket_name+':'+index_name+':'+stat_name
            if key in json_parsed:
                return status, json_parsed[key]
            attempts += 1
            log.info("Stat {0} not available yet".format(stat_name))
            time.sleep(1)
        log.error("ERROR: Stat {0} error on {1} on bucket {2}: {3}".
                  format(stat_name, index_name, bucket_name, e))

    def get_bucket_status(self, bucket):
        if not bucket:
            log.error("Bucket Name not Specified")
            return None
        api = self.baseUrl + 'pools/default/buckets'
        status, content, header = self._http_request(api)
        if status:
            json_parsed = json.loads(content)
            for item in json_parsed:
                if item["name"] == bucket:
                    return item["nodes"][0]["status"]
            log.error("Bucket {0} doesn't exist".format(bucket))
            return None

    def get_bucket_stats_json(self, bucket='default'):
        stats = {}
        api = "{0}{1}{2}{3}".format(self.baseUrl, 'pools/default/buckets/', bucket, "/stats")
        if isinstance(bucket, Bucket):
            api = '{0}{1}{2}{3}'.format(self.baseUrl, 'pools/default/buckets/', bucket.name, "/stats")
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        return status, json_parsed

    def get_bucket_json(self, bucket='default'):
        api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/', bucket)
        if isinstance(bucket, Bucket):
            api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/', bucket.name)
        status, content, header = self._http_request(api)
        if not status:
            raise GetBucketInfoFailed(bucket, content)
        return json.loads(content)

    def get_bucket_maxTTL(self, bucket='default'):
        bucket_info = self.get_bucket_json(bucket=bucket)
        return bucket_info['maxTTL']

    def get_bucket_compressionMode(self, bucket='default'):
        bucket_info = self.get_bucket_json(bucket=bucket)
        info = self.get_nodes_self()
        if 5.5 > float(info.version[:3]):
            bucket_info['compressionMode'] = "off"
        return bucket_info['compressionMode']

    def is_lww_enabled(self, bucket='default'):
        bucket_info = self.get_bucket_json(bucket=bucket)
        try:
            if bucket_info['conflictResolutionType'] == 'lww':
                return True
        except KeyError:
            return False

    def get_bucket(self, bucket='default', num_attempt=1, timeout=1):
        #log.info("-->get_bucket...") 
        bucketInfo = None
        try:
          bucket = bucket.decode()
        except AttributeError:
          pass
        api = '%s%s%s?basic_stats=true' % (self.baseUrl, 'pools/default/buckets/', bucket)
        if isinstance(bucket, Bucket):
            api = '%s%s%s?basic_stats=true' % (self.baseUrl, 'pools/default/buckets/', bucket.name)
        status, content, header = self._http_request(api)
        num = 1
        while not status and num_attempt > num:
            log.error("try to get {0} again after {1} sec".format(api, timeout))
            time.sleep(timeout)
            status, content, header = self._http_request(api)
            num += 1
        if status:
            bucketInfo = RestParser().parse_get_bucket_response(content)
        return bucketInfo

    def get_vbuckets(self, bucket='default'):
        b = self.get_bucket(bucket)
        return None if not b else b.vbuckets

    def delete_bucket(self, bucket='default'):
        api = '%s%s%s' % (self.baseUrl, 'pools/default/buckets/', bucket)
        if isinstance(bucket, Bucket):
            api = '%s%s%s' % (self.baseUrl, 'pools/default/buckets/', bucket.name)
        status, content, header = self._http_request(api, 'DELETE')

        if int(header['status']) == 500:
            # According to http://docs.couchbase.com/couchbase-manual-2.5/cb-rest-api/#deleting-buckets
            # the cluster will return with 500 if it failed to nuke
            # the bucket on all of the nodes within 30 secs
            log.warn("Bucket deletion timed out waiting for all nodes")

        return status

    def delete_all_buckets(self):
        buckets = self.get_buckets()
        for bucket in buckets:
            if isinstance(bucket, Bucket):
                api = '%s%s%s' % (self.baseUrl, 'pools/default/buckets/', bucket.name)
                self._http_request(api, 'DELETE')

    '''Load any of the three sample buckets'''
    def load_sample(self, sample_name):
        api = '{0}{1}'.format(self.baseUrl, "sampleBuckets/install")
        data = '["{0}"]'.format(sample_name)
        status, content, header = self._http_request(api, 'POST', data)
        # Sleep to allow the sample bucket to be loaded
        time.sleep(15)
        return status

    # figure out the proxy port
    def create_bucket(self, bucket='',
                      ramQuotaMB=1,
                      authType='none',
                      saslPassword='',
                      replicaNumber=1,
                      proxyPort=11211,
                      bucketType='membase',
                      replica_index=1,
                      threadsNumber=3,
                      flushEnabled=1,
                      evictionPolicy='valueOnly',
                      lww=False,
                      maxTTL=None,
                      compressionMode='passive'):


        api = '{0}{1}'.format(self.baseUrl, 'pools/default/buckets')
        params = urllib.parse.urlencode({})



        # this only works for default bucket ?
        if bucket == 'default':
            init_params = {'name': bucket,
                           'authType': 'sasl',
                           'saslPassword': saslPassword,
                           'ramQuotaMB': ramQuotaMB,
                           'replicaNumber': replicaNumber,
                           #'proxyPort': proxyPort,
                           'bucketType': bucketType,
                           'replicaIndex': replica_index,
                           'threadsNumber': threadsNumber,
                           'flushEnabled': flushEnabled,
                           'evictionPolicy': evictionPolicy}

        elif authType == 'none':
            init_params = {'name': bucket,
                           'ramQuotaMB': ramQuotaMB,
                           'authType': authType,
                           'replicaNumber': replicaNumber,
                           #'proxyPort': proxyPort,
                           'bucketType': bucketType,
                           'replicaIndex': replica_index,
                           'threadsNumber': threadsNumber,
                           'flushEnabled': flushEnabled,
                           'evictionPolicy': evictionPolicy}
        elif authType == 'sasl':
            init_params = {'name': bucket,
                           'ramQuotaMB': ramQuotaMB,
                           'authType': authType,
                           'saslPassword': saslPassword,
                           'replicaNumber': replicaNumber,
                           #'proxyPort': self.get_nodes_self().moxi,
                           'bucketType': bucketType,
                           'replicaIndex': replica_index,
                           'threadsNumber': threadsNumber,
                           'flushEnabled': flushEnabled,
                           'evictionPolicy': evictionPolicy}
        if lww:
            init_params['conflictResolutionType'] = 'lww'

        if maxTTL:
            init_params['maxTTL'] = maxTTL

        if compressionMode and self.is_enterprise_edition():
            init_params['compressionMode'] = compressionMode

        if bucketType == 'ephemeral':
            del init_params['replicaIndex']     # does not apply to ephemeral buckets, and is even rejected

        pre_spock = not self.check_cluster_compatibility("5.0")
        if pre_spock:
            init_params['proxyPort'] = proxyPort

        params = urllib.parse.urlencode(init_params)

        log.info("{0} with param: {1}".format(api, params))
        create_start_time = time.time()

        maxwait = 60
        for numsleep in range(maxwait):
            status, content, header = self._http_request(api, 'POST', params)
            if status:
                break
            elif (int(header['status']) == 503 and
                    '{"_":"Bucket with given name still exists"}' in content):
                log.info("The bucket still exists, sleep 1 sec and retry")
                time.sleep(1)
            else:
                raise BucketCreationException(ip=self.ip, bucket_name=bucket)

        if (numsleep + 1) == maxwait:
            log.error("Tried to create the bucket for {0} secs.. giving up".
                      format(maxwait))
            raise BucketCreationException(ip=self.ip, bucket_name=bucket)




        create_time = time.time() - create_start_time
        log.info("{0:.02f} seconds to create bucket {1}".
                 format(round(create_time, 2), bucket))
        return status

    def change_bucket_props(self, bucket,
                      ramQuotaMB=None,
                      authType=None,
                      saslPassword=None,
                      replicaNumber=None,
                      proxyPort=None,
                      replicaIndex=None,
                      flushEnabled=None,
                      timeSynchronization=None,
                      maxTTL=None,
                      compressionMode=None):
        api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/', bucket)
        if isinstance(bucket, Bucket):
            api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/', bucket.name)
        params = urllib.parse.urlencode({})
        params_dict = {}
        existing_bucket = self.get_bucket_json(bucket)
        if ramQuotaMB:
            params_dict["ramQuotaMB"] = ramQuotaMB
        if authType:
            params_dict["authType"] = authType
        if saslPassword:
            params_dict["authType"] = "sasl"
            params_dict["saslPassword"] = saslPassword
        if replicaNumber:
            params_dict["replicaNumber"] = replicaNumber
        #if proxyPort:
        #    params_dict["proxyPort"] = proxyPort
        if replicaIndex:
            params_dict["replicaIndex"] = replicaIndex
        if flushEnabled:
            params_dict["flushEnabled"] = flushEnabled
        if timeSynchronization:
            params_dict["timeSynchronization"] = timeSynchronization
        if maxTTL:
            params_dict["maxTTL"] = maxTTL
        if compressionMode and self.is_enterprise_edition():
            params_dict["compressionMode"] = compressionMode

        params = urllib.parse.urlencode(params_dict)

        log.info("%s with param: %s" % (api, params))
        status, content, header = self._http_request(api, 'POST', params)
        if timeSynchronization:
            if status:
                raise Exception("Erroneously able to set bucket settings %s for bucket on time-sync" % (params, bucket))
            return status, content
        if not status:
            raise Exception("Unable to set bucket settings %s for bucket" % (params, bucket))
        log.info("bucket %s updated" % bucket)
        return status

    # return AutoFailoverSettings
    def get_autofailover_settings(self):
        settings = None
        api = self.baseUrl + 'settings/autoFailover'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            settings = AutoFailoverSettings()
            settings.enabled = json_parsed["enabled"]
            settings.count = json_parsed["count"]
            settings.timeout = json_parsed["timeout"]
            settings.failoverOnDataDiskIssuesEnabled = json_parsed["failoverOnDataDiskIssues"]["enabled"]
            settings.failoverOnDataDiskIssuesTimeout = json_parsed["failoverOnDataDiskIssues"]["timePeriod"]
            settings.maxCount = json_parsed["maxCount"]
            settings.failoverServerGroup = json_parsed["failoverServerGroup"]
            if json_parsed["canAbortRebalance"]:
                settings.can_abort_rebalance = json_parsed["canAbortRebalance"]
        return settings

    def update_autofailover_settings(self, enabled, timeout, canAbortRebalance=False, enable_disk_failure=False,
                                     disk_timeout=120, maxCount=1, enableServerGroup=False):
        params_dict = {}
        params_dict['timeout'] = timeout
        if enabled:
            params_dict['enabled'] = 'true'

        else:
            params_dict['enabled'] = 'false'
        if canAbortRebalance:
            params_dict['canAbortRebalance'] = 'true'
        if enable_disk_failure:
            params_dict['failoverOnDataDiskIssues[enabled]'] = 'true'
            params_dict['failoverOnDataDiskIssues[timePeriod]'] = disk_timeout
        else:
            params_dict['failoverOnDataDiskIssues[enabled]'] = 'false'
        params_dict['maxCount'] = maxCount
        if enableServerGroup:
            params_dict['failoverServerGroup'] = 'true'
        else:
            params_dict['failoverServerGroup'] = 'false'
        params = urllib.parse.urlencode(params_dict)
        api = self.baseUrl + 'settings/autoFailover'
        log.info('settings/autoFailover params : {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        if not status:
            log.error('''failed to change autofailover_settings!
                         See MB-7282. Workaround:
                         wget --user=Administrator --password=asdasd --post-data='rpc:call(mb_master:master_node(), erlang, apply ,[fun () -> erlang:exit(erlang:whereis(mb_master), kill) end, []]).' http://localhost:8091/diag/eval''')
        return status

    # return AutoReprovisionSettings
    def get_autoreprovision_settings(self):
        settings = None
        api = self.baseUrl + 'settings/autoReprovision'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            settings = AutoReprovisionSettings()
            settings.enabled = json_parsed["enabled"]
            settings.count = json_parsed["count"]
            settings.max_nodes = json_parsed["max_nodes"]
        return settings

    def update_autoreprovision_settings(self, enabled, maxNodes=1):
        if enabled:
            params = urllib.parse.urlencode({'enabled': 'true',
                                       'maxNodes': maxNodes})
        else:
            params = urllib.parse.urlencode({'enabled': 'false',
                                       'maxNodes': maxNodes})
        api = self.baseUrl + 'settings/autoReprovision'
        log.info('settings/autoReprovision params : {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        if not status:
            log.error('failed to change autoReprovision_settings!')
        return status

    def reset_autofailover(self):
        api = self.baseUrl + 'settings/autoFailover/resetCount'
        status, content, header = self._http_request(api, 'POST', '')
        return status

    def reset_autoreprovision(self):
        api = self.baseUrl + 'settings/autoReprovision/resetCount'
        status, content, header = self._http_request(api, 'POST', '')
        return status

    def set_alerts_settings(self, recipients, sender, email_username, email_password, email_host='localhost', email_port=25, email_encrypt='false', alerts='auto_failover_node,auto_failover_maximum_reached'):
        api = self.baseUrl + 'settings/alerts'
        params = urllib.parse.urlencode({'enabled': 'true',
                                   'recipients': recipients,
                                   'sender': sender,
                                   'emailUser': email_username,
                                   'emailPass': email_password,
                                   'emailHost': email_host,
                                   'emailPort': email_port,
                                   'emailEncrypt': email_encrypt,
                                   'alerts': alerts})
        log.info('settings/alerts params : {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def get_alerts_settings(self):
        api = self.baseUrl + 'settings/alerts'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if not status:
            raise Exception("unable to get autofailover alerts settings")
        return json_parsed

    def disable_alerts(self):
        api = self.baseUrl + 'settings/alerts'
        params = urllib.parse.urlencode({'enabled': 'false'})
        log.info('settings/alerts params : {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def set_cas_drift_threshold(self, bucket, ahead_threshold_in_millisecond, behind_threshold_in_millisecond):

        api = self.baseUrl + 'pools/default/buckets/{0}'. format( bucket )
        params_dict ={'driftAheadThresholdMs': ahead_threshold_in_millisecond,
                      'driftBehindThresholdMs': behind_threshold_in_millisecond}
        params = urllib.parse.urlencode(params_dict)
        log.info("%s with param: %s" % (api, params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def stop_rebalance(self, wait_timeout=10):
        api = self.baseUrl + '/controller/stopRebalance'
        status, content, header = self._http_request(api, 'POST')
        if status:
            for i in range(wait_timeout):
                if self._rebalance_progress_status() == 'running':
                    log.warn("rebalance is not stopped yet after {0} sec".format(i + 1))
                    time.sleep(1)
                    status = False
                else:
                    log.info("rebalance was stopped")
                    status = True
                    break
        else:
            log.error("Rebalance is not stopped due to {0}".format(content))
        return status

    def set_data_path(self, data_path=None, index_path=None):
        api = self.baseUrl + '/nodes/self/controller/settings'
        paths = {}
        if data_path:
            paths['path'] = data_path
        if index_path:
            paths['index_path'] = index_path
        if paths:
            params = urllib.parse.urlencode(paths)
            log.info('/nodes/self/controller/settings params : {0}'.format(params))
            status, content, header = self._http_request(api, 'POST', params)
            if status:
                log.info("Setting data_path: {0}: status {1}".format(data_path, status))
            else:
                log.error("Unable to set data_path {0} : {1}".format(data_path, content))
            return status

    def get_database_disk_size(self, bucket='default'):
        api = self.baseUrl + "pools/{0}/buckets".format(bucket)
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        # disk_size in MB
        disk_size = (json_parsed[0]["basicStats"]["diskUsed"]) // (1024 * 1024)
        return status, disk_size

    def ddoc_compaction(self, design_doc_id, bucket="default"):
        api = self.baseUrl + "pools/default/buckets/%s/ddocs/%s/controller/compactView" % \
            (bucket, design_doc_id)
        status, content, header = self._http_request(api, 'POST')
        if not status:
            raise CompactViewFailed(design_doc_id, content)
        log.info("compaction for ddoc '%s' was triggered" % design_doc_id)

    def check_compaction_status(self, bucket_name):
        tasks = self.active_tasks()
        if "error" in tasks:
            raise Exception(tasks)
        for task in tasks:
            log.info("Task is {0}".format(task))
            if task["type"] == "bucket_compaction":
                if task["bucket"] == bucket_name:
                    return True, task["progress"]
        return False, None

    def change_memcached_t_option(self, value):
        cmd = '[ns_config:update_key({node, N, memcached}, fun (PList)' + \
              ' -> lists:keystore(verbosity, 1, PList, {verbosity, \'-t ' + str(value) + '\'}) end)' + \
              ' || N <- ns_node_disco:nodes_wanted()].'
        return self.diag_eval(cmd)

    def set_ensure_full_commit(self, value):
        """Dynamic settings changes"""
        # the boolean paramter is used to turn on/off ensure_full_commit(). In XDCR,
        # issuing checkpoint in this function is expensive and not necessary in some
        # test, turning off this function would speed up some test. The default value
        # is ON.
        cmd = 'ns_config:set(ensure_full_commit_enabled, {0}).'.format(value)
        return self.diag_eval(cmd)

    def get_internalSettings(self, param):
            """allows to get internalSettings values for:
            indexAwareRebalanceDisabled, rebalanceIndexWaitingDisabled,
            rebalanceIndexPausingDisabled, maxParallelIndexers,
            maxParallelReplicaIndexers, maxBucketCount"""
            api = self.baseUrl + "internalSettings"
            status, content, header = self._http_request(api)
            json_parsed = json.loads(content)
            param = json_parsed[param]
            return param

    def set_internalSetting(self, param, value):
        "Set any internal setting"
        api = self.baseUrl + "internalSettings"

        if isinstance(value, bool):
            value = str(value).lower()

        params = urllib.parse.urlencode({param : value})
        status, content, header = self._http_request(api, "POST", params)
        log.info('Update internal setting {0}={1}'.format(param, value))
        return status

    def get_replication_for_buckets(self, src_bucket_name, dest_bucket_name):
        replications = self.get_replications()
        for replication in replications:
            if src_bucket_name in replication['source'] and \
                replication['target'].endswith(dest_bucket_name):
                return replication
        raise XDCRException("Replication with Src bucket: {0} and Target bucket: {1} not found".
                        format(src_bucket_name, dest_bucket_name))

    """ By default, these are the global replication settings -
        { optimisticReplicationThreshold:256,
        workerBatchSize:500,
        failureRestartInterval:1,
        docBatchSizeKb":2048,
        checkpointInterval":1800,
        maxConcurrentReps":32}
        You can override these using set_xdcr_param()
    """
    def set_xdcr_param(self, src_bucket_name,
                                         dest_bucket_name, param, value):
        replication = self.get_replication_for_buckets(src_bucket_name, dest_bucket_name)
        api = self.baseUrl[:-1] + replication['settingsURI']
        value = str(value).lower()
        params = urllib.parse.urlencode({param: value})
        status, _, _ = self._http_request(api, "POST", params)
        if not status:
            raise XDCRException("Unable to set replication setting {0}={1} on bucket {2} on node {3}".
                            format(param, value, src_bucket_name, self.ip))
        log.info("Updated {0}={1} on bucket'{2}' on {3}".format(param, value, src_bucket_name, self.ip))

    def set_global_xdcr_param(self, param, value):
        api = self.baseUrl[:-1] + "/settings/replications"
        value = str(value).lower()
        params = urllib.parse.urlencode({param: value})
        status, _, _ = self._http_request(api, "POST", params)
        if not status:
            raise XDCRException("Unable to set replication setting {0}={1} on node {2}".
                            format(param, value, self.ip))
        log.info("Updated {0}={1} on {2}".format(param, value, self.ip))

    # Gets per-replication setting value
    def get_xdcr_param(self, src_bucket_name,
                                    dest_bucket_name, param):
        replication = self.get_replication_for_buckets(src_bucket_name, dest_bucket_name)
        api = self.baseUrl[:-1] + replication['settingsURI']
        status, content, _ = self._http_request(api)
        if not status:
            raise XDCRException("Unable to get replication setting {0} on bucket {1} on node {2}".
                      format(param, src_bucket_name, self.ip))
        json_parsed = json.loads(content)
        # when per-replication settings match global(internal) settings,
        # the param is not returned by rest API
        # in such cases, return internalSetting value for the param
        try:
            return json_parsed[param]
        except KeyError:
            if param == 'pauseRequested':
                return False
            else:
                param = 'xdcr' + param[0].upper() + param[1:]
                log.info("Trying to fetch xdcr param:{0} from global settings".
                         format(param))
                return self.get_internalSettings(param)

    # Returns a boolean value on whether replication
    def is_replication_paused(self, src_bucket_name, dest_bucket_name):
        return self.get_xdcr_param(src_bucket_name, dest_bucket_name, 'pauseRequested')

    def is_replication_paused_by_id(self, repl_id):
        repl_id = repl_id.replace('/', '%2F')
        api = self.baseUrl + 'settings/replications/' + repl_id
        status, content, header = self._http_request(api)
        if not status:
            raise XDCRException("Unable to retrieve pause resume status for replication {0}".
                                format(repl_id))
        repl_stats = json.loads(content)
        return repl_stats['pauseRequested']

    def pause_resume_repl_by_id(self, repl_id, param, value):
        repl_id = repl_id.replace('/', '%2F')
        api = self.baseUrl + 'settings/replications/' + repl_id
        params = urllib.parse.urlencode({param: value})
        status, _, _ = self._http_request(api, "POST", params)
        if not status:
            raise XDCRException("Unable to update {0}={1} setting for replication {2}".
                            format(param, value, repl_id))
        log.info("Updated {0}={1} on {2}".format(param, value, repl_id))

    def get_recent_xdcr_vb_ckpt(self, repl_id):
        command = 'ns_server_testrunner_api:grab_all_goxdcr_checkpoints().'
        status, content = self.diag_eval(command)
        if not status:
            raise Exception("Unable to get recent XDCR checkpoint information")
        repl_ckpt_list = json.loads(content)
        # a single decoding will only return checkpoint record as string
        # convert string to dict using json
        chkpt_doc_string = repl_ckpt_list['/ckpt/%s/0' % repl_id].replace('"', '\"')
        chkpt_dict = json.loads(chkpt_doc_string)
        return chkpt_dict['checkpoints'][0]

    """ Start of FTS rest apis"""

    def set_fts_ram_quota(self, value):
        """set fts ram quota"""
        api = self.baseUrl + "pools/default"
        params = urllib.parse.urlencode({"ftsMemoryQuota": value})
        status, content, _ = self._http_request(api, "POST", params)
        if status:
            log.info("SUCCESS: FTS RAM quota set to {0}mb".format(value))
        else:
            raise Exception("Error setting fts ram quota: {0}".format(content))
        return status


    def create_fts_index(self, index_name, params):
        """create or edit fts index , returns {"status":"ok"} on success"""
        api = self.fts_baseUrl + "api/index/{0}".format(index_name)
        log.info(json.dumps(params))
        status, content, header = self._http_request(api,
                                    'PUT',
                                    json.dumps(params, ensure_ascii=False),
                                    headers=self._create_capi_headers(),
                                    timeout=30)
        if status:
            log.info("Index {0} created".format(index_name))
        else:
            raise Exception("Error creating index: {0}".format(content))
        return status

    def update_fts_index(self, index_name, index_def):
        api = self.fts_baseUrl + "api/index/{0}".format(index_name)
        log.info(json.dumps(index_def, indent=3))
        status, content, header = self._http_request(api,
                                    'PUT',
                                    json.dumps(index_def, ensure_ascii=False),
                                    headers=self._create_capi_headers(),
                                    timeout=30)
        if status:
            log.info("Index/alias {0} updated".format(index_name))
        else:
            raise Exception("Error updating index: {0}".format(content))
        return status

    def get_fts_index_definition(self, name, timeout=30):
        """ get fts index/alias definition """
        json_parsed = {}
        api = self.fts_baseUrl + "api/index/{0}".format(name)
        status, content, header = self._http_request(
            api,
            headers=self._create_capi_headers(),
            timeout=timeout)
        if status:
            json_parsed = json.loads(content)
        return status, json_parsed

    def get_fts_index_doc_count(self, name, timeout=30):
        """ get number of docs indexed"""
        json_parsed = {}
        api = self.fts_baseUrl + "api/index/{0}/count".format(name)
        status, content, header = self._http_request(
            api,
            headers=self._create_capi_headers(),
            timeout=timeout)
        if status:
            json_parsed = json.loads(content)
        return json_parsed['count']

    def get_fts_index_uuid(self, name, timeout=30):
        """ Returns uuid of index/alias """
        json_parsed = {}
        api = self.fts_baseUrl + "api/index/{0}/".format(name)
        status, content, header = self._http_request(
            api,
            headers=self._create_capi_headers(),
            timeout=timeout)
        if status:
            json_parsed = json.loads(content)
        return json_parsed['indexDef']['uuid']

    def delete_fts_index(self, name):
        """ delete fts index/alias """
        api = self.fts_baseUrl + "api/index/{0}".format(name)
        status, content, header = self._http_request(
            api,
            'DELETE',
            headers=self._create_capi_headers())
        return status

    def stop_fts_index_update(self, name):
        """ method to stop fts index from updating"""
        api = self.fts_baseUrl + "api/index/{0}/ingestControl/pause".format(name)
        status, content, header = self._http_request(
            api,
            'POST',
            '',
            headers=self._create_capi_headers())
        return status

    def freeze_fts_index_partitions(self, name):
        """ method to freeze index partitions asignment"""
        api = self.fts_baseUrl+ "api/index/{0}/planFreezeControl".format(name)
        status, content, header = self._http_request(
            api,
            'POST',
            '',
            headers=self._create_capi_headers())
        return status

    def disable_querying_on_fts_index(self, name):
        """ method to disable querying on index"""
        api = self.fts_baseUrl + "api/index/{0}/queryControl/disallow".format(name)
        status, content, header = self._http_request(
            api,
            'POST',
            '',
            headers=self._create_capi_headers())
        return status

    def enable_querying_on_fts_index(self, name):
        """ method to enable querying on index"""
        api = self.fts_baseUrl + "api/index/{0}/queryControl/allow".format(name)
        status, content, header = self._http_request(
            api,
            'POST',
            '',
            headers=self._create_capi_headers())
        return status

    def run_fts_query(self, index_name, query_json, timeout=70):
        """Method run an FTS query through rest api"""
        api = self.fts_baseUrl + "api/index/{0}/query".format(index_name)
        headers = self._create_capi_headers()
        status, content, header = self._http_request(
            api,
            "POST",
            json.dumps(query_json, ensure_ascii=False).encode('utf8'),
            headers,
            timeout=timeout)

        if status:
            content = json.loads(content)
            return content['total_hits'], content['hits'], content['took'], \
                   content['status']

    def run_fts_query_with_facets(self, index_name, query_json):
        """Method run an FTS query through rest api"""
        api = self.fts_baseUrl + "api/index/{0}/query".format(index_name)
        headers = self._create_capi_headers()
        status, content, header = self._http_request(
            api,
            "POST",
            json.dumps(query_json, ensure_ascii=False).encode('utf8'),
            headers,
            timeout=70)

        if status:
            content = json.loads(content)
            return content['total_hits'], content['hits'], content['took'], \
                   content['status'], content['facets']


    """ End of FTS rest APIs """

    def set_reb_cons_view(self, disable):
        """Enable/disable consistent view for rebalance tasks"""
        api = self.baseUrl + "internalSettings"
        params = {"indexAwareRebalanceDisabled": str(disable).lower()}
        params = urllib.parse.urlencode(params)
        status, content, header = self._http_request(api, "POST", params)
        log.info('Consistent-views during rebalance was set as indexAwareRebalanceDisabled={0}'\
                 .format(str(disable).lower()))
        return status

    def set_reb_index_waiting(self, disable):
        """Enable/disable rebalance index waiting"""
        api = self.baseUrl + "internalSettings"
        params = {"rebalanceIndexWaitingDisabled": str(disable).lower()}
        params = urllib.parse.urlencode(params)
        status, content, header = self._http_request(api, "POST", params)
        log.info('rebalance index waiting was set as rebalanceIndexWaitingDisabled={0}'\
                 .format(str(disable).lower()))
        return status

    def set_rebalance_index_pausing(self, disable):
        """Enable/disable index pausing during rebalance"""
        api = self.baseUrl + "internalSettings"
        params = {"rebalanceIndexPausingDisabled": str(disable).lower()}
        params = urllib.parse.urlencode(params)
        status, content, header = self._http_request(api, "POST", params)
        log.info('index pausing during rebalance was set as rebalanceIndexPausingDisabled={0}'\
                 .format(str(disable).lower()))
        return status

    def set_max_parallel_indexers(self, count):
        """set max parallel indexer threads"""
        api = self.baseUrl + "internalSettings"
        params = {"maxParallelIndexers": count}
        params = urllib.parse.urlencode(params)
        status, content, header = self._http_request(api, "POST", params)
        log.info('max parallel indexer threads was set as maxParallelIndexers={0}'.\
                 format(count))
        return status

    def set_max_parallel_replica_indexers(self, count):
        """set max parallel replica indexers threads"""
        api = self.baseUrl + "internalSettings"
        params = {"maxParallelReplicaIndexers": count}
        params = urllib.parse.urlencode(params)
        status, content, header = self._http_request(api, "POST", params)
        log.info('max parallel replica indexers threads was set as maxParallelReplicaIndexers={0}'.\
                 format(count))
        return status

    def get_internal_replication_type(self):
        buckets = self.get_buckets()
        cmd = "\'{ok, BC} = ns_bucket:get_bucket(%s), ns_bucket:replication_type(BC).\'" % buckets[0].name
        return self.diag_eval(cmd)

    def set_mc_threads(self, mc_threads=4):
        """
        Change number of memcached threads and restart the cluster
        """
        cmd = "[ns_config:update_key({node, N, memcached}, " \
              "fun (PList) -> lists:keystore(verbosity, 1, PList," \
              " {verbosity, \"-t %s\"}) end) " \
              "|| N <- ns_node_disco:nodes_wanted()]." % mc_threads

        return self.diag_eval(cmd)

    def get_auto_compaction_settings(self):
        api = self.baseUrl + "settings/autoCompaction"
        status, content, header = self._http_request(api)
        return json.loads(content)

    def set_auto_compaction(self, parallelDBAndVC="false",
                            dbFragmentThreshold=None,
                            viewFragmntThreshold=None,
                            dbFragmentThresholdPercentage=None,
                            viewFragmntThresholdPercentage=None,
                            allowedTimePeriodFromHour=None,
                            allowedTimePeriodFromMin=None,
                            allowedTimePeriodToHour=None,
                            allowedTimePeriodToMin=None,
                            allowedTimePeriodAbort=None,
                            bucket=None):
        """Reset compaction values to default, try with old fields (dp4 build)
        and then try with newer fields"""
        params = {}
        api = self.baseUrl

        if bucket is None:
            # setting is cluster wide
            api = api + "controller/setAutoCompaction"
        else:
            # overriding per/bucket compaction setting
            api = api + "pools/default/buckets/" + bucket
            params["autoCompactionDefined"] = "true"
            # reuse current ram quota in mb per node
            num_nodes = len(self.node_statuses())
            bucket_info = self.get_bucket_json(bucket)
            quota = self.get_bucket_json(bucket)["quota"]["ram"] // (1048576 * num_nodes)
            params["ramQuotaMB"] = quota
            if bucket_info["authType"] == "sasl" and bucket_info["name"] != "default":
                params["authType"] = self.get_bucket_json(bucket)["authType"]
                params["saslPassword"] = self.get_bucket_json(bucket)["saslPassword"]

        params["parallelDBAndViewCompaction"] = parallelDBAndVC
        # Need to verify None because the value could be = 0
        if dbFragmentThreshold is not None:
            params["databaseFragmentationThreshold[size]"] = dbFragmentThreshold
        if viewFragmntThreshold is not None:
            params["viewFragmentationThreshold[size]"] = viewFragmntThreshold
        if dbFragmentThresholdPercentage is not None:
            params["databaseFragmentationThreshold[percentage]"] = dbFragmentThresholdPercentage
        if viewFragmntThresholdPercentage is not None:
            params["viewFragmentationThreshold[percentage]"] = viewFragmntThresholdPercentage
        if allowedTimePeriodFromHour is not None:
            params["allowedTimePeriod[fromHour]"] = allowedTimePeriodFromHour
        if allowedTimePeriodFromMin is not None:
            params["allowedTimePeriod[fromMinute]"] = allowedTimePeriodFromMin
        if allowedTimePeriodToHour is not None:
            params["allowedTimePeriod[toHour]"] = allowedTimePeriodToHour
        if allowedTimePeriodToMin is not None:
            params["allowedTimePeriod[toMinute]"] = allowedTimePeriodToMin
        if allowedTimePeriodAbort is not None:
            params["allowedTimePeriod[abortOutside]"] = allowedTimePeriodAbort

        params = urllib.parse.urlencode(params)
        log.info("'%s' bucket's settings will be changed with parameters: %s" % (bucket, params))
        return self._http_request(api, "POST", params)

    def disable_auto_compaction(self):
        """
           Cluster-wide Setting
              Disable autocompaction on doc and view
        """
        api = self.baseUrl + "controller/setAutoCompaction"
        log.info("Disable autocompaction in cluster-wide setting")
        status, content, header = self._http_request(api, "POST",
                                  "parallelDBAndViewCompaction=false")
        return status

    def set_purge_interval_and_parallel_compaction(self, interval=3, parallel="false"):
        """
           Cluster-wide setting.
           Set purge interval
           Set parallel db and view compaction
           Return: status
        """
        api = self.baseUrl + "controller/setAutoCompaction"
        log.info("Set purgeInterval to %s and parallel DB and view compaction to %s"\
                                                              % (interval, parallel))
        params = {}
        params["purgeInterval"] = interval
        params["parallelDBAndViewCompaction"] = parallel
        params = urllib.parse.urlencode(params)
        status, content, header = self._http_request(api, "POST", params)
        return status, content

    def set_indexer_compaction(self, mode="circular", indexDayOfWeek=None, indexFromHour=0,
                                indexFromMinute=0, abortOutside=False,
                                indexToHour=0, indexToMinute=0, fragmentation=30):
        """Reset compaction values to default, try with old fields (dp4 build)
        and then try with newer fields"""
        params = {}
        api = self.baseUrl + "controller/setAutoCompaction"
        params["indexCompactionMode"] = mode
        params["indexCircularCompaction[interval][fromHour]"] = indexFromHour
        params["indexCircularCompaction[interval][fromMinute]"] = indexFromMinute
        params["indexCircularCompaction[interval][toHour]"] = indexToHour
        params["indexCircularCompaction[interval][toMinute]"] = indexToMinute
        if indexDayOfWeek:
            params["indexCircularCompaction[daysOfWeek]"] = indexDayOfWeek
        params["indexCircularCompaction[interval][abortOutside]"] = str(abortOutside).lower()
        params["parallelDBAndViewCompaction"] = "false"
        if mode == "full":
            params["indexFragmentationThreshold[percentage]"] = fragmentation
        log.info("Indexer Compaction Settings: %s" % (params))
        params = urllib.parse.urlencode(params)
        return self._http_request(api, "POST", params)

    def set_global_loglevel(self, loglevel='error'):
        """Set cluster-wide logging level for core components

        Possible loglevel:
            -- debug
            -- info
            -- warn
            -- error
        """

        api = self.baseUrl + 'diag/eval'
        request_body = 'rpc:eval_everywhere(erlang, apply, [fun () -> \
                        [ale:set_loglevel(L, {0}) || L <- \
                        [ns_server, couchdb, user, menelaus, ns_doctor, stats, \
                        rebalance, cluster, views, stderr]] end, []]).'.format(loglevel)
        return self._http_request(api=api, method='POST', params=request_body,
                                  headers=self._create_headers())

    def set_indexer_params(self, parameter, val):
        """
        :Possible  parameters:
            -- indexerThreads
            -- memorySnapshotInterval
            -- stableSnapshotInterval
            -- maxRollbackPoints
            -- logLevel
        """
        params = {}
        api = self.baseUrl + 'settings/indexes'
        params[parameter] = val
        params = urllib.parse.urlencode(params)
        status, content, header = self._http_request(api, "POST", params)
        log.info('Indexer {0} set to {1}'.format(parameter, val))
        return status

    def get_global_index_settings(self):
        api = self.baseUrl + "settings/indexes"
        status, content, header = self._http_request(api)
        if status:
            return json.loads(content)
        return None

    def set_couchdb_option(self, section, option, value):
        """Dynamic settings changes"""

        cmd = 'ns_config:set({{couchdb, {{{0}, {1}}}}}, {2}).'.format(section,
                                                                      option,
                                                                      value)
        return self.diag_eval(cmd)

    def get_alerts(self):
        api = self.baseUrl + "pools/default/"
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            if "alerts" in json_parsed:
                return json_parsed['alerts']
        else:
            return None

    def get_nodes_data_from_cluster(self, param="nodes"):
        api = self.baseUrl + "pools/default/"
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            if param in json_parsed:
                return json_parsed[param]
        else:
            return None

    def flush_bucket(self, bucket="default"):
        if isinstance(bucket, Bucket):
            bucket_name = bucket.name
        else:
            bucket_name = bucket
        api = self.baseUrl + "pools/default/buckets/%s/controller/doFlush" % (bucket_name)
        status, content, header = self._http_request(api, 'POST')
        if not status:
            raise BucketFlushFailed(self.ip, bucket_name)
        log.info("Flush for bucket '%s' was triggered" % bucket_name)

    def update_notifications(self, enable):
        api = self.baseUrl + 'settings/stats'
        params = urllib.parse.urlencode({'sendStats' : enable})
        log.info('settings/stats params : {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    def get_notifications(self):
        api = self.baseUrl + 'settings/stats'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            return json_parsed["sendStats"]
        return None

    def get_logs(self, last_n=10, contains_text=None):
        api = self.baseUrl + 'logs'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        logs = json_parsed['list']
        logs.reverse()
        result = []
        for i in range(min(last_n, len(logs))):
            result.append(logs[i])
            if contains_text is not None and contains_text in logs[i]["text"]:
                break
        return result

    def print_UI_logs(self, last_n=10, contains_text=None):
        logs = self.get_logs(last_n, contains_text)
        log.info("Latest logs from UI on {0}:".format(self.ip))
        for lg in logs: log.error(lg)

    def get_ro_user(self):
        api = self.baseUrl + 'settings/readOnlyAdminName'
        status, content, header = self._http_request(api, 'GET', '')
        return content, status

    def delete_ro_user(self):
        api = self.baseUrl + 'settings/readOnlyUser'
        status, content, header = self._http_request(api, 'DELETE', '')
        return status

    def create_ro_user(self, username, password):
        api = self.baseUrl + 'settings/readOnlyUser'
        params = urllib.parse.urlencode({'username' : username, 'password' : password})
        log.info('settings/readOnlyUser params : {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    # Change password for readonly user
    def changePass_ro_user(self, username, password):
        api = self.baseUrl + 'settings/readOnlyUser'
        params = urllib.parse.urlencode({'username' : username, 'password' : password})
        log.info('settings/readOnlyUser params : {0}'.format(params))
        status, content, header = self._http_request(api, 'PUT', params)
        return status

    '''Start Monitoring/Profiling Rest Calls'''
    def set_completed_requests_collection_duration(self, server, min_time):
        http = httplib2.Http()
        n1ql_port = 8093
        api = "http://%s:%s/" % (server.ip, n1ql_port) + "admin/settings"
        body = {"completed-threshold": min_time}
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "POST", headers=headers, body=json.dumps(body))
        return response, content

    def set_completed_requests_max_entries(self, server, no_entries):
        http = httplib2.Http()
        n1ql_port = 8093
        api = "http://%s:%s/" % (server.ip, n1ql_port) + "admin/settings"
        body = {"completed-limit": no_entries}
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "POST", headers=headers, body=json.dumps(body))
        return response, content

    def set_profiling(self, server, setting):
        http = httplib2.Http()
        n1ql_port = 8093
        api = "http://%s:%s/" % (server.ip, n1ql_port) + "admin/settings"
        body = {"profile": setting}
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "POST", headers=headers, body=json.dumps(body))
        return response, content

    def set_profiling_controls(self, server, setting):
        http = httplib2.Http()
        n1ql_port = 8093
        api = "http://%s:%s/" % (server.ip, n1ql_port) + "admin/settings"
        body = {"controls": setting}
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "POST", headers=headers, body=json.dumps(body))
        return response, content

    def get_query_admin_settings(self, server):
        http = httplib2.Http()
        n1ql_port = 8093
        api = "http://%s:%s/" % (server.ip, n1ql_port) + "admin/settings"
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "GET", headers=headers)
        result = json.loads(content)
        return result

    def get_query_vitals(self, server):
        http = httplib2.Http()
        n1ql_port = 8093
        api = "http://%s:%s/" % (server.ip, n1ql_port) + "admin/vitals"
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "GET", headers=headers)
        return response, content
    '''End Monitoring/Profiling Rest Calls'''

    def create_whitelist(self, server, whitelist):
        http = httplib2.Http()
        api = "http://%s:%s/" % (server.ip, server.port) + "settings/querySettings/curlWhitelist"
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "POST", headers=headers, body=json.dumps(whitelist))
        return response, content

    def query_tool(self, query, port=8093, timeout=1300, query_params={}, is_prepared=False, named_prepare=None,
                   verbose = True, encoded_plan=None, servers=None):
        key = 'prepared' if is_prepared else 'statement'
        headers = None
        content=""
        prepared = json.dumps(query)
        if is_prepared:
            if named_prepare and encoded_plan:
                http = httplib2.Http()
                if len(servers)>1:
                    url = "http://%s:%s/query/service" % (servers[1].ip, port)
                else:
                    url = "http://%s:%s/query/service" % (self.ip, port)

                headers = self._create_headers_encoded_prepared()
                body = {'prepared': named_prepare, 'encoded_plan':encoded_plan}

                response, content = http.request(url, 'POST', headers=headers, body=json.dumps(body))

                return eval(content)

            elif named_prepare and not encoded_plan:
                params = 'prepared=' + urllib.parse.quote(prepared, '~()')
                params = 'prepared="%s"'% named_prepare
            else:
                if isinstance(query, dict):
                    prepared = json.dumps(query['name'])
                else:
                    prepared = json.dumps(query)
                prepared = str(prepared)
                params = 'prepared=' + urllib.parse.quote(prepared, '~()')
            if 'creds' in query_params and query_params['creds']:
                headers = self._create_headers_with_auth(query_params['creds'][0]['user'],
                                                         query_params['creds'][0]['pass'])
            api = "http://%s:%s/query/service?%s" % (self.ip, port, params)
            log.info("%s"%api)
        else:
            params = {key : query}
            #log.info("-->query_params:{}".format(query_params))
            try:
              #log.info("-->user={}".format(query_params['creds'][0]['user']))
              #log.info("-->password={}".format(query_params['creds'][0]['pass']))
              if 'creds' in query_params and query_params['creds']:
                headers = self._create_headers_with_auth(query_params['creds'][0]['user'],
                                                         query_params['creds'][0]['pass'])
                del query_params['creds']
              #log.info("-->headers={}".format(headers))
            except Exception:
                traceback.print_exc()

            params.update(query_params)
            #log.info("-->before urlencode params:{}".format(params))
            params = urllib.parse.urlencode(params)
            #log.info("-->after urlencode params:{}".format(params))
            if verbose:
                log.info('query params : {0}'.format(params))
            api = "http://%s:%s/query?%s" % (self.ip, port, params)

        status, content, header = self._http_request(api, 'POST', timeout=timeout, headers=headers)
        try:
            return json.loads(content)
        except ValueError:
            return content

    def analytics_tool(self, query, port=8095, timeout=650, query_params={}, is_prepared=False, named_prepare=None,
                   verbose = True, encoded_plan=None, servers=None):
        key = 'prepared' if is_prepared else 'statement'
        headers = None
        content=""
        prepared = json.dumps(query)
        if is_prepared:
            if named_prepare and encoded_plan:
                http = httplib2.Http()
                if len(servers)>1:
                    url = "http://%s:%s/query/service" % (servers[1].ip, port)
                else:
                    url = "http://%s:%s/query/service" % (self.ip, port)

                headers = {'Content-type': 'application/json'}
                body = {'prepared': named_prepare, 'encoded_plan':encoded_plan}

                response, content = http.request(url, 'POST', headers=headers, body=json.dumps(body))

                return eval(content)

            elif named_prepare and not encoded_plan:
                params = 'prepared=' + urllib.parse.quote(prepared, '~()')
                params = 'prepared="%s"'% named_prepare
            else:
                prepared = json.dumps(query)
                prepared = str(prepared.encode('utf-8'))
                params = 'prepared=' + urllib.parse.quote(prepared, '~()')
            if 'creds' in query_params and query_params['creds']:
                headers = self._create_headers_with_auth(query_params['creds'][0]['user'],
                                                         query_params['creds'][0]['pass'])
            api = "%s/analytics/service?%s" % (self.cbas_base_url, params)
            log.info("%s"%api)
        else:
            params = {key : query}
            if 'creds' in query_params and query_params['creds']:
                headers = self._create_headers_with_auth(query_params['creds'][0]['user'],
                                                         query_params['creds'][0]['pass'])
                del query_params['creds']
            params.update(query_params)
            params = urllib.parse.urlencode(params)
            if verbose:
                log.info('query params : {0}'.format(params))
            api = "%s/analytics/service?%s" % (self.cbas_base_url, params)
        status, content, header = self._http_request(api, 'POST', timeout=timeout, headers=headers)
        try:
            return json.loads(content)
        except ValueError:
            return content

    def query_tool_stats(self):
        log.info('query n1ql stats')
        api = "http://%s:8093/admin/stats" % (self.ip)
        status, content, header = self._http_request(api, 'GET')
        log.info(content)
        try:
            return json.loads(content)
        except ValueError:
            return content

    def index_tool_stats(self):
        log.info('index n1ql stats')
        api = "http://%s:8091/indexStatus" % (self.ip)
        params = ""
        status, content, header = self._http_request(api, 'GET', params)
        log.info(content)
        try:
            return json.loads(content)
        except ValueError:
            return content

    # return all rack/zone info
    def get_all_zones_info(self, timeout=120):
        zones = {}
        api = self.baseUrl + 'pools/default/serverGroups'
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            zones = json.loads(content)
        else:
            raise Exception("Failed to get all zones info.\n \
                  Zone only supports from couchbase server version 2.5 and up.")
        return zones

    # return group name and unique uuid
    def get_zone_names(self):
        zone_names = {}
        zone_info = self.get_all_zones_info()
        if zone_info and len(zone_info["groups"]) >= 1:
            for i in range(0, len(zone_info["groups"])):
                # pools/default/serverGroups/ = 27 chars
                zone_names[zone_info["groups"][i]["name"]] = zone_info["groups"][i]["uri"][28:]
        return zone_names

    def add_zone(self, zone_name):
        api = self.baseUrl + 'pools/default/serverGroups'
        request_name = "name={0}".format(zone_name)
        status, content, header = self._http_request(api, "POST", \
                                        params=request_name)
        if status:
            log.info("zone {0} is added".format(zone_name))
            return True
        else:
            raise Exception("Failed to add zone with name: %s " % zone_name)

    def delete_zone(self, zone_name):
        api = self.baseUrl + 'pools/default/serverGroups/'
        # check if zone exist
        found = False
        zones = self.get_zone_names()
        for zone in zones:
            if zone_name == zone:
                api += zones[zone_name]
                found = True
                break
        if not found:
            raise Exception("There is not zone with name: %s in cluster" % zone_name)
        status, content, header = self._http_request(api, "DELETE")
        if status:
            log.info("zone {0} is deleted".format(zone_name))
        else:
            raise Exception("Failed to delete zone with name: %s " % zone_name)

    def rename_zone(self, old_name, new_name):
        api = self.baseUrl + 'pools/default/serverGroups/'
        # check if zone exist
        found = False
        zones = self.get_zone_names()
        for zone in zones:
            if old_name == zone:
                api += zones[old_name]
                request_name = "name={0}".format(new_name)
                found = True
                break
        if not found:
            raise Exception("There is not zone with name: %s in cluster" % old_name)
        status, content, header = self._http_request(api, "PUT", params=request_name)
        if status:
            log.info("zone {0} is renamed to {1}".format(old_name, new_name))
        else:
            raise Exception("Failed to rename zone with name: %s " % old_name)

    # get all nodes info in one zone/rack/group
    def get_nodes_in_zone(self, zone_name):
        nodes = {}
        tmp = {}
        zone_info = self.get_all_zones_info()
        if zone_name != "":
            found = False
            if len(zone_info["groups"]) >= 1:
                for i in range(0, len(zone_info["groups"])):
                    if zone_info["groups"][i]["name"] == zone_name:
                        tmp = zone_info["groups"][i]["nodes"]
                        if not tmp:
                            log.info("zone {0} is existed but no node in it".format(zone_name))
                        # remove port
                        for node in tmp:
                            node["hostname"] = node["hostname"].split(":")
                            node["hostname"] = node["hostname"][0]
                            nodes[node["hostname"]] = node
                        found = True
                        break
            if not found:
                raise Exception("There is not zone with name: %s in cluster" % zone_name)
        return nodes

    def get_zone_and_nodes(self):
        """ only return zones with node in its """
        zones = {}
        tmp = {}
        zone_info = self.get_all_zones_info()
        if len(zone_info["groups"]) >= 1:
            for i in range(0, len(zone_info["groups"])):
                tmp = zone_info["groups"][i]["nodes"]
                if not tmp:
                    log.info("zone {0} is existed but no node in it".format(tmp))
                # remove port
                else:
                    nodes = []
                    for node in tmp:
                        node["hostname"] = node["hostname"].split(":")
                        node["hostname"] = node["hostname"][0]
                        print(node["hostname"][0])
                        nodes.append(node["hostname"])
                    zones[zone_info["groups"][i]["name"]] = nodes
        return zones

    def get_zone_uri(self):
        zone_uri = {}
        zone_info = self.get_all_zones_info()
        if zone_info and len(zone_info["groups"]) >= 1:
            for i in range(0, len(zone_info["groups"])):
                zone_uri[zone_info["groups"][i]["name"]] = zone_info["groups"][i]["uri"]
        return zone_uri

    def shuffle_nodes_in_zones(self, moved_nodes, source_zone, target_zone):
        # moved_nodes should be a IP list like
        # ["192.168.171.144", "192.168.171.145"]
        request = ""
        for i in range(0, len(moved_nodes)):
            moved_nodes[i] = "ns_1@" + moved_nodes[i]

        all_zones = self.get_all_zones_info()
        api = self.baseUrl + all_zones["uri"][1:]

        moved_node_json = []
        for i in range(0, len(all_zones["groups"])):
            for node in all_zones["groups"][i]["nodes"]:
                if all_zones["groups"][i]["name"] == source_zone:
                    for n in moved_nodes:
                        if n == node["otpNode"]:
                            moved_node_json.append({"otpNode": node["otpNode"]})

        zone_json = {}
        group_json = []
        for i in range(0, len(all_zones["groups"])):
            node_j = []
            zone_json["uri"] = all_zones["groups"][i]["uri"]
            zone_json["name"] = all_zones["groups"][i]["name"]
            zone_json["nodes"] = node_j

            if not all_zones["groups"][i]["nodes"]:
                if all_zones["groups"][i]["name"] == target_zone:
                    for i in range(0, len(moved_node_json)):
                        zone_json["nodes"].append(moved_node_json[i])
                else:
                    zone_json["nodes"] = []
            else:
                for node in all_zones["groups"][i]["nodes"]:
                    if all_zones["groups"][i]["name"] == source_zone and \
                                           node["otpNode"] in moved_nodes:
                        pass
                    else:
                        node_j.append({"otpNode": node["otpNode"]})
                if all_zones["groups"][i]["name"] == target_zone:
                    for k in range(0, len(moved_node_json)):
                        node_j.append(moved_node_json[k])
                    zone_json["nodes"] = node_j
            group_json.append({"name": zone_json["name"], "uri": zone_json["uri"], "nodes": zone_json["nodes"]})
        request = '{{"groups": {0} }}'.format(json.dumps(group_json))
        status, content, header = self._http_request(api, "PUT", params=request)
        # sample request format
        # request = ' {"groups":[{"uri":"/pools/default/serverGroups/0","nodes": [] },\
        #                       {"uri":"/pools/default/serverGroups/c8275b7a88e6745c02815dde4a505e70","nodes": [] },\
        #                        {"uri":"/pools/default/serverGroups/1acd9810a027068bd14a1ddd43db414f","nodes": \
        #                               [{"otpNode":"ns_1@192.168.171.144"},{"otpNode":"ns_1@192.168.171.145"}]} ]} '
        return status

    def is_zone_exist(self, zone_name):
        found = False
        zones = self.get_zone_names()
        if zones:
            for zone in zones:
                if zone_name == zone:
                    found = True
                    return True
                    break
        if not found:
            log.error("There is not zone with name: {0} in cluster.".format(zone_name))
            return False

    def get_items_info(self, keys, bucket='default'):
        items_info = {}
        for key in keys:
            api = '{0}{1}{2}/docs/{3}'.format(self.baseUrl, 'pools/default/buckets/', bucket, key)
            status, content, header = self._http_request(api)
            if status:
                items_info[key] = json.loads(content)
        return items_info

    def start_cluster_logs_collection(self, nodes="*", upload=False, \
                                      uploadHost=None, customer="", ticket=""):
        if not upload:
            params = urllib.parse.urlencode({"nodes":nodes})
        else:
            params = urllib.parse.urlencode({"nodes":nodes, "uploadHost":uploadHost, \
                                       "customer":customer, "ticket":ticket})
        api = self.baseUrl + "controller/startLogsCollection"
        status, content, header = self._http_request(api, "POST", params)
        return status, content

    def get_cluster_logs_collection_info(self):
        api = self.baseUrl + "pools/default/tasks/"
        status, content, header = self._http_request(api, "GET")
        if status:
            tmp = json.loads(content)
            for k in tmp:
                if k["type"] == "clusterLogsCollection":
                    content = k
                    return content
        return None

    """ result["progress"]: progress logs collected at cluster level
        result["status]: status logs collected at cluster level
        result["perNode"]: all information logs collected at each node """
    def get_cluster_logs_collection_status(self):
        result = self.get_cluster_logs_collection_info()
        if result:
            return result["progress"], result["status"], result["perNode"]
        return None, None, None

    def cancel_cluster_logs_collection(self):
        api = self.baseUrl + "controller/cancelLogsCollection"
        status, content, header = self._http_request(api, "POST")
        return status, content

    def set_log_redaction_level(self, redaction_level="none"):
        api = self.baseUrl + "settings/logRedaction"
        params = urllib.parse.urlencode({"logRedactionLevel":redaction_level})
        status, content, header = self._http_request(api, "POST", params)
        if status:
            result = json.loads(content)
            if result["logRedactionLevel"] == redaction_level:
                return True
            else:
                return False
        return False

    def get_bucket_CCCP(self, bucket):
        log.info("Getting CCCP config ")
        api = '%spools/default/b/%s' % (self.baseUrl, bucket)
        if isinstance(bucket, Bucket):
            api = '%spools/default/b/%s' % (self.baseUrl, bucket.name)
        status, content, header = self._http_request(api)
        if status:
            return json.loads(content)
        return None

    def get_recovery_task(self):
        content = self.ns_server_tasks()
        for item in content:
            if item["type"] == "recovery":
                return item
        return None


    def get_recovery_progress(self, recoveryStatusURI):
        api = '%s%s' % (self.baseUrl, recoveryStatusURI)
        status, content, header = self._http_request(api)
        if status:
            return json.loads(content)
        return None

    def get_warming_up_tasks(self):
        tasks = self.ns_server_tasks()
        tasks_warmup = []
        for task in tasks:
            if task["type"] == "warming_up":
                tasks_warmup.append(task)
        return tasks_warmup

    def compact_bucket(self, bucket="default"):
        api = self.baseUrl + 'pools/default/buckets/{0}/controller/compactBucket'.format(bucket)
        status, content, header = self._http_request(api, 'POST')
        if status:
            log.info('bucket compaction successful')
        else:
            raise BucketCompactionException(bucket)

        return True

    def cancel_bucket_compaction(self, bucket="default"):
        api = self.baseUrl + 'pools/default/buckets/{0}/controller/cancelBucketCompaction'.format(bucket)
        if isinstance(bucket, Bucket):
            api = self.baseUrl + 'pools/default/buckets/{0}/controller/cancelBucketCompaction'.format(bucket.name)
        status, content, header = self._http_request(api, 'POST')
        log.info("Status is {0}".format(status))
        if status:
            log.info('Cancel bucket compaction successful')
        else:
            raise BucketCompactionException(bucket)
        return True


    '''LDAP Rest API '''
    '''
    clearLDAPSettings - Function to clear LDAP settings
    Parameter - None
    Returns -
    status of LDAPAuth clear command
    '''
    def clearLDAPSettings(self):
        api = self.baseUrl + 'settings/saslauthdAuth'
        params = urllib.parse.urlencode({'enabled':'false'})
        status, content, header = self._http_request(api, 'POST', params)
        return status, content, header

    '''
    ldapUserRestOperation - Execute LDAP REST API
    Input Parameter -
        authOperation - this is for auth need to be enabled or disabled - True or 0
        currAdmmins - a list of username to add to full admin matching with ldap
        currROAdmins - a list of username to add to RO Admin
    Returns - status, content and header for the command executed
    '''
    def ldapUserRestOperation(self, authOperation, adminUser='', ROadminUser=''):
        authOperation = authOperation
        currAdmins = ''
        currROAdmins = ''

        if (adminUser != ''):
            for user in adminUser:
                currAdmins = user[0] + "\n\r" + currAdmins

        if (ROadminUser != ''):
            for user in ROadminUser:
                currROAdmins = user[0] + "\n\r" + currROAdmins
        content = self.executeLDAPCommand(authOperation, currAdmins, currROAdmins)

    '''LDAP Rest API '''
    '''
    clearLDAPSettings - Function to clear LDAP settings
    Parameter - None
    Returns -
    status of LDAPAuth clear command
    '''
    def clearLDAPSettings (self):
        api = self.baseUrl + 'settings/saslauthdAuth'
        params = urllib.parse.urlencode({'enabled':'false'})
        status, content, header = self._http_request(api, 'POST', params)
        return status, content, header

    '''
    ldapUserRestOperation - Execute LDAP REST API
    Input Parameter -
        authOperation - this is for auth need to be enabled or disabled - True or 0
        currAdmmins - a list of username to add to full admin matching with ldap
        currROAdmins - a list of username to add to RO Admin
    Returns - status, content and header for the command executed
    '''
    def ldapUserRestOperation(self, authOperation, adminUser='', ROadminUser='', exclude=None):
        if (authOperation):
            authOperation = 'true'
        else:
            authOperation = 'false'

        currAdmins = ''
        currROAdmins = ''

        if (adminUser != ''):
            for user in adminUser:
                currAdmins = user[0] + "\n\r" + currAdmins

        if (ROadminUser != ''):
            for user in ROadminUser:
                currROAdmins = user[0] + "\n\r" + currROAdmins
        content = self.executeLDAPCommand(authOperation, currAdmins, currROAdmins, exclude)

    '''
    executeLDAPCommand - Execute LDAP REST API
    Input Parameter -
        authOperation - this is for auth need to be enabled or disabled - True or 0
        currAdmmins - a list of username to add to full admin matching with ldap
        currROAdmins - a list of username to add to RO Admin
    Returns - status, content and header for the command executed
    '''
    def executeLDAPCommand(self, authOperation, currAdmins, currROAdmins, exclude=None):
        api = self.baseUrl + "settings/saslauthdAuth"

        if (exclude is None):
            log.info ("into exclude is None")
            params = urllib.parse.urlencode({
                                            'enabled': authOperation,
                                            'admins': '{0}'.format(currAdmins),
                                            'roAdmins': '{0}'.format(currROAdmins),
                                            })
        else:
            log.info ("Into exclude for value of fullAdmin {0}".format(exclude))
            if (exclude == 'fullAdmin'):
                params = urllib.parse.urlencode({
                                            'enabled': authOperation,
                                            'roAdmins': '{0}'.format(currROAdmins),
                                            })
            else:
                log.info ("Into exclude for value of fullAdmin {0}".format(exclude))
                params = urllib.parse.urlencode({
                                            'enabled': authOperation,
                                            'admins': '{0}'.format(currAdmins),
                                            })


        status, content, header = self._http_request(api, 'POST', params)
        return content
    '''
    validateLogin - Validate if user can login using a REST API
    Input Parameter - user and password to check for login. Also take a boolean to
    decide if the status should be 200 or 400 and everything else should be
    false
    Returns - True of false based if user should login or login fail
    '''
    def validateLogin(self, user, password, login, getContent=False):
        api = self.baseUrl + "uilogin"
        header = {'Content-type': 'application/x-www-form-urlencoded'}
        params = urllib.parse.urlencode({'user':'{0}'.format(user), 'password':'{0}'.format(password)})
        log.info ("value of param is {0}".format(params))
        http = httplib2.Http()
        status, content = http.request(api, 'POST', headers=header, body=params)
        log.info ("Status of login command - {0}".format(status))
        if (getContent):
            return status, content
        if ((status['status'] == "200" and login == True) or (status ['status'] == "400" and login == False)):
            return True
        else:
            return False

    '''
    ldapRestOperationGet - Get setting of LDAPAuth - Settings
    Returns - list of Admins, ROAdmins and is LDAPAuth enabled or not
    '''
    def ldapRestOperationGetResponse(self):
        log.info ("GET command for LDAP Auth")
        api = self.baseUrl + "settings/saslauthdAuth"
        status, content, header = self._http_request(api, 'GET')
        return json.loads(content)

    '''
    executeValidateCredentials - API to check credentials of users
    Input - user and password that needs validation
    Returns -
        [role]:<currentrole>
        [source]:<saslauthd,builtin>
    '''
    def executeValidateCredentials(self, user, password):
        api = self.baseUrl + "validateCredentials"
        params = urllib.parse.urlencode({
                                   'user':'{0}'.format(user),
                                   'password':'{0}'.format(password)
                                   })
        status, content, header = self._http_request(api, 'POST', params)
        log.info ("Status of executeValidateCredentials command - {0}".format(status))
        return status, json.loads(content)

    '''
    Audit Commands
    '''
    '''
    getAuditSettings - API returns audit settings for Audit
    Input - None
    Returns -
        [archive_path]:<path for archieve>
        [auditd_enabled]:<enabled disabled status for auditd>
        [log_path]:<path for logs>
        [rotate_interval]:<log rotate interval>
    '''
    def getAuditSettings(self):
        api = self.baseUrl + "settings/audit"
        status, content, header = self._http_request(api, 'GET')
        return json.loads(content)

    '''
    getAuditSettings - API returns audit settings for Audit
    Input -
        [archive_path]:<path for archieve>
        [auditd_enabled]:<enabled disabled status for auditd>
        [rotate_interval]:<log rotate interval in seconds>
    '''
    def setAuditSettings(self, enabled='true', rotateInterval=86400, logPath='/opt/couchbase/var/lib/couchbase/logs'):
        api = self.baseUrl + "settings/audit"
        params = urllib.parse.urlencode({
                                    'rotateInterval':'{0}'.format(rotateInterval),
                                    'auditdEnabled':'{0}'.format(enabled),
                                    'logPath':'{0}'.format(logPath)
                                    })
        status, content, header = self._http_request(api, 'POST', params)
        log.info ("Value os status is {0}".format(status))
        log.info ("Value of content is {0}".format(content))
        if (status):
            return status
        else:
            return status, json.loads(content)

    def set_downgrade_storage_mode_with_rest(self, downgrade=True, username="Administrator",
                                                                   password="password"):
        authorization = self.get_authorization(username, password)
        if downgrade:
            api = self.index_baseUrl + 'settings/storageMode?downgrade=true'
        else:
            api = self.index_baseUrl + 'settings/storageMode?downgrade=false'
        headers = {'Content-type': 'application/json','Authorization': 'Basic %s'
                                                                 % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers)
        if not status:
            raise Exception(content)
        return json.loads(content)

    def create_index_with_rest(self, create_info, username="Administrator", password="password"):
        log.info("CREATE INDEX USING REST WITH PARAMETERS: " + str(create_info))
        authorization = self.get_authorization(username, password)
        api = self.index_baseUrl + 'internal/indexes?create=true'
        headers = {'Content-type': 'application/json','Authorization': 'Basic %s' % authorization}
        params = json.loads("{0}".format(create_info).replace('\'', '"').replace('True', 'true').replace('False', 'false'))
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                             params=json.dumps(params).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return json.loads(content)

    def build_index_with_rest(self, id, username="Administrator", password="password"):
        credentials = '{}:{}'.format(self.username, self.password)
        authorization = base64.encodebytes(credentials.encode('utf-8'))
        authorization = authorization.decode('utf-8').rstrip('\n')
        api = self.index_baseUrl + 'internal/indexes?build=true'
        build_info = {'ids': [id]}
        headers = {'Content-type': 'application/json','Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'PUT', headers=headers,
                                               params=json.dumps(build_info))
        if not status:
            raise Exception(content)
        return json.loads(content)

    def drop_index_with_rest(self, id, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = 'internal/index/{0}'.format(id)
        api = self.index_baseUrl + url
        headers = {'Content-type': 'application/json','Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'DELETE', headers=headers)
        if not status:
            raise Exception(content)

    def get_all_indexes_with_rest(self, username="Administrator", password="password"):
        credentials = '{}:{}'.format(self.username, self.password)
        authorization = base64.encodebytes(credentials.encode('utf-8'))
        authorization = authorization.decode('utf-8').rstrip('\n')
        url = 'internal/indexes'
        api = self.index_baseUrl + url
        headers = {'Content-type': 'application/json','Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return json.loads(content)

    def lookup_gsi_index_with_rest(self, id, body, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = 'internal/index/{0}?lookup=true'.format(id)
        api = self.index_baseUrl + url
        headers = {'Content-type': 'application/json','Authorization': 'Basic %s' % authorization}
        params = json.loads("{0}".format(body).replace('\'', '"').replace('True', 'true').replace('False', 'false'))
        status, content, header = self._http_request(api, 'GET', headers=headers,
                                             params=json.dumps(params).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return json.loads(content)

    def full_table_scan_gsi_index_with_rest(self, id, body, username="Administrator", password="password"):
        if "limit" not in list(body.keys()):
            body["limit"] = 900000
        authorization = self.get_authorization(username, password)
        url = 'internal/index/{0}?scanall=true'.format(id)
        api = self.index_baseUrl + url
        headers = {'Content-type': 'application/json','Authorization': 'Basic %s' % authorization}
        params = json.loads("{0}".format(body).replace('\'', '"').replace('True', 'true').replace('False', 'false'))
        status, content, header = self._http_request(
            api, 'GET', headers=headers,
            params=json.dumps(params).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        # Following line is added since the content uses chunked encoding
        chunkless_content = content.decode().replace("][", ", \n")

        return json.loads(chunkless_content)

    def range_scan_gsi_index_with_rest(self, id, body, username="Administrator", password="password"):
        if "limit" not in list(body.keys()):
            body["limit"] = 300000
        authorization = self.get_authorization(username, password)
        url = 'internal/index/{0}?range=true'.format(id)
        api = self.index_baseUrl + url
        headers = {'Content-type': 'application/json',
                   'Authorization': 'Basic %s' % authorization}
        params = json.loads("{0}".format(body).replace(
            '\'', '"').replace('True', 'true').replace('False', 'false'))
        status, content, header = self._http_request(
            api, 'GET', headers=headers,
            params=json.dumps(params).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        #Below line is there because of MB-20758
        content = content.split(b'[]')[0].decode()
        # Following line is added since the content uses chunked encoding
        chunkless_content = content.decode().replace("][", ", \n")
        return json.loads(chunkless_content)

    def multiscan_for_gsi_index_with_rest(self, id, body, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = 'internal/index/{0}?multiscan=true'.format(id)
        api = self.index_baseUrl + url
        headers = {'Accept': 'application/json','Authorization': 'Basic %s' % authorization}
        params = json.loads("{0}".format(body).replace('\'', '"').replace(
            'True', 'true').replace('False', 'false').replace(
            "~[]{}UnboundedtruenilNA~", "~[]{}UnboundedTruenilNA~"))
        params = json.dumps(params).encode("ascii", "ignore").decode().replace("\\\\", "\\")
        log.info(json.dumps(params).encode("ascii", "ignore"))
        status, content, header = self._http_request(api, 'GET', headers=headers,
                                                     params=params)
        if not status:
            raise Exception(content)
        #Below line is there because of MB-20758
        content = content.split(b'[]')[0].decode()
        # Following line is added since the content uses chunked encoding
        chunkless_content = content.replace("][", ", \n")
        if chunkless_content:
            return json.loads(chunkless_content)
        else:
            return content

    def multiscan_count_for_gsi_index_with_rest(self, id, body, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = 'internal/index/{0}?multiscancount=true'.format(id)
        api = self.index_baseUrl + url
        headers = {'Accept': 'application/json','Authorization': 'Basic %s' % authorization}
        count_cmd_body = body.replace('\'', '"').replace('True', 'true').replace('False', 'false')
        count_cmd_body = count_cmd_body.replace("~[]{}UnboundedtruenilNA~", "~[]{}UnboundedTruenilNA~")
        params = json.loads(count_cmd_body)
        params = json.dumps(params).encode("ascii", "ignore").decode().replace("\\\\", "\\")
        log.info(json.dumps(params).encode("ascii", "ignore"))
        status, content, header = self._http_request(api, 'GET', headers=headers,
                                                     params=params)
        if not status:
            raise Exception(content)
        #Below line is there because of MB-20758
        content = content.split(b'[]')[0].decode()
        # Following line is added since the content uses chunked encoding
        chunkless_content = content.replace("][", ", \n")
        if chunkless_content:
            return json.loads(chunkless_content)
        else:
            return content

    'Get list of all roles that exist in the system'
    def retrive_all_user_role(self):
        url = "/settings/rbac/roles"
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'GET')
        if not status:
            raise Exception(content)
        return json.loads(content)

    'Get list of current users and rols assigned to them'
    def retrieve_user_roles(self):
        url = "/settings/rbac/users"
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'GET')
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
    Add/Update user role assignment
    user_id=userid of the user to act on
    payload=name=<nameofuser>&roles=admin,cluster_admin'''
    def set_user_roles(self, user_id, payload):
        url = "settings/rbac/users/" + user_id
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'PUT', payload)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
    Delete user from couchbase role assignment
    user_id=userid of user to act on'''
    def delete_user_roles(self, user_id):
        url = "/settings/rbac/users/" + user_id
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'DELETE')
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
    Returns base64 string of username:password
    '''
    def get_authorization(self, username, password):
        credentials = '{}:{}'.format(username, password)
        authorization = base64.encodebytes(credentials.encode('utf-8'))
        return authorization.decode('utf-8').rstrip('\n')

    '''
    Return list of permission with True/False if user has permission or not
    user_id = userid for checking permission
    password = password for userid
    permission_set=cluster.bucket[default].stats!read,cluster.bucket[default]!write
    '''
    def check_user_permission(self, user_id, password, permission_set):
        url = "pools/default/checkPermissions/"
        api = self.baseUrl + url
        authorization = self.get_authorization(user_id, password)
        header = {'Content-Type': 'application/x-www-form-urlencoded',
              'Authorization': 'Basic %s' % authorization,
              'Accept': '*/*'}
        status, content, header = self._http_request(api, 'POST', params=permission_set, headers=header)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
    Add/Update user role assignment
    user_id=userid of the user to act on
    payload=name=<nameofuser>&roles=admin,cluster_admin&password=<password>
    if roles=<empty> user will be created with no roles'''
    def add_set_builtin_user(self, user_id, payload):
        url = "settings/rbac/users/local/" + user_id
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'PUT', payload)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
    Delete built-in user
    '''
    def delete_builtin_user(self, user_id):
        url = "settings/rbac/users/local/" + user_id
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'DELETE')
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
    Add/Update user role assignment
    user_id=userid of the user to act on
    password=<new password>'''
    def change_password_builtin_user(self, user_id, password):
        url = "controller/changePassword/" + user_id
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'POST', password)
        if not status:
            raise Exception(content)
        return json.loads(content)

    # Applicable to eventing service
    '''
        Save the Function so that it is visible in UI
    '''
    def save_function(self, name, body):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/saveAppTempStore/?name=" + name
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    '''
            Deploy the Function
    '''
    def deploy_function(self, name, body):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/setApplication/?name=" + name
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    '''
            GET all the Functions
    '''
    def get_all_functions(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/functions"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Undeploy the Function
    '''
    def set_settings_for_function(self, name, body):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/functions/" + name +"/settings"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))

        if not status:
            raise Exception(content)
        return content

    '''
        undeploy the Function 
    '''
    def undeploy_function(self, name):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/functions/" + name +"/settings"
        body= {"deployment_status": False, "processing_status": False}
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    '''
        Delete all the functions 
    '''
    def delete_all_function(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/functions"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'DELETE', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Delete single function
    '''
    def delete_single_function(self, name):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/functions/" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'DELETE', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Delete the Function from UI
    '''
    def delete_function_from_temp_store(self, name):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/deleteAppTempStore/?name=" + name
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'DELETE', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Delete the Function
    '''
    def delete_function(self, name):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/deleteApplication/?name=" + name
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'DELETE', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Export the Function
    '''
    def export_function(self, name):
        export_map = {}
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/export/" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        if status:
            json_parsed = json.loads(content)
            for key in list(json_parsed[0].keys()): # returns an array
                tokens = key.split(":")
                val = json_parsed[0][key]
                if len(tokens) == 1:
                    field = tokens[0]
                    export_map[field] = val
        return export_map

    '''
             Ensure that the eventing node is out of bootstrap node
    '''
    def get_deployed_eventing_apps(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "getDeployedApps"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
            Ensure that the eventing node is out of bootstrap node
    '''

    def get_running_eventing_apps(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "getRunningApps"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
            composite status of a handler
    '''
    def get_composite_eventing_status(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "/api/v1/status"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
             Get Eventing processing stats
    '''
    def get_event_processing_stats(self, name, eventing_map=None):
        if eventing_map is None:
            eventing_map = {}
        authorization = self.get_authorization(self.username, self.password)
        url = "getEventProcessingStats?name=" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if status:
            json_parsed = json.loads(content)
            for key in list(json_parsed.keys()):
                tokens = key.split(":")
                val = json_parsed[key]
                if len(tokens) == 1:
                    field = tokens[0]
                    eventing_map[field] = val
        return eventing_map

    '''
            Get Aggregate Eventing processing stats
    '''
    def get_aggregate_event_processing_stats(self, name, eventing_map=None):
        if eventing_map is None:
            eventing_map = {}
        authorization = self.get_authorization(self.username, self.password)
        url = "getAggEventProcessingStats?name=" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if status:
            json_parsed = json.loads(content)
            for key in list(json_parsed.keys()):
                tokens = key.split(":")
                val = json_parsed[key]
                if len(tokens) == 1:
                    field = tokens[0]
                    eventing_map[field] = val
        return eventing_map

    '''
            Get Eventing execution stats
    '''
    def get_event_execution_stats(self, name, eventing_map=None):
        if eventing_map is None:
            eventing_map = {}
        authorization = self.get_authorization(self.username, self.password)
        url = "getExecutionStats?name=" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if status:
            json_parsed = json.loads(content)
            for key in list(json_parsed.keys()):
                tokens = key.split(":")
                val = json_parsed[key]
                if len(tokens) == 1:
                    field = tokens[0]
                    eventing_map[field] = val
        return eventing_map

    '''
            Get Eventing failure stats
    '''
    def get_event_failure_stats(self, name, eventing_map=None):
        if eventing_map is None:
            eventing_map = {}
        authorization = self.get_authorization(self.username, self.password)
        url = "getFailureStats?name=" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if status:
            json_parsed = json.loads(content)
            for key in list(json_parsed.keys()):
                tokens = key.split(":")
                val = json_parsed[key]
                if len(tokens) == 1:
                    field = tokens[0]
                    eventing_map[field] = val
        return eventing_map

    '''
            Get all eventing stats
    '''
    def get_all_eventing_stats(self, seqs_processed=False, eventing_map=None):
        if eventing_map is None:
            eventing_map = {}
        authorization = self.get_authorization(self.username, self.password)
        if seqs_processed:
            url = "api/v1/stats?type=full"
        else:
            url = "api/v1/stats"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
            Cleanup eventing 
    '''
    def cleanup_eventing(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "cleanupEventing"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
               enable debugger
    '''
    def enable_eventing_debugger(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/api/v1/config"
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        body="{\"enable_debugger\": true}"
        status, content, header = self._http_request(api, 'POST', headers=headers, params=body)
        if not status:
            raise Exception(content)
        return content

    '''
                   disable debugger
    '''

    def disable_eventing_debugger(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/api/v1/config"
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        body = "{\"enable_debugger\": false}"
        status, content, header = self._http_request(api, 'POST', headers=headers, params=body)
        if not status:
            raise Exception(content)
        return content

    '''
            Start debugger
    '''
    def start_eventing_debugger(self, name):
        authorization = self.get_authorization(self.username, self.password)
        url="/pools/default"
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        url = "_p/event/startDebugger/?name=" + name
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers, params=content)
        if not status:
            raise Exception(content)
        return content

    '''
            Stop debugger
    '''
    def stop_eventing_debugger(self, name):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/stopDebugger/?name=" + name
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Get debugger url
    '''
    def get_eventing_debugger_url(self, name):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/getDebuggerUrl/?name=" + name
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers)
        if not status:
            raise Exception(content)
        return content

    def create_function(self, name, body):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/functions/" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    def update_function(self, name, body):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/functions/" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        body['appname']=name
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    def get_function_details(self, name):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/functions/" + name
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return content

    def get_eventing_go_routine_dumps(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "debug/pprof/goroutine?debug=1"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return content

    def set_eventing_retry(self, name, body):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/functions/" + name + "/retry"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    def get_user(self, user_id):
        url = "settings/rbac/users/"
        api = self.baseUrl + url
        status, content, header = self._http_request(api, "GET")

        if content != None:
            content_json = json.loads(content)

        for i in range(len(content_json)):
            user = content_json[i]
            if user.get('id') == user_id:
                return user
        return {}

    '''
    Update IP version on server.
    afamily: The value must be one of the following: [ipv4,ipv6] 
    '''
    def set_ip_version(self, afamily='ipv4'):
        params = urllib.parse.urlencode({'afamily': afamily})
        api = "%snode/controller/setupNetConfig" % (self.baseUrl)
        status, content, header = self._http_request(api, 'POST', params)
        return status, content

    # These methods are added for Auto-Rebalance On Failure tests
    def set_retry_rebalance_settings(self, body):
        url = "settings/retryRebalance"
        api = self.baseUrl + url
        params = urllib.parse.urlencode(body)
        headers = self._create_headers()
        status, content, header = self._http_request(api, 'POST', headers=headers, params=params)

        if not status:
            raise Exception(content)
        return content

    def get_retry_rebalance_settings(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "settings/retryRebalance"
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)

        if not status:
            raise Exception(content)
        return content

    def get_pending_rebalance_info(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "pools/default/pendingRetryRebalance"
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)

        if not status:
            raise Exception(content)
        return content

    def cancel_pending_rebalance(self, id):
        authorization = self.get_authorization(self.username, self.password)
        url = "controller/cancelRebalanceRetry/" + str(id)
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers)

        if not status:
            raise Exception(content)
        return content

class MembaseServerVersion:
    def __init__(self, implementationVersion='', componentsVersion=''):
        self.implementationVersion = implementationVersion
        self.componentsVersion = componentsVersion


# this class will also contain more node related info
class OtpNode(object):
    def __init__(self, id='', status=''):
        self.id = id
        self.ip = ''
        self.replication = ''
        self.port = 8091
        self.gracefulFailoverPossible = 'true'
        # extract ns ip from the otpNode string
        # its normally ns_1@10.20.30.40
        if id.find('@') >= 0:
            self.ip = id[id.index('@') + 1:]
            if self.ip.count(':') > 0:
                # raw ipv6? enclose in square brackets
                self.ip = '[' + self.ip + ']'
        self.status = status


class NodeInfo(object):
    def __init__(self):
        self.availableStorage = None  # list
        self.memoryQuota = None


class NodeDataStorage(object):
    def __init__(self):
        self.type = ''  # hdd or ssd
        self.path = ''
        self.index_path = ''
        self.quotaMb = ''
        self.state = ''  # ok

    def __str__(self):
        return '{0}'.format({'type': self.type,
                             'path': self.path,
                             'index_path' : self.index_path,
                             'quotaMb': self.quotaMb,
                             'state': self.state})

    def get_data_path(self):
        return self.path

    def get_index_path(self):
        return self.index_path


class NodeDiskStorage(object):
    def __init__(self):
        self.type = 0
        self.path = ''
        self.sizeKBytes = 0
        self.usagePercent = 0


class Bucket(object):
    def __init__(self, bucket_size='', name="", authType="sasl", saslPassword="", num_replicas=0, port=11211, master_id=None,
                 type='', eviction_policy="valueOnly", bucket_priority=None, uuid="", lww=False, maxttl=None):
        self.name = name
        self.port = port
        self.type = type
        self.nodes = None
        self.stats = None
        self.servers = []
        self.vbuckets = []
        self.forward_map = []
        self.numReplicas = num_replicas
        self.saslPassword = saslPassword
        self.authType = ""
        self.bucket_size = bucket_size
        self.kvs = {1:KVStore()}
        self.authType = authType
        self.master_id = master_id
        self.eviction_policy = eviction_policy
        self.bucket_priority = bucket_priority
        self.uuid = uuid
        self.lww = lww
        self.maxttl = maxttl


    def __str__(self):
        return self.name


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
        self.clusterMembership = ""
        self.version = ""
        self.os = ""
        self.ports = []
        self.availableStorage = []
        self.storage = []
        self.memoryQuota = 0
        self.moxi = 11211
        self.memcached = 11210
        self.id = ""
        self.ip = ""
        self.rest_username = ""
        self.rest_password = ""
        self.port = 8091
        self.services = []
        self.storageTotalRam = 0


class AutoFailoverSettings(object):
    def __init__(self):
        self.enabled = True
        self.timeout = 0
        self.count = 0
        self.failoverOnDataDiskIssuesEnabled = False
        self.failoverOnDataDiskIssuesTimeout = 0
        self.maxCount = 1
        self.failoverServerGroup = False
        self.can_abort_rebalance = False


class AutoReprovisionSettings(object):
    def __init__(self):
        self.enabled = True
        self.max_nodes = 0
        self.count = 0


class NodePort(object):
    def __init__(self):
        self.proxy = 0
        self.direct = 0


class BucketStats(object):
    def __init__(self):
        self.opsPerSec = 0
        self.itemCount = 0
        self.diskUsed = 0
        self.memUsed = 0
        self.ram = 0


class vBucket(object):
    def __init__(self):
        self.master = ''
        self.replica = []
        self.id = -1

class RestParser(object):
    def parse_index_status_response(self, parsed):
        index_map = {}
        for map in parsed["indexes"]:
            bucket_name = map['bucket'].encode('ascii', 'ignore')
            if bucket_name not in list(index_map.keys()):
                index_map[bucket_name] = {}
            index_name = map['index'].encode('ascii', 'ignore')
            index_map[bucket_name][index_name] = {}
            index_map[bucket_name][index_name]['status'] = map['status'].encode('ascii', 'ignore')
            index_map[bucket_name][index_name]['progress'] = str(map['progress']).encode('ascii', 'ignore')
            index_map[bucket_name][index_name]['definition'] = map['definition'].encode('ascii', 'ignore')
            if len(map['hosts']) == 1:
                index_map[bucket_name][index_name]['hosts'] = map['hosts'][0].encode('ascii', 'ignore')
            else:
                index_map[bucket_name][index_name]['hosts'] = map['hosts']
            index_map[bucket_name][index_name]['id'] = map['id']
        return index_map

    def parse_index_stats_response(self, parsed, index_map=None):
        if index_map == None:
            index_map = {}
        for key in list(parsed.keys()):
            tokens = key.split(":")
            val = parsed[key]
            if len(tokens) > 2:
                bucket = tokens[0]
                index_name = tokens[1]
                stats_name = tokens[2]
                if bucket not in list(index_map.keys()):
                    index_map[bucket] = {}
                if index_name not in list(index_map[bucket].keys()):
                    index_map[bucket][index_name] = {}
                index_map[bucket][index_name][stats_name] = val
        return index_map

    def parse_get_nodes_response(self, parsed):
        node = Node()
        node.uptime = parsed['uptime']
        node.memoryFree = parsed['memoryFree']
        node.memoryTotal = parsed['memoryTotal']
        node.mcdMemoryAllocated = parsed['mcdMemoryAllocated']
        node.mcdMemoryReserved = parsed['mcdMemoryReserved']
        node.status = parsed['status']
        node.hostname = parsed['hostname']
        node.clusterCompatibility = parsed['clusterCompatibility']
        node.clusterMembership = parsed['clusterMembership']
        node.version = parsed['version']
        node.curr_items = 0
        if 'interestingStats' in parsed and 'curr_items' in parsed['interestingStats']:
            node.curr_items = parsed['interestingStats']['curr_items']
        node.port = parsed["hostname"][parsed["hostname"].rfind(":") + 1:]
        node.os = parsed['os']

        if "services" in parsed:
            node.services = parsed["services"]

        if "otpNode" in parsed:
            node.id = parsed["otpNode"]
        if "hostname" in parsed:
            # should work for both: ipv4 and ipv6
            node.ip = parsed["hostname"].rsplit(":", 1)[0]

        # memoryQuota
        if 'memoryQuota' in parsed:
            node.memoryQuota = parsed['memoryQuota']
        if 'availableStorage' in parsed:
            availableStorage = parsed['availableStorage']
            for key in availableStorage:
                # let's assume there is only one disk in each noce
                dict_parsed = parsed['availableStorage']
                if 'path' in dict_parsed and 'sizeKBytes' in dict_parsed and 'usagePercent' in dict_parsed:
                    diskStorage = NodeDiskStorage()
                    diskStorage.path = dict_parsed['path']
                    diskStorage.sizeKBytes = dict_parsed['sizeKBytes']
                    diskStorage.type = key
                    diskStorage.usagePercent = dict_parsed['usagePercent']
                    node.availableStorage.append(diskStorage)
                    log.info(diskStorage)

        if 'storage' in parsed:
            storage = parsed['storage']
            for key in storage:
                disk_storage_list = storage[key]
                for dict_parsed in disk_storage_list:
                    if 'path' in dict_parsed and 'state' in dict_parsed and 'quotaMb' in dict_parsed:
                        dataStorage = NodeDataStorage()
                        dataStorage.path = dict_parsed['path']
                        dataStorage.index_path = dict_parsed.get('index_path', '')
                        dataStorage.quotaMb = dict_parsed['quotaMb']
                        dataStorage.state = dict_parsed['state']
                        dataStorage.type = key
                        node.storage.append(dataStorage)

        # ports":{"proxy":11211,"direct":11210}
        if "ports" in parsed:
            ports = parsed["ports"]
            if "proxy" in ports:
                node.moxi = ports["proxy"]
            if "direct" in ports:
                node.memcached = ports["direct"]

        if "storageTotals" in parsed:
            storageTotals = parsed["storageTotals"]
            if storageTotals.get("ram"):
                if storageTotals["ram"].get("total"):
                    ramKB = storageTotals["ram"]["total"]
                    node.storageTotalRam = ramKB//(1024*1024)

                    if IS_CONTAINER:
                        # the storage total values are more accurate than
                        # mcdMemoryReserved - which is container host memory
                        node.mcdMemoryReserved = node.storageTotalRam * 0.70
        return node

    def parse_get_bucket_response(self, response):
        parsed = json.loads(response)
        return self.parse_get_bucket_json(parsed)

    def parse_get_bucket_json(self, parsed):
        bucket = Bucket()
        bucket.name = parsed['name']
        bucket.uuid = parsed['uuid']
        bucket.type = parsed['bucketType']
        if 'proxyPort' in parsed:
            bucket.port = parsed['proxyPort']
        bucket.authType = parsed["authType"]
        bucket.saslPassword = parsed["saslPassword"]
        bucket.nodes = list()
        if 'vBucketServerMap' in parsed:
            vBucketServerMap = parsed['vBucketServerMap']
            serverList = vBucketServerMap['serverList']
            bucket.servers.extend(serverList)
            if "numReplicas" in vBucketServerMap:
                bucket.numReplicas = vBucketServerMap["numReplicas"]
            # vBucketMapForward
            if 'vBucketMapForward' in vBucketServerMap:
                # let's gather the forward map
                vBucketMapForward = vBucketServerMap['vBucketMapForward']
                counter = 0
                for vbucket in vBucketMapForward:
                    # there will be n number of replicas
                    vbucketInfo = vBucket()
                    vbucketInfo.master = serverList[vbucket[0]]
                    if vbucket:
                        for i in range(1, len(vbucket)):
                            if vbucket[i] != -1:
                                vbucketInfo.replica.append(serverList[vbucket[i]])
                    vbucketInfo.id = counter
                    counter += 1
                    bucket.forward_map.append(vbucketInfo)
            vBucketMap = vBucketServerMap['vBucketMap']
            counter = 0
            for vbucket in vBucketMap:
                # there will be n number of replicas
                vbucketInfo = vBucket()
                vbucketInfo.master = serverList[vbucket[0]]
                if vbucket:
                    for i in range(1, len(vbucket)):
                        if vbucket[i] != -1:
                            vbucketInfo.replica.append(serverList[vbucket[i]])
                vbucketInfo.id = counter
                counter += 1
                bucket.vbuckets.append(vbucketInfo)
                # now go through each vbucket and populate the info
            # who is master , who is replica
        # get the 'storageTotals'
        log.debug('read {0} vbuckets'.format(len(bucket.vbuckets)))
        stats = parsed['basicStats']
        # vBucketServerMap
        bucketStats = BucketStats()
        log.debug('stats:{0}'.format(stats))
        bucketStats.opsPerSec = stats['opsPerSec']
        bucketStats.itemCount = stats['itemCount']
        if bucket.type != "memcached":
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
            if 'clusterCompatibility' in nodeDictionary:
                node.clusterCompatibility = nodeDictionary['clusterCompatibility']
            if 'clusterMembership' in nodeDictionary:
                node.clusterCompatibility = nodeDictionary['clusterMembership']
            node.version = nodeDictionary['version']
            node.os = nodeDictionary['os']
            if "ports" in nodeDictionary:
                ports = nodeDictionary["ports"]
                if "proxy" in ports:
                    node.moxi = ports["proxy"]
                if "direct" in ports:
                    node.memcached = ports["direct"]
            if "hostname" in nodeDictionary:
                value = str(nodeDictionary["hostname"])
                node.ip = value[:value.rfind(":")]
                node.port = int(value[value.rfind(":") + 1:])
            if "otpNode" in nodeDictionary:
                node.id = nodeDictionary["otpNode"]
            bucket.nodes.append(node)
        return bucket
