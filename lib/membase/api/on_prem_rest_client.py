import base64
import json
import urllib.request, urllib.parse, urllib.error

import requests
from urllib3._collections import HTTPHeaderDict

from . import httplib2
import logger
from table_view import TableView
import traceback
import socket
import time
import re
import uuid
from copy import deepcopy
from threading import Thread
from TestInput import TestInputSingleton
from testconstants import MIN_KV_QUOTA, INDEX_QUOTA, FTS_QUOTA, CBAS_QUOTA
from testconstants import IS_CONTAINER, CLUSTER_QUOTA_RATIO
from decorator import decorator
import pprint

from lib.Cb_constants.CBServer import CbServer

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

    @staticmethod
    def humanBytes(b):
        """Return the given bytes as a human friendly
        KB, MB, GB, or TB string"""

        B = float(b)
        KB = float(1024)
        MB = float(KB ** 2)  # 1,048,576
        GB = float(KB ** 3)  # 1,073,741,824
        TB = float(KB ** 4)  # 1,099,511,627,776

        if B < KB:
            return '{0} {1}'.format(B, 'Bytes' if 0 == B > 1 else 'Byte')
        elif KB <= B < MB:
            return '{0:.2f} KiB'.format(B / KB)
        elif MB <= B < GB:
            return '{0:.2f} MiB'.format(B / MB)
        elif GB <= B < TB:
            return '{0:.2f} GiB'.format(B / GB)
        elif TB <= B:
            return '{0:.2f} TiB'.format(B / TB)

    def is_ns_server_running(self, timeout_in_seconds=360):
        log.info("-->is_ns_server_running?")
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
        while progress != -1 and progress < percentage and retry < retry_count:
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


@decorator
def not_for_capella(method, *args, **kwargs):
    if CbServer.capella_run:
        log.warning("This is a capella_run so will not have access to run this call: " + str(method.__name__))
        return True
    else:
        return method(*args, **kwargs)

class RestConnection(object):
    DELETE = "DELETE"
    GET = "GET"
    POST = "POST"
    PUT = "PUT"

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
            port = CbServer.port

        if CbServer.use_https:
            port = CbServer.ssl_port

        if int(port) in range(9091, 9100):
            # return elastic search rest connection
            from membase.api.esrest_client import EsRestConnection
            obj = super(EsRestConnection,cls).__new__(cls)
        else:
            # default
            obj = object.__new__(cls)

        return obj

    def __init__(self, serverInfo):
        # serverInfo can be a json object/dictionary
        if isinstance(serverInfo, dict):
            self.ip = serverInfo["ip"]
            self.internal_ip = serverInfo.get("internal_ip")
            self.username = serverInfo["username"]
            self.password = serverInfo["password"]
            self.port = serverInfo["port"]
            self.index_port = CbServer.index_port
            self.fts_port = CbServer.fts_port
            self.query_port = CbServer.n1ql_port
            self.eventing_port = CbServer.eventing_port
            self.capi_port = CbServer.capi_port
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
            self.internal_ip = serverInfo.internal_ip if hasattr(serverInfo, "internal_ip") else None
            self.username = serverInfo.rest_username
            self.password = serverInfo.rest_password
            self.port = serverInfo.port
            self.hostname = ''
            self.index_port = CbServer.index_port
            self.fts_port = CbServer.fts_port
            self.query_port = CbServer.n1ql_port
            self.eventing_port = CbServer.eventing_port
            self.capi_port = CbServer.capi_port
            self.services = "kv"
            self.debug_logs = False
            if hasattr(serverInfo, "services") and serverInfo.services:
                self.services = serverInfo.services
            if hasattr(serverInfo, 'index_port') and serverInfo.index_port:
                self.index_port = serverInfo.index_port
            if hasattr(serverInfo, 'query_port') and serverInfo.query_port:
                self.query_port = serverInfo.query_port
            if hasattr(serverInfo, 'fts_port') and serverInfo.fts_port:
                self.fts_port = serverInfo.fts_port
            if hasattr(serverInfo, 'eventing_port') and serverInfo.eventing_port:
                self.eventing_port = serverInfo.eventing_port
            if hasattr(serverInfo, 'hostname') and serverInfo.hostname and\
               serverInfo.hostname.find(self.ip) == -1:
                self.hostname = serverInfo.hostname
        self.input = TestInputSingleton.input
        if self.input is not None:
            """ from watson, services param order and format:
                new_services=fts-kv-index-n1ql """
            self.services_node_init = self.input.param("new_services", None)
            self.debug_logs = self.input.param("debug-logs", False)
            self.is_elixir = self.input.param("is_elixir", False)
            self.reduce_query_logging = self.input.param("reduce_query_logging", False)
            # Adding CDC params
            self.enable_cdc = self.input.param("enable_cdc", False)
            self.history_retention_collection_default = self.input.param("history_retention_collection_default", 'true')
            self.history_retention_bytes = self.input.param("history_retention_bytes", 4294967296)  # 4 GB
            self.history_retention_secs = self.input.param("history_retention_secs", 86400)  # 1 day
            self.magma_key_tree_data_block_size = self.input.param("magma_key_tree_data_block_size", 10096)
            self.magma_seq_tree_data_block_size = self.input.param("magma_seq_tree_data_block_size", 131072)
            # Adding bucket weight and width params
            self.bucket_width = self.input.param("bucket_width", 1)
            self.bucket_weight = self.input.param("bucket_weight", 30)

        if CbServer.use_https:
            self.port = CbServer.ssl_port_map.get(str(self.port),
                                                  str(self.port))
            self.index_port = CbServer.ssl_port_map.get(str(self.index_port),
                                                        str(self.index_port))
            self.query_port = CbServer.ssl_port_map.get(str(self.query_port),
                                                        str(self.query_port))
            self.fts_port = CbServer.ssl_port_map.get(str(self.fts_port),
                                                      str(self.fts_port))
            self.eventing_port = CbServer.ssl_port_map.get(str(self.eventing_port),
                                                           str(self.eventing_port))
            self.capi_port = CbServer.ssl_port_map.get(str(self.capi_port), str(self.capi_port))
        http_url = "http://%s:%s/"
        https_url = "https://%s:%s/"
        generic_url = http_url
        if CbServer.use_https:
            generic_url = https_url
        url_host = "%s" % self.ip
        if self.hostname:
            url_host = "%s" % self.hostname
        self.baseUrl = generic_url % (url_host, self.port)
        self.fts_baseUrl = generic_url % (url_host, self.fts_port)
        self.index_baseUrl = generic_url % (url_host, self.index_port)
        self.query_baseUrl = generic_url % (url_host, self.query_port)
        self.capiBaseUrl = generic_url % (url_host, self.capi_port)
        self.eventing_baseUrl = generic_url % (url_host, self.eventing_port)

        # Initialization of CBAS related params
        self.cbas_ip = self.ip
        self.cbas_port = CbServer.cbas_port
        if hasattr(self.input, 'cbas'):
            if self.input.cbas:
                self.cbas_node = self.input.cbas
                if hasattr(self.cbas_node, 'port'):
                    self.cbas_port = self.cbas_node.port
                if hasattr(self.cbas_node, 'ip'):
                    self.cbas_ip = self.cbas_node.ip
        if CbServer.use_https:
            self.cbas_port = CbServer.ssl_cbas_port
        self.cbas_base_url = generic_url % (self.cbas_ip, self.cbas_port)
        self.cbas_base_url = self.cbas_base_url[:-1]

        # for Node is unknown to this cluster error
        for iteration in range(5):
            http_res, success = self.init_http_request(api=self.baseUrl + "pools/default")
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
        if isinstance(http_res, dict):
            http_res = self.extract_nodes_self_from_pools_default(http_res)
            if not http_res or http_res["version"][0:2] == "1." or CbServer.cluster_profile == "serverless":
                self.capiBaseUrl = self.baseUrl + "couchBase"
            else:
                for iteration in range(5):
                    if "couchApiBase" not in http_res.keys():
                        if self.is_cluster_mixed() or CbServer.capella_run:
                            self.capiBaseUrl = self.baseUrl + "couchBase"
                            return
                        time.sleep(0.2)
                        http_res, success = self.init_http_request(self.baseUrl + 'pools/default')
                        http_res = self.extract_nodes_self_from_pools_default(http_res)
                        return
                    else:
                        if CbServer.use_https:
                            self.capiBaseUrl = http_res["couchApiBaseHTTPS"]
                        else:
                            self.capiBaseUrl = http_res["couchApiBase"]
                        if self.internal_ip and "alternateAddresses" in http_res and "external" in http_res["alternateAddresses"]:
                            self.capiBaseUrl = self.capiBaseUrl.replace(self.internal_ip, http_res["alternateAddresses"]["external"]["hostname"])
                        return
                raise ServerUnavailableException("couchApiBase doesn't exist in pools/default: %s " % http_res)


    def get_https_base_url(self):
        port = CbServer.ssl_port_map.get(str(self.port),
                                  str(self.port))
        https_url = "https://%s:%s/"
        return https_url % (self.ip, port)

    def sasl_streaming_rq(self, bucket, timeout=120,
                          disable_ssl_certificate_validation=True):
        api = self.baseUrl + 'pools/default/bucketsStreaming/{0}'.format(bucket)
        if isinstance(bucket, Bucket):
            api = self.baseUrl + 'pools/default/bucketsStreaming/{0}'.format(bucket.name)
        try:
            httplib2.Http(timeout=timeout, disable_ssl_certificate_validation=disable_ssl_certificate_validation).\
                request(api, 'GET', '', headers=self._create_capi_headers())
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

    def is_cluster_mixed(self, timeout=120):
            http_res, success = self.init_http_request(self.baseUrl + 'pools/default', timeout=timeout)
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

    def init_http_request(self, api, timeout=120):
        content = None
        try:
            headers = self._create_capi_headers()
            status, content, header = self._http_request(api, 'GET', headers=headers, timeout=timeout)
            json_parsed = json.loads(content)
            if status:
                return json_parsed, True
            else:
                print("{0} with status {1}: {2}".format(api, status, json_parsed))
                return json_parsed, False
        except ValueError as e:
            if content is not None:
                print("{0}: {1}".format(api, content))
            else:
                print(e)
            return content, False

    def rename_node(self, hostname, username='Administrator', password='password'):
        params = urllib.parse.urlencode({'username': username,
                                   'password': password,
                                   'hostname': hostname})

        api = "%snode/controller/rename" % self.baseUrl
        status, content, header = self._http_request(api, 'POST', params)
        return status, content

    def active_tasks(self):
        api = self.baseUrl + "pools/default/tasks"
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
        retries = 3
        while retries:
            try:
                status, content, header = self._http_request(api, 'GET', headers=self._create_headers())
                return json.loads(content)
            except ValueError:
                time.sleep(10)
                retries -= 1
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

          status, content, header = self._http_request(api, 'PUT', str(design_doc),
                                                     headers=self._create_capi_headers())
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

    def get_index_path(self):
        node_info = self.get_nodes_self()
        data_path = node_info.storage[0].get_index_path()
        return data_path

    def get_exclude_node_value(self, timeout=120):
        api = self.index_baseUrl + 'getLocalIndexMetadata'
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            val = json_parsed['localSettings']
        return val

    def get_memcached_port(self):
        node_info = self.get_nodes_self()
        return node_info.memcached

    def get_ddoc(self, bucket, ddoc_name):
        status, json, meta = self._get_design_doc(bucket, ddoc_name)
        if not status:
            raise ReadDocumentException(ddoc_name, json)
        return json, meta

    # the same as Preview a Random Document on UI
    def get_random_key(self, bucket):
        api = self.baseUrl + 'pools/default/buckets/%s/localRandomKey' % bucket
        status, content, header = self._http_request(api, headers=self._create_capi_headers())
        json_parsed = json.loads(content)
        if not status:
            raise Exception("unable to get random document/key for bucket %s" % bucket)
        return json_parsed

    def create_scope(self, bucket, scope, params=None, num_retries=3):
        api = self.baseUrl + 'pools/default/buckets/%s/scopes' % bucket
        body = {'name': scope}
        if params:
            body.update(params)
        params = urllib.parse.urlencode(body)
        headers = self._create_headers()
        while num_retries > 0:
            status, content, header = self._http_request(api, 'POST', params=params, headers=headers)
            log.info("{0} with params: {1}".format(api, params))
            if status:
                json_parsed = json.loads(content)
                log.info("Scope created {}->{} {}".format(bucket, scope, json_parsed))
                break
            elif header["status"] == "400":
                log.info("Scope already exists. Skipping create {}->{}".format(bucket, scope))
                break
            else:
                time.sleep(10)
                num_retries -= 1
        else:
            raise Exception("Create scope failed : status:{0},content:{1}".format(status, content))
        return status

    def _create_single_collection(self, bucket, scope, collection, params=None):
        api = self.baseUrl + 'pools/default/buckets/%s/scopes/%s/collections' % (bucket, scope)
        body = {'name': collection}
        if params:
            body.update(params)
        params = urllib.parse.urlencode(body)
        headers = self._create_headers()
        status, content, header = self._http_request(api, 'POST', params=params, headers=headers)
        log.info("{0} with params: {1}".format(api, params))
        return status,content,header

    def create_collection(self, bucket, scope, collection, params=None, num_retries=3):
        if not isinstance(collection, list):
            collection = [collection]
        for c in collection:
            while num_retries > 0:
                status, content, header = self._create_single_collection(bucket, scope, c, params)
                if status:
                    json_parsed = json.loads(content)
                    log.info("Collection created {}->{}->{} manifest:{}".format(bucket, scope, c, json_parsed))
                    break
                elif header["status"] == "400":
                    log.info("Collection already exists. Skipping create {}->{}-{}".format(bucket, scope, c))
                    break
                else:
                    time.sleep(10)
                    num_retries -= 1
            else:
                raise Exception("Create collection failed : status:{0},content:{1}".format(status, content))
        return status

    def put_collection_scope_manifest(self, bucket, manifest, ensure_manifest=True):
        """ Put collection scope manifest to bulk update collection/scopes

        Args:
            ensure_manifest (bool): If set, blocks until the manifest has been applied to all nodes as
            the endpoint is asynchronous.
        """
        if isinstance(bucket, Bucket):
            bucket = bucket.name

        params, headers = json.dumps(manifest), self._create_capi_headers()
        status, content, _ = self._http_request(f"{self.baseUrl}pools/default/buckets/{bucket}/scopes", 'PUT',
                                                params=params, headers=headers)

        if ensure_manifest:
            uid = json.loads(content)['uid']
            ensure_manifest_status, manifest_content, _ = self._http_request(
                f"{self.baseUrl}pools/default/buckets/{bucket}/scopes/@ensureManifest/{uid}", 'POST',
                headers=headers)

        return status

    def get_bucket_manifest(self, bucket):
        if isinstance(bucket, Bucket):
            bucket = bucket.name
        api = '{0}{1}{2}{3}'.format(self.baseUrl, 'pools/default/buckets/', bucket, '/scopes')
        status, content, header = self._http_request(api)
        if status:
            return json.loads(content)
        else:
            raise Exception(
                "Cannot get manifest for bucket {}: status:{}, content:{}".format(bucket, status, content))

    def _parse_manifest(self, bucket, extract=None, system=False):
        try:
            manifest = self.get_bucket_manifest(bucket)
            scopes = []
            collections = []
            for scope in manifest["scopes"]:
                if not system:
                    if scope['name'] == '_system':
                        continue
                scopes.append(scope["name"])
                for collection in scope["collections"]:
                    collections.append(collection["name"])
            if extract == "scopes":
                return scopes
            elif extract == "collections":
                return collections
        except Exception as e:
            raise Exception("Cannot extract {} for bucket {} from manifest {}".format(extract, bucket, e.message))

    def get_bucket_scopes(self, bucket):
        return self._parse_manifest(bucket, "scopes")

    def get_bucket_collections(self, bucket):
        return self._parse_manifest(bucket, "collections")

    def get_scope_collections(self, bucket, scope):
        try:
            manifest = self.get_bucket_manifest(bucket)
            scope_found = False
            collections_in_scope = []
            for scopes in manifest["scopes"]:
                if scopes['name'] == '_system':
                    continue
                if scopes['name'] == scope:
                    scope_found = True
                    for collection in scopes['collections']:
                        collections_in_scope.append(collection['name'])
            if not scope_found:
                log.error("Cannot get collections for scope {} because it does not exist".format(scope))
            return collections_in_scope
        except Exception as e:
            raise Exception("Cannot get collections for bucket {}-> scope{} {}".format(bucket, scope, e.message))

    def delete_scope(self, bucket, scope):
        api = self.baseUrl + 'pools/default/buckets/%s/scopes/%s' % (bucket, scope)
        headers = self._create_headers()
        status, content, header = self._http_request(api, 'DELETE', headers=headers)
        log.info("{0}".format(api))
        return status

    def get_rest_endpoint_data(self, endpoint=None, ip=None, port=None):
        protocol = "http"
        if CbServer.use_https:
            port = CbServer.ssl_port_map.get(str(port), str(port))
            protocol = "https"
        endpoint_base_url = "{0}://{1}:{2}/".format(protocol, ip, port)
        api = str(endpoint_base_url) + str(endpoint)
        print(f'Executing GET on: {api}')
        headers = self._create_headers()
        status, content, header = self._http_request(api, 'GET', headers=headers)
        return status, content

    def delete_collection(self, bucket, scope, collection):
        api = self.baseUrl + 'pools/default/buckets/%s/scopes/%s/collections/%s' % (bucket, scope, collection)
        headers = self._create_headers()
        status, content, header = self._http_request(api, 'DELETE', headers=headers)
        return status

    def get_collection(self, bucket):
        api = self.baseUrl + 'pools/default/buckets/%s/scopes' % bucket
        headers = self._create_headers()
        status, content, header = self._http_request(api, 'GET', headers=headers)
        return status, content

    def get_collection_uid(self, bucket, scope, collection):
        try:
            manifest = self.get_bucket_manifest(bucket)
            for scopes in manifest["scopes"]:
                if scopes['name'] == scope:
                    for col in scopes['collections']:
                        if col['name'] == collection:
                            return col['uid']
            log.error("Cannot get collection uid because {0}.{1}.{2} does not exist"
                      .format(bucket, scope, collection))
        except Exception as e:
            raise Exception("Exception thrown while getting collection uid {}"
                            .format(e.message))

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
        return status, json_parsed

    def _get_design_doc(self, bucket, name):
        api = self.capiBaseUrl + '/%s/_design/%s' % (bucket, name)
        if isinstance(bucket, Bucket):
            api = self.capiBaseUrl + '/%s/_design/%s' % (bucket.name, name)

        status, content, header = self._http_request(api, headers=self._create_capi_headers())
        json_parsed = json.loads(content.decode())
        meta_parsed = ""
        if status:
            # in dp4 builds meta data is in content, not in header
            if 'X-Couchbase-Meta' in header:
                meta = header['X-Couchbase-Meta']
                meta_parsed = json.loads(meta)
            elif 'x-couchbase-meta' in header:
                meta = header['x-couchbase-meta']
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
        key = 'Authorization'
        if key in headers:
            val = headers[key]
            if val.startswith("Basic "):
              try:
                val = val.encode()
                return str("auth: " + base64.decodebytes(val[6:]).decode())
              except Exception as e:
                print(e)
        return ""

    def _http_request(self, api, method='GET', params='',
                      headers=None, timeout=120, disable_ssl_certificate_validation=True):
        if not headers:
            headers = self._create_headers()
        end_time = time.time() + timeout
        log.debug("Executing {0} request for following api {1} with Params: {2}  and Headers: {3}"\
                                                                .format(method, api, params, headers))
        count = 1
        t1 = 3
        while True:
            try:
                try:
                    if TestInputSingleton.input.param("debug.api.calls", False):
                        log.info("--->Start calling httplib2.Http({}).request({},{},{},{})".format(timeout,api,headers,method,params))
                except AttributeError:
                    pass

                response, content = httplib2.Http(timeout=timeout,
                                                  disable_ssl_certificate_validation=disable_ssl_certificate_validation).\
                    request(api, method, params, headers)

                try:
                    if TestInputSingleton.input.param("debug.api.calls", False):
                        log.info(
                            "--->End calling httplib2.Http({}).request({},{},{},{})".format(timeout, api, headers,
                                                                                              method, params))
                except AttributeError:
                    pass

                if response['status'] in ['200', '201', '202', '204']:
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
                    log.error("Giving up due to {2}! Tried {0} connect {1} times.".format(
                        api, count, e))
                    raise ServerUnavailableException(ip=self.ip)
            except (AttributeError, httplib2.ServerNotFoundError) as e:
                if count < 4:
                    log.error("ServerNotFoundError error while connecting to {0} error {1} "\
                                                                              .format(api, e))
                if time.time() > end_time:
                    log.error("Giving up due to {2}! Tried {0} connect {1} times.".\
                              format(api, count, e))
                    raise ServerUnavailableException(ip=self.ip)
            time.sleep(t1)
            count += 1
            t1 *= 2

    def urllib_request(self, api, verb='GET', params='', headers=None, timeout=100, try_count=3):
        if headers is None:
            headers = self._create_capi_headers()
        client_cert_path_tuple = None
        if CbServer.multiple_ca and CbServer.use_client_certs:
            client_cert_path_tuple=CbServer.x509.get_client_cert(int_ca_name="iclient1_clientroot")
            log.info("Using client cert auth")
            del headers['Authorization']
        if CbServer.cacert_verify:
            verify = CbServer.x509.ALL_CAs_PATH + CbServer.x509.ALL_CAs_PEM_NAME
        else:
            verify = False
        tries = 0
        if not self.reduce_query_logging:
            log.info("Making a rest request api={0} verb={1} params={2} "
                     "client_cert={3} verify={4}".format(api, verb, params, client_cert_path_tuple, verify))
        while tries < try_count:
            try:
                if verb == 'GET':
                    response = requests.get(api, params=params, headers=headers,
                                            timeout=timeout, cert=client_cert_path_tuple,
                                            verify=verify)
                elif verb == 'POST':
                    response = requests.post(api, data=params, headers=headers,
                                             timeout=timeout, cert=client_cert_path_tuple,
                                             verify=verify)
                elif verb == 'DELETE':
                    response = requests.delete(api, data=params, headers=headers,
                                               timeout=timeout, cert=client_cert_path_tuple,
                                               verify=verify)
                elif verb == "PUT":
                    response = requests.put(api, data=params, headers=headers,
                                            timeout=timeout, cert=client_cert_path_tuple,
                                            verify=verify)
                status = response.status_code
                content = response.content
                if status in [200, 201, 202]:
                    return True, content, response
                else:
                    log.error(str(content))
                    return False, content, response
            except Exception as e:
                tries = tries + 1
                if tries >= try_count:
                    raise Exception(e)
                else:
                    log.error("Trying again ...")
                    time.sleep(5)

    @not_for_capella
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

    def init_node(self, set_node_services=None):
        """ need a standalone method to initialize a node that could call
            anywhere with quota from testconstant """

        self.node_services = []
        if set_node_services is None:
            set_node_services = self.services_node_init
        if set_node_services is None and self.services == "":
            self.node_services = ["kv"]
        elif set_node_services is None and self.services != "":
            self.node_services = self.services.split(",")
        elif set_node_services is not None:
            if "-" in set_node_services:
                self.node_services = set_node_services.split("-")
            if "," in set_node_services:
                self.node_services = set_node_services.split(",")
        kv_quota = 0
        while kv_quota == 0:
            time.sleep(1)
            kv_quota = int(self.get_nodes_self().mcdMemoryReserved)
        info = self.get_nodes_self()
        kv_quota = int(info.mcdMemoryReserved * CLUSTER_QUOTA_RATIO)

        cb_version = info.version[:5]
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
        self.init_node_services(username=self.username, password=self.password,
                                services=self.node_services)
        self.init_cluster(username=self.username, password=self.password)
        return kv_quota

    @not_for_capella
    def init_node_services(self, username='Administrator', password='password', hostname='127.0.0.1', port='8091', services=None):
        if CbServer.use_https:
            port = CbServer.ssl_port_map.get(str(port), str(port))
        log.info("--> init_node_services({},{},{},{},{})".format(username,password,hostname,port,services))
        api = self.baseUrl + 'node/controller/setupServices'
        if services == None:
            log.info(" services are marked as None, will not work")
            return False

        params_dict = {'user': username,
                       'password': password,
                       'services': ",".join(services)}

        if hostname == "127.0.0.1":
            hostname = "{0}:{1}".format(hostname, port)
        params = urllib.parse.urlencode({ 'hostname': hostname,
                                    'user': username,
                                    'password': password,
                                    'services': ",".join(services)})
        log.info('node/controller/setupServices params on {0}: {1}:{2}'.format(self.ip, self.port, params))

        status, content, header = self._http_request(api, 'POST', params)
        error_message = "cannot change node services after cluster is provisioned"
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

    @not_for_capella
    def init_cluster_memoryQuota(self, username='Administrator',
                                 password='password',
                                 memoryQuota=256):
        api = self.baseUrl + 'pools/default'
        params = urllib.parse.urlencode({'memoryQuota': memoryQuota})
        log.info('pools/default params : {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        return status

    @not_for_capella
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

    @not_for_capella
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
        protocol = "http"
        if CbServer.use_https:
            protocol = "https"
        if server:
            api = "{0}://{1}:{2}/".format(protocol, server.ip, self.index_port) + 'cleanupRebalance'
        else:
            api = self.baseUrl + 'cleanupRebalance'
        status, content, _ = self._http_request(api, 'GET')
        if status:
            return content
        else:
            log.error("cleanupRebalance:{0},content:{1}".format(status, content))
            raise Exception("indexer rebalance cleanup failed")

    def list_indexer_rebalance_tokens(self, server):
        protocol = "http"
        if CbServer.use_https:
            protocol = "https"
        if server:
            api = "{0}://{1}:{2}/".format(protocol, server.ip, self.index_port) + 'listRebalanceTokens'
        else:
            api = self.baseUrl + 'listRebalanceTokens'
        print(api)
        status, content, _ = self._http_request(api, 'GET')
        if status:
            return content.decode('utf-8')
        else:
            log.error("listRebalanceTokens:{0},content:{1}".format(status, content))
            raise Exception("list rebalance tokens failed")

    def wait_until_cbas_is_ready(self, timeout):
        """ Wait until a http request can be made to the analytics service """
        timeout = time.time() + timeout

        while time.time() < timeout:
            try:
                self.execute_statement_on_cbas("SELECT 'hello' as message", None)
                return True
            except ServerUnavailableException:
                self.sleep(1, "Waiting for analytics server to be ready")

        return False

    def execute_statement_on_cbas(self, statement, mode, pretty=True,
                                  timeout=70, client_context_id=None,
                                  username=None, password=None, warnings=1):
        if not username:
            username = self.username
        if not password:
            password = self.password
        api = self.cbas_base_url + "/analytics/service"
        headers = self._create_capi_headers_with_auth(username, password)

        params = {'statement': statement, 'pretty': pretty, 'client_context_id': client_context_id, 'max-warnings': warnings}

        if mode is not None:
            params['mode'] = mode

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
            log.error("analytics/service status:{0},content:{1}".format(
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
                "analytics/admin/active_requests status:{0},content:{1}".format(
                    status, content))
            raise Exception("Analytics Admin API failed")

    def get_cluster_ceritificate(self):
        api = self.baseUrl + 'pools/default/certificate'
        status, content, _ = self._http_request(api, 'GET')
        if status:
            return content.decode("utf-8")
        else:
            log.error("pools/default/certificate status:{0},content:{1}".format(status, content))
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
                          certificate='', clientCertificate=None, clientKey=None, encryptionType=None):
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
            param_map['secureType'] = encryptionType
            if certificate:
                param_map['certificate'] = certificate
            if clientCertificate:
                param_map['clientCertificate'] = clientCertificate
                param_map['clientKey'] = clientKey
                param_map.pop("username")
                param_map.pop("password")

        params = urllib.parse.urlencode(param_map)
        log.info("with settings {0}".format(param_map))
        retries = 5
        while retries:
            status, content, _ = self._http_request(api, 'POST', params)
            # sample response :
            # [{"name":"two","uri":"/pools/default/remoteClusters/two","validateURI":"/pools/default/remoteClusters/two?just_validate=1","hostname":"127.0.0.1:9002","username":"Administrator"}]
            remoteCluster = json.loads(content)
            if status or "Duplicate cluster" in remoteCluster["_"]:
                return remoteCluster
            retries -= 1
        raise Exception("remoteCluster API '{0} remote cluster' failed".format(op))

    def add_remote_cluster(self, remoteIp, remotePort, username, password, name, demandEncryption=0, certificate=None,
                           clientCertificate=None, clientKey=None, encryptionType="full"):
        # example : password:password username:Administrator hostname:127.0.0.1:9002 name:two
        msg = "adding remote cluster hostname:{0}:{1} with username:password {2}:{3} name:{4} to source node: {5}:{6}"
        log.info(msg.format(remoteIp, remotePort, username, password, name, self.ip, self.port))
        api = self.baseUrl + 'pools/default/remoteClusters'
        return self.__remote_clusters(api, 'add', remoteIp, remotePort, username, password, name, demandEncryption,
                                      certificate, clientCertificate, clientKey, encryptionType)

    def start_connection_pre_check(self, remoteIp, remotePort, username, password, name, demandEncryption=0, certificate=None,
                           clientCertificate=None, clientKey=None, encryptionType="full"):
        msg = "connection pre check for :{0}:{1} with username:password {2}:{3} name:{4} to source node: {5}:{6} started"
        log.info(msg.format(remoteIp, remotePort, username, password, name, self.ip, self.port))
        api = self.baseUrl + 'xdcr/connectionPreCheck'
        return self.__remote_clusters(api, 'connection pre check', remoteIp, remotePort, username, password, name, demandEncryption,
                                      certificate, clientCertificate, clientKey, encryptionType)

    def connection_pre_check_status(self, username, password, taskId):
        precheckStatus = {}
        msg = "connection pre check status for :{0}:{1} with username:password {2}:{3} and taskId: {4}"
        log.info(msg.format(self.ip, self.port, username, password, taskId))
        api = self.baseUrl + 'xdcr/connectionPreCheck?taskId=' + taskId
        params = urllib.parse.urlencode({})
        status, content, _ = self._http_request(api, 'GET', params)
        if status:
            precheckStatus = json.loads(content)
            log.info(str(precheckStatus))
        return precheckStatus

    def modify_remote_cluster(self, remoteIp, remotePort, username, password, name, demandEncryption=0, certificate=None,
                              clientCertificate=None, clientKey=None, encryptionType="full"):
        log.info("modifying remote cluster name:{0}".format(name))
        api = self.baseUrl + 'pools/default/remoteClusters/' + urllib.parse.quote(name)
        return self.__remote_clusters(api, 'modify', remoteIp, remotePort, username, password, name, demandEncryption,
                                      certificate, clientCertificate, clientKey, encryptionType)

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
        retries = 3
        while retries:
            try:
                status, content, header = self._http_request(api, 'POST', params)
                # response : {"id": "replication_id"}
                json_parsed = json.loads(content)
                log.info("Replication created with id: {0}".format(json_parsed['id']))
                return json_parsed['id']
            except ValueError:
                time.sleep(10)
                retries -= 1
            except:
                raise Exception("create replication failed: status:{0},content:{1}".format(status, content))

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
        retries = 3
        while retries:
            status, content, header = self._http_request(api, 'DELETE')
            if status:
                log.info("Replication deleted successfully")
                return
            else:
                retries -= 1
                time.sleep(10)
        raise Exception("delete replication failed: status:{0}, content:{1}".format(status, content))

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

        protocol = "http"
        if (float(self.get_major_version()) >= 6.5 or float(self.get_major_version()) == 0.0) \
                and self.is_enterprise_edition():
            port = CbServer.ssl_port_map.get(str(port), str(port))
            protocol = "https"

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

        params = urllib.parse.urlencode({'hostname': "{0}://{1}:{2}".format(protocol, remoteIp, port),
                                   'user': user,
                                   'password': password})
        if services != None:
            services = ','.join(services)
            params = urllib.parse.urlencode({'hostname': "{0}://{1}:{2}".format(protocol, remoteIp, port),
                                   'user': user,
                                   'password': password,
                                   'services': services})
        if self.monitorRebalance():
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
        else:
            raise AddNodeException(nodeIp=self.ip,
                                   remoteIp=remoteIp,
                                   reason="Rebalance error, cannot add node")
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
        if CbServer.use_https:
            port = CbServer.ssl_port
        log.info('adding remote node @{0}:{1} to this cluster @{2}:{3}'\
                          .format(remoteIp, port, self.ip, self.port))
        api = self.baseUrl + 'node/controller/doJoinCluster'
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
        if self.internal_ip:
            self.set_alternate_address(self.ip)

    """ when we do reset couchbase server by force reject, couchbase server will not
        down right away but delay few seconds to be down depend on server spec.
        This fx will detect that delay and return true when couchbase server down and
        up again after force reject """
    def check_delay_restart_coucbase_server(self,disable_ssl_certificate_validation=True):
        api = self.baseUrl + 'nodes/self'
        headers = self._create_headers()
        break_out = 0
        count_cbserver_up = 0
        while break_out < 60 and count_cbserver_up < 2:
            try:
                response, content = httplib2.Http(timeout=120,disable_ssl_certificate_validation=disable_ssl_certificate_validation\
                                                                                                ).request(api, 'GET', '', headers)
                if response['status'] in ['200', '201', '202'] and count_cbserver_up == 0:
                    log.info("couchbase server is up but down soon.")
                    time.sleep(1)
                    break_out += 1  # time needed for couchbase server reload after reset config
                    if break_out == 7:
                        log.info("couchbase server may be up already")
                        count_cbserver_up = 1
                elif response['status'] in ['200', '201', '202']:
                    count_cbserver_up = 2
                    log.info("couchbase server is up again in few seconds")
                    time.sleep(7)
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
        if content:
            try:
                content = content.decode('utf-8')
            except (UnicodeDecodeError, AttributeError):
                pass
        if print_log:
            log.info("diag/eval status on {0}:{1}: {2} content: {3} command: {4}".
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

    def change_flusher_total_batch_limit(self, flusher_total_batch_limit=3,
                                           bucket='default'):
        code = "ns_bucket:update_bucket_props(\"" + bucket \
               + "\", [{extra_config_string, " \
               + "\"flusher_total_batch_limit=" \
               + str(flusher_total_batch_limit) + "\"}])."
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

    def _rebalance_progress_status(self, node_url=None):
        api = self.baseUrl + "pools/default/rebalanceProgress"
        if node_url:
            api = "https://" + node_url + ":18091/pools/default/rebalanceProgress"
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

    def set_gsi_tier_limit(self, bucket='test_bucket', scope='_default', limit=1):
        """
        This method sets the upper limit for GSI indexes in Free-Tier
        :param limit:
        """
        setting_json = f'name={scope}&limits={{"index": {{"num_indexes":{limit} }}}}'

        api = self.baseUrl + f'pools/default/buckets/{bucket}/scopes'
        status, content, header = self._http_request(api, 'POST', setting_json)
        if not status:
            raise Exception(content)
        log.info("{0} set".format(setting_json))

    def set_fts_tier_limit(self, bucket='test_bucket', scope='_default', limit=1):
        """
        This method sets the upper limit for GSI indexes in Free-Tier
        :param limit:
        """
        setting_json = f'name={scope}&limits={{"fts": {{"num_fts_indexes":{limit} }}}}'

        api = self.baseUrl + f'pools/default/buckets/{bucket}/scopes'
        status, content, header = self._http_request(api, 'POST', setting_json)
        if not status:
            raise Exception(content)
        log.info("{0} set".format(setting_json))

    @not_for_capella
    def set_index_settings(self, setting_json, timeout=120):
        api = self.index_baseUrl + 'settings'
        status, content, header = self.urllib_request(api, verb='POST', params=json.dumps(setting_json))
        if not status:
            raise Exception(content)
        log.info("{0} set".format(setting_json))

    @not_for_capella
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

    def set_shard_affinity_provisoned_mode(self, setting_json, timeout=120):
        '''
        Works only in mixed mode
        '''
        api = self.index_baseUrl + 'settings/shardAffinity'
        status, content, header = self._http_request(api, 'POST', json.dumps(setting_json))
        if not status:
            raise Exception(content)
        log.info("{0} set".format(setting_json))


    def get_index_settings(self, timeout=120):
        node = None
        api = self.index_baseUrl + 'settings'
        status, content, header = self.urllib_request(api, timeout=timeout)
        if not status:
            raise Exception(content)
        return json.loads(content)

    def get_index_storage_mode(self, timeout=120):
        api = self.index_baseUrl + 'settings'
        status, content, header = self.urllib_request(api, timeout=timeout)
        if not status:
            raise Exception(content)
        return json.loads(content)["indexer.settings.storage_mode"]

    def set_index_planner_settings(self, setting, timeout=120):
        api = self.index_baseUrl + 'settings/planner?{0}'.format(setting)
        status, content, header = self._http_request(api, timeout=timeout)
        if not status:
            raise Exception(content)
        return json.loads(content)

    def get_index_defrag_output(self):
        api = self.baseUrl + 'pools/default/services/index/defragmented'
        status, content, header = self._http_request(api)
        if status:
            json_parsed = json.loads(content)
            return json_parsed
        raise Exception("API defrag didn't return result.")

    def get_fts_defrag_output(self, node, creds):
        api = "https://%s:%s/pools/default/services/fts/defragmented" % (node, CbServer.ssl_port)
        r = requests.get(api, auth=(creds['username'], creds['password']), verify=False)
        r.raise_for_status()
        return r

    def get_index_stats(self, timeout=120, index_map=None, return_system_query_scope=False):
        api = self.index_baseUrl + 'stats?async=false'
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            index_map = RestParser().parse_index_stats_response(json_parsed, index_map=index_map,
                                                                return_system_query_scope=return_system_query_scope)
        return index_map

    def get_all_index_stats(self, timeout=120, inst_id_filter=[], consumer_filter=None, text=False, system=False):
        """return: json object or text response of :9102/stats"""
        api = self.index_baseUrl + 'stats'
        all_index_stats = {}
        if inst_id_filter:
            inst_id_filter = json.dumps(inst_id_filter)
        elif consumer_filter:
            api += f"?consumerFilter={consumer_filter}"
        else:
            inst_id_filter = ""
        status, content, _ = self._http_request(api, timeout=timeout, params=inst_id_filter)
        if status:
            if text:
                all_index_stats = content.decode("utf8").replace('":', '": ').replace(",", ", ")
            else:
                all_index_stats = json.loads(content)
        if not system and not text:
            new_stats_dict = {}
            for key in all_index_stats:
                if '_system:_query' in key:
                    continue
                new_stats_dict[key] = all_index_stats[key]
            return new_stats_dict
        return all_index_stats

    def get_index_official_stats(self, timeout=120, index_map=None, bucket="", scope="",
                                 collection="", system_scope=False):
        api = self.index_baseUrl + 'api/v1/stats'
        if bucket:
            api += f'/`{bucket.replace("%", "%25")}`'
            if scope:
                api += f'.{scope}'
                if collection:
                    api += f'.{collection}'
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
        if not system_scope:
            json_parsed_new = {}
            for item in json_parsed:
                if '_system:_query' not in item:
                    json_parsed_new[item] = json_parsed[item]
            return json_parsed_new
        return json_parsed

    def get_indexes_count(self):
        indexes_count = {}
        index_map = self.get_index_storage_stats()
        for bucket, indexes in index_map.items():
            for index, stats in indexes.items():
                indexes_count[index] = stats["MainStore"]["count"]

        return indexes_count

    def get_metadata_tokens(self):
        api = self.index_baseUrl + 'listMetadataTokens'
        status, content, header = self._http_request(api)
        if status:
            content = content.decode("utf8")
            content = content.strip()
            try:
                metadata_list = content.split('\n')
                metadata_dict = {}
                for item in metadata_list:
                    key, value = item.split('-')
                    metadata_dict[key.strip()] = value.strip()
                return metadata_dict
            except Exception as err:
                raise err

    def get_index_storage_stats(self, timeout=120, index_map=None):
        api = self.index_baseUrl + 'stats/storage'
        status, content, header = self.urllib_request(api, timeout=timeout)
        if not status:
            raise Exception(content)
        json_parsed = json.loads(content)
        index_storage_stats = {}
        for index_stats in json_parsed:
            bucket = index_stats["Index"].split(":")[0]
            index_name = index_stats["Index"].split(":")[-1]
            if bucket not in list(index_storage_stats.keys()):
                index_storage_stats[bucket] = {}
            index_storage_stats[bucket][index_name] = index_stats["Stats"]
        return index_storage_stats

    def get_indexer_stats(self, timeout=120, index_map=None, baseUrl=None):
        if baseUrl is None:
            api = self.index_baseUrl + 'stats'
        else:
            api = baseUrl + 'stats'
        index_map = {}
        status, content, header = self.urllib_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            for key in list(json_parsed.keys()):
                tokens = key.split(":")
                val = json_parsed[key]
                if len(tokens) == 1:
                    field = tokens[0]
                    index_map[field] = val
                if len(tokens) == 3:
                    if tokens[0] not in index_map:
                        index_map[tokens[0]] = dict()
                    if tokens[1] not in index_map[tokens[0]]:
                        index_map[tokens[0]][tokens[1]] = dict()
                    index_map[tokens[0]][tokens[1]][tokens[2]] = val
        return index_map

    def get_indexer_metadata(self, timeout=120, index_map=None, return_system_query_scope=False):
        api = self.index_baseUrl + 'getIndexStatus'
        index_map = {}
        status, content, header = self.urllib_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            for key in list(json_parsed.keys()):
                tokens = key.split(":")
                val = json_parsed[key]
                if len(tokens) == 1:
                    field = tokens[0]
                    index_map[field] = val
        if not return_system_query_scope and 'status' in index_map:
            index_map_new = {'code': index_map['code'], 'status': []}
            for item in index_map['status']:
                if item['scope'] != '_system' and item['collection'] != '_query':
                    index_map_new['status'].append(item)
            return index_map_new
        else:
            return index_map

    def get_index_aggregate_metadata(self):
        api = self.index_baseUrl + 'getIndexMetadata'
        status, content, header = self.urllib_request(api)
        if status:
            json_resp = json.loads(content)
            return json_resp

    def get_index_local_metadata(self):
        api = self.index_baseUrl + 'getLocalIndexMetadata'
        status, content, header = self.urllib_request(api)
        if status:
            json_resp = json.loads(content)
            return json_resp

    def get_indexer_internal_stats(self, timeout=120, index_map=None):
        api = self.index_baseUrl + 'settings?internal=ok'
        index_map = {}
        status, content, header = self.urllib_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            for key in list(json_parsed.keys()):
                tokens = key.split(":")
                val = json_parsed[key]
                if len(tokens) == 1:
                    field = tokens[0]
                    index_map[field] = val
        return index_map

    def trigger_compaction(self, timeout=120):
        api = self.index_baseUrl + 'plasmaDiag'
        command = {'Cmd': 'listDBs'}
        status, content, header = self._http_request(api, 'POST', json.dumps(command), timeout=timeout)
        for l in list(iter(str(content, 'utf-8').splitlines())):
            try:
                x, id = l.split(" : ")
                if id:
                    log.info(f'Triggering compaction for instance id {id}')
                    compact_command = {'Cmd': 'compactAll', 'Args': [id]}
                    status, content, header = self._http_request(api, 'POST', json.dumps(compact_command))
                    if not status:
                        log.error(f'Failed to trigger compaction : {content}')
            except ValueError:
                pass

    def trigger_eviction(self, timeout=120):
        api = self.index_baseUrl + 'plasmaDiag'
        command = {'Cmd': 'listDBs'}
        status, content, header = self._http_request(api, 'POST', json.dumps(command), timeout=timeout)
        for l in list(iter(str(content, 'utf-8').splitlines())):
            try:
                x, id = l.split(" : ")
                if id:
                    log.info(f'Triggering eviction for instance id {id}')
                    compact_command = {'Cmd': 'evictAll', 'Args': [id]}
                    status, content, header = self._http_request(api, 'POST', json.dumps(compact_command))
                    if not status:
                        log.error(f'Failed to trigger eviction : {content}')
            except ValueError:
                pass


    def get_index_status(self, timeout=120, index_map=None, return_system_query_scope=False):
        api = self.baseUrl + 'indexStatus'
        index_map = {}
        status, content, header = self.urllib_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            index_map = RestParser().parse_index_status_response(json_parsed,
                                                                 return_system_query_scope=return_system_query_scope)
        return index_map

    def get_index_id_map(self, timeout=120):
        api = self.baseUrl + 'indexStatus'
        index_map = {}
        status, content, header = self._http_request(api, timeout=timeout)
        if status:
            json_parsed = json.loads(content)
            for map in json_parsed["indexes"]:
                bucket_name = map['bucket']
                if bucket_name not in list(index_map.keys()):
                    index_map[bucket_name] = {}
                index_name = map['index']
                index_map[bucket_name][index_name] = {}
                index_map[bucket_name][index_name]['id'] = map['id']
        return index_map

    def get_index_statements(self, timeout=120, return_system_query_scope=False):
        api = self.index_baseUrl + 'getIndexStatement'
        status, content, header = self.urllib_request(api, timeout=timeout)
        json_parsed = []
        if status:
            json_parsed = json.loads(content)
            if not return_system_query_scope:
                json_parsed_final = []
                if json_parsed:
                    for statement in json_parsed:
                        if "`_system`.`_query`" in statement:
                            continue
                        json_parsed_final.append(statement)
                return json_parsed_final
        return json_parsed

    # returns node data for this host
    def get_nodes_self(self, timeout=120):
        if CbServer.capella_run:
            return self.get_limited_nodes_self(timeout)
        else:
            node = None
            api = self.baseUrl + 'nodes/self'
            status, content, header = self._http_request(api, timeout=timeout)
            if status:
                json_parsed = json.loads(content)
                node = RestParser().parse_get_nodes_response(json_parsed)
            return node

    # only admin users can call /nodes/self but almost always the
    # data we need can be found in /pools/default
    def get_limited_nodes_self(self, timeout=120):
        node = None
        pools_default = self.get_pools_default(timeout=timeout)
        if "nodes" in pools_default:
            node = self.extract_nodes_self_from_pools_default(pools_default)
            if node:
                node = RestParser().parse_get_nodes_response(node)
        return node

    def extract_nodes_self_from_pools_default(self, pools_default):
        return next(filter(lambda node: "thisNode" in node and node["thisNode"], pools_default["nodes"]), None)

    def get_ip_from_ini_file(self):
        """ in alternate address, we need to get hostname from ini file """
        return self.ip

    def set_alternate_address(self, alternate_address, alternate_ports={}, network_type="external"):
        api = self.baseUrl + 'node/controller/setupAlternateAddresses/' + network_type
        params_dict = {}
        params_dict["hostname"] = alternate_address
        for service, port in alternate_ports.items():
            params_dict[service] = port
        params = urllib.parse.urlencode(params_dict)
        status, _, _ = self._http_request(api, 'PUT', params)
        if not status:
            raise Exception("Couldn't set alternate address")


    def node_statuses(self, timeout=120):

        id_to_ip_map = {}
        real_nodes = self.get_nodes()
        for node in real_nodes:
            id_to_ip_map[node.id] = node.ip

        nodes = []
        api = self.baseUrl + 'nodeStatuses'
        status, content, header = self._http_request(api, timeout=timeout)
        json_parsed = json.loads(content)
        if status:
            for key in json_parsed:
                # each key contain node info
                value = json_parsed[key]
                # Create an OtpNode object given the id and status.
                # Note the OtpNode object grabs the ip address from the id.
                node = OtpNode(id=value['otpNode'],
                               status=value['status'])
                if node.ip == 'cb.local':
                    node.ip = self.ip
                    node.id = node.id.replace('cb.local',
                                              self.ip.__str__())
                # The ip address grabbed from the id is '127.0.0.1' or '::1'
                # when the node is not part of a cluster.  This can be amended
                # to the ip address in the TestInputServer object that is
                # provided.
                if node.ip in ['127.0.0.1', '[::1]']:
                    node.ip = self.ip

                if node.id in id_to_ip_map:
                    node.ip = id_to_ip_map[node.id]

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

    def get_orchestrator(self, timeout=30):
        api = self.baseUrl + 'pools/default/terseClusterInfo'
        status, content, header = self._http_request(api, timeout=timeout)
        json_parsed = json.loads(content)
        orchestrator = json_parsed['orchestrator'].split('@')[-1]
        return orchestrator

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

    def get_buckets(self, num_retries=3, poll_interval=15):
        buckets = []
        api = '{0}{1}'.format(self.baseUrl, 'pools/default/buckets?basic_stats=true')
        buckets_are_received = False
        status = ""
        content = ""
        while num_retries > 0:
            try:
                # get all the buckets
                status, content, header = self._http_request(api)
                json_parsed = json.loads(content)
                if status:
                    for item in json_parsed:
                        bucketInfo = RestParser().parse_get_bucket_json(item)
                        buckets.append(bucketInfo)
                    buckets_are_received = True
                    break
                else:
                    log.error("Response status is: False, response content is: {0}".format(content))
                    num_retries -= 1
                    time.sleep(poll_interval)
            except Exception as e:
                num_retries -= 1
                log.error(e)
                log.error('{0} seconds sleep before calling get_buckets again...'.format(poll_interval))
                time.sleep(poll_interval)

        if not buckets_are_received:
            log.error("Could not get buckets list from the following api: {0}".format(api))
            log.error("Last response status is: {0}".format(status))
            log.error("Last response content is: {0}".format(content))

        return buckets

    def get_bucket_by_name(self,bucket_name):
        # get all the buckets
        buckets = []
        api = '{0}{1}'.format(self.baseUrl, 'pools/default/buckets?basic_stats=true')
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        if status:
            for item in json_parsed:
                bucketInfo = RestParser().parse_get_bucket_json(item)
                if bucketInfo.name == bucket_name:
                    buckets.append(bucketInfo)
        return buckets

    def get_bucket_by_name_directly(self,bucket_name):
        api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/', bucket_name)
        status, content, header = self._http_request(api)
        json_parsed = [json.loads(content)]
        if status:
            for item in json_parsed:
                bucketInfo = RestParser().parse_get_bucket_json(item)
                if bucketInfo.name == bucket_name:
                    return bucketInfo
        return None

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
        return bucket_map

    def print_on_prem_bucket_stats(self):
        table = TableView(log.info)
        buckets = self.get_buckets()
        if len(buckets) == 0:
            table.add_row(["No buckets"])
        else:
            table.set_headers(["Bucket", "Type", "Storage", "Replicas",
                               "Durability", "TTL", "Items", "Vbuckets",
                               "RAM Quota", "RAM Used", "Disk Used", "ARR"])
            if CbServer.cluster_profile == "serverless":
                table.headers += ["Width/Weight"]
            for bucket in buckets:
                num_vbuckets = resident_ratio = storage_backend = "-"
                if bucket.type == "membase":
                    storage_backend = bucket.bucket_storage
                    try:
                        resident_ratio = self.fetch_bucket_stats(bucket.name)[
                            "op"]["samples"]["vb_active_resident_items_ratio"][-1]
                    except KeyError:
                        resident_ratio = 100
                    num_vbuckets = str(bucket.numVBuckets)
                bucket_data = [
                    bucket.name, bucket.type, storage_backend,
                    bucket.numReplicas, bucket.durability_level,
                    bucket.maxttl, bucket.stats.itemCount, num_vbuckets,
                    RestHelper.humanBytes(str(bucket.stats.ram)),
                    RestHelper.humanBytes(str(bucket.stats.memUsed)),
                    RestHelper.humanBytes(str(bucket.stats.diskUsed)),
                    resident_ratio]
                table.add_row(bucket_data)
        table.display("Bucket statistics")

    def get_bucket_stats_for_node(self, bucket='default', node=None):
        if not node:
            log.error('node_ip not specified')
            return None
        stats = {}
        api = "{0}{1}{2}{3}{4}:{5}{6}".format(self.baseUrl, 'pools/default/buckets/',
                                     bucket, "/nodes/", node.cluster_ip, node.port, "/stats")
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

    def get_node_settings(self, setting_name=None):
        api = "{0}{1}".format(self.fts_baseUrl, 'api/manager')
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        options_vals = json_parsed['mgr']['options']
        if setting_name in options_vals.keys():
            return options_vals[setting_name]
        log.error("Setting {0} not available".format(setting_name))

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
        ret_val = -1
        retries = 10
        while retries > 0:
            try:
                ret_val = bucket_stats['op']['samples']['curr_items'][-1]
                return ret_val
            except KeyError as err:
                log.error(f"get_active_key_count() function for bucket {bucket} reported an error {err}")
                log.error(f"Corresponding bucket stats JSON is {bucket_stats}")
                time.sleep(2)
                retries = retries - 1
        return ret_val

    def get_replica_key_count(self, bucket):
        """Fetch bucket stats and return the bucket's replica count"""
        bucket_stats = self.fetch_bucket_stats(bucket)
        return bucket_stats['op']['samples']['vb_replica_curr_items'][-1]

    def get_nodes(self, get_all_nodes=False):
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
                    if get_all_nodes or node.clusterMembership == 'active':
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

    def get_major_version(self):
        """ Returns the major version of the node (e.g. 6.5) """
        return self.get_nodes_self().major_version

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

    def get_fts_stats(self, index_name=None, bucket_name=None, stat_name=None):
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
            if bucket_name is None and index_name is None and stat_name is None:
                return status, content
            if bucket_name is None and index_name is None:
                key = stat_name
            else:
                key = bucket_name+':'+index_name+':'+stat_name
            if key in json_parsed:
                return status, json_parsed[key]
            attempts += 1
            log.info("Stat {0} not available yet".format(stat_name))
            time.sleep(1)
        log.error("ERROR: Stat {0} error on {1} on bucket {2}".
                  format(stat_name, index_name, bucket_name))
    def get_specific_nsstats(self, node, creds):
        try:
            endpoint = f"https://{node}:18094/api/nsstats"
            r = requests.get(endpoint, auth=(creds['username'], creds['password']), verify=False, timeout=300)
            r.raise_for_status()
            return r
        except Exception as err:
            log.error(f"Failed to fetch nsstats, reason : {str(err)}")
            return {}

    def get_fts_cfg_stats(self, node, creds):
        try:
            endpoint = f"https://{node}:18094/api/cfg"
            resp = requests.get(endpoint, auth=(creds['username'], creds['password']), verify=False, timeout=300)
            resp.raise_for_status()
            return resp
        except Exception as err:
            log.error(f"Failed to fetch FTS CFG Stats, reason : {str(err)}")
            return None

    def start_fts_index_compaction(self, index_name):
        api = "{0}{1}".format(self.fts_baseUrl, f'api/index/{index_name}/tasks')
        params = {"op": "merge"}
        status, content, header = self._http_request(api,
                                    method='POST',
                                    params=json.dumps(params, ensure_ascii=False),
                                    headers=self._create_capi_headers(),
                                    timeout=30)
        json_parsed = json.loads(content)
        return status, json_parsed

    def get_fts_index_compactions(self, index_name):
        api = "{0}{1}".format(self.fts_baseUrl, f'api/index/{index_name}/tasks')
        params = {"op": "get"}
        status, content, header = self._http_request(api,
                                    method='POST',
                                    params=json.dumps(params, ensure_ascii=False),
                                    headers=self._create_capi_headers(),
                                    timeout=30)
        json_parsed = json.loads(content)
        return status, json_parsed

    def cancel_fts_index_compaction(self, index_name=None, uuid=None):
        api = "{0}{1}".format(self.fts_baseUrl, f'api/index/{index_name}/tasks')
        params = {"op": "cancel", "uuid": uuid}
        status, content, header = self._http_request(api,
                                    method='POST',
                                    params=json.dumps(params, ensure_ascii=False),
                                    headers=self._create_capi_headers(),
                                    timeout=30)
        json_parsed = json.loads(content)
        return status, json_parsed



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

    def delete_bucket(self, bucket='default', num_retries=3, poll_interval=5):
        api = '%s%s%s' % (self.baseUrl, 'pools/default/buckets/', bucket)
        if isinstance(bucket, Bucket):
            api = '%s%s%s' % (self.baseUrl, 'pools/default/buckets/', bucket.name)

        status = False
        while num_retries > 0:
            try:
                status, content, header = self._http_request(api, 'DELETE')
                if int(header['status']) == 500:
                    # According to http://docs.couchbase.com/couchbase-manual-2.5/cb-rest-api/#deleting-buckets
                    # the cluster will return with 500 if it failed to nuke
                    # the bucket on all of the nodes within 30 secs
                    log.warning("Bucket deletion timed out waiting for all nodes, retrying...")
                    num_retries -= 1
                    time.sleep(poll_interval)
                elif int(header['status']) == 404:
                    log.warning("Bucket does not exist..exiting bucket delete")
                    break
                else:
                    break
            except Exception as e:
                num_retries -= 1
                log.error(e)
                log.error('{0} seconds sleep before calling delete_bucket again...'.format(poll_interval))
                time.sleep(poll_interval)


        return status

    def delete_all_buckets(self):
        buckets = self.get_buckets()
        for bucket in buckets:
            if isinstance(bucket, Bucket):
                api = '%s%s%s' % (self.baseUrl, 'pools/default/buckets/', bucket.name)
                self._http_request(api, 'DELETE')

    '''Load any of the three sample buckets'''
    def load_sample(self, sample_name, poll_interval=3, max_wait_time=1200, max_error_retries=3):
        api = '{0}{1}'.format(self.baseUrl, "sampleBuckets/install")
        data = '["{0}"]'.format(sample_name)
        status, content, header = self._http_request(api, 'POST', data)
        # Allow the sample bucket to be loaded
        self.wait_until_bucket_loaded(sample_name, poll_interval, max_wait_time, max_error_retries)
        return status

    def wait_until_bucket_loaded(self, bucket_name, poll_interval=3, max_wait_time=1200, max_error_retries=3):
        max_time = time.time() + float(max_wait_time)
        is_bucket_loaded = False
        response = ""
        api = '{0}{1}'.format(self.baseUrl, "pools/default/buckets/{}".format(bucket_name))
        previous_doc_count = 0
        while time.time() < max_time and max_error_retries > 0:
            time.sleep(poll_interval)
            status, content, response = self._http_request(api, method='GET')
            data = json.loads(content)
            current_doc_count = int(data["basicStats"]["itemCount"])
            if status:
                if current_doc_count == previous_doc_count:
                    is_bucket_loaded = True
                    break
                else:
                    previous_doc_count = current_doc_count
            else:
                max_error_retries -= 1
                log.warning("Something wrong happened while getting bucket {0} items count, retrying.".format(bucket_name))
                log.warning("Server response is {0}".format(str(response)))

        if not is_bucket_loaded:
            log.error("Bucket {0} was not loaded completely")
            log.error("Last response is: {0}".format(str(response)))

    # figure out the proxy port
    def create_bucket(self, bucket='',
                      ramQuotaMB=256,
                      replicaNumber=1,
                      proxyPort=11211,
                      bucketType='membase',
                      replica_index=1,
                      threadsNumber=3,
                      flushEnabled=1,
                      evictionPolicy='fullEviction',
                      lww=False,
                      maxTTL=None,
                      compressionMode='passive',
                      storageBackend='magma'):
        api = '{0}{1}'.format(self.baseUrl, 'pools/default/buckets')
        params = urllib.parse.urlencode({})



        init_params = {'name': bucket,
                       'ramQuotaMB': ramQuotaMB,
                       'replicaNumber': replicaNumber,
                       # 'proxyPort': proxyPort,
                       'bucketType': bucketType,
                       'replicaIndex': replica_index,
                       'threadsNumber': threadsNumber,
                       'flushEnabled': flushEnabled,
                       'evictionPolicy': evictionPolicy}

        if bucketType == "memcached":
            log.info("Create memcached bucket")
            # 'replicaNumber' is not valid for memcached buckets
            init_params.pop("replicaNumber", None)

        if lww:
            init_params['conflictResolutionType'] = 'lww'

        if maxTTL:
            init_params['maxTTL'] = maxTTL

        if compressionMode and self.is_enterprise_edition():
            init_params['compressionMode'] = compressionMode

        if bucketType == 'ephemeral':
            del init_params['replicaIndex']     # does not apply to ephemeral buckets, and is even rejected

        # bucket storage is applicable only for membase bucket
        if bucketType == "membase":
            if storageBackend == "magma":
                if ramQuotaMB in range(256, 1024) and ramQuotaMB < self.get_internalSettings("magmaMinMemoryQuota"):
                    log.info("Setting magmaMinMemoryQuota to {0} MB".format(ramQuotaMB))
                    self.set_internalSetting("magmaMinMemoryQuota", ramQuotaMB)
                if self.enable_cdc:
                    log.info("Enabling history retentions settings for CDC changes")
                    init_params['historyRetentionCollectionDefault'] = self.history_retention_collection_default
                    init_params['historyRetentionBytes'] = self.history_retention_bytes
                    init_params['historyRetentionSeconds'] = self.history_retention_secs
                    init_params['magmaKeyTreeDataBlockSize'] = self.magma_seq_tree_data_block_size
                    init_params['magmaSeqTreeDataBlockSize'] = self.magma_key_tree_data_block_size
            init_params['storageBackend'] = storageBackend

            if CbServer.cluster_profile == "serverless":
                init_params['width'] = self.bucket_width
                init_params['weight'] = self.bucket_weight

        params = urllib.parse.urlencode(init_params)

        log.info("{0} with param: {1}".format(api, params))
        create_start_time = time.time()

        maxwait = 60
        for numsleep in range(maxwait):
            status, content, header = self._http_request(api, 'POST', params)
            if status:
                break
            elif (int(header['status']) == 503 and
                    '{"_":"Bucket with given name still exists"}'.encode('utf-8') in content):
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
        self.print_on_prem_bucket_stats()
        return status

    def change_bucket_props(self, bucket,
                      ramQuotaMB=None,
                      replicaNumber=None,
                      proxyPort=None,
                      replicaIndex=None,
                      flushEnabled=None,
                      timeSynchronization=None,
                      maxTTL=None,
                      compressionMode=None,
                      enableCrossClusterVersioning=None,
                      versionPruningWindowHrs=None):
        api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/', bucket)
        if isinstance(bucket, Bucket):
            api = '{0}{1}{2}'.format(self.baseUrl, 'pools/default/buckets/', bucket.name)
        params = urllib.parse.urlencode({})
        params_dict = {}
        existing_bucket = self.get_bucket_json(bucket)
        if ramQuotaMB:
            params_dict["ramQuotaMB"] = ramQuotaMB
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
        if enableCrossClusterVersioning:
            params_dict["enableCrossClusterVersioning"] = enableCrossClusterVersioning
        if versionPruningWindowHrs:
            params_dict["versionPruningWindowHrs"] = versionPruningWindowHrs

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
            settings.failoverOnDataDiskIssuesEnabled = \
                json_parsed["failoverOnDataDiskIssues"]["enabled"]
            settings.failoverOnDataDiskIssuesTimeout = \
                json_parsed["failoverOnDataDiskIssues"]["timePeriod"]
            settings.maxCount = json_parsed["maxCount"]
        return settings

    def update_autofailover_settings(self, enabled, timeout,
                                     enable_disk_failure=False,
                                     disk_timeout=120, maxCount=1):
        params_dict = dict()
        params_dict['timeout'] = timeout
        if enabled:
            params_dict['enabled'] = 'true'

        else:
            params_dict['enabled'] = 'false'
        if enable_disk_failure:
            params_dict['failoverOnDataDiskIssues[enabled]'] = 'true'
            params_dict['failoverOnDataDiskIssues[timePeriod]'] = disk_timeout
        else:
            params_dict['failoverOnDataDiskIssues[enabled]'] = 'false'
        if maxCount:
            params_dict['maxCount'] = maxCount
        params = urllib.parse.urlencode(params_dict)
        api = self.baseUrl + 'settings/autoFailover'
        log.info('settings/autoFailover params : {0}'.format(params))
        status, content, header = self._http_request(api, 'POST', params)
        if not status:
            log.warning('''failed to change autofailover_settings!
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
        api = self.baseUrl + 'controller/stopRebalance'
        status, content, header = self._http_request(api, 'POST')
        if status:
            for i in range(int(wait_timeout)):
                if self._rebalance_progress_status() == 'running':
                    log.warning("rebalance is not stopped yet after {0} sec".format(i + 1))
                    time.sleep(1)
                    status = False
                else:
                    log.info("rebalance was stopped")
                    status = True
                    break
        else:
            log.error("Rebalance is not stopped due to {0}".format(content))
        return status

    def set_data_path(self, data_path=None, index_path=None, cbas_path=None):
        end_point = '/nodes/self/controller/settings'
        api = self.baseUrl + end_point
        paths = HTTPHeaderDict()
        set_path = False
        if data_path:
            set_path = True
            paths.add('path', data_path)
        if index_path:
            set_path = True
            paths.add('index_path', index_path)
        if cbas_path:
            set_path = True
            import ast
            for cbas in ast.literal_eval(cbas_path):
                paths.add('cbas_path', cbas)
        if set_path:
            params = urllib.parse.urlencode(paths)
            log.info('%s : %s' % (end_point, params))
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

    @not_for_capella
    def update_memcached_settings(self, num_reader_threads="default",
                                  num_writer_threads="default",
                                  num_storage_threads="default"):
        api = self.baseUrl + "pools/default/settings/memcached/global"
        params = {"num_reader_threads": num_reader_threads,
                  "num_writer_threads": num_writer_threads,
                  "num_storage_threads": num_storage_threads}
        params = urllib.parse.urlencode(params)
        status, content, header = self._http_request(api, 'POST', params)
        if not status:
            raise Exception(content)
        return status

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
        status, content, header = self._http_request(api, "POST", params)
        if not status:
            raise XDCRException("Unable to set replication setting {0}={1} on bucket {2} on node {3}".
                                format(param, value, src_bucket_name, self.ip))
        else:
            log.info("Updated {0}={1} on bucket '{2}' on {3}".format(param, value, src_bucket_name, self.ip))

    def set_xdcr_params(self, src_bucket_name,
                        dest_bucket_name, param_value_map):
        replication = self.get_replication_for_buckets(src_bucket_name, dest_bucket_name)
        api = self.baseUrl[:-1] + replication['settingsURI']
        params = urllib.parse.urlencode(param_value_map)
        status, content, header = self._http_request(api, "POST", params)
        if not status:
            raise XDCRException("{0} \n Unable to set replication settings {1} on bucket {2} on node {3}".
                                format(content, param_value_map, src_bucket_name, self.ip))
        else:
            log.info("Updated {0} on bucket '{1}' on {2}".format(param_value_map, src_bucket_name, self.ip))

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
        status, content = self.diag_eval(command, print_log=False)
        if not status:
            raise Exception("Unable to get recent XDCR checkpoint information")
        repl_ckpt_list = json.loads(content)
        # a single decoding will only return checkpoint record as string
        # convert string to dict using json
        chkpt_doc_string = repl_ckpt_list['/ckpt/%s/0' % repl_id].replace('"', '\"')
        chkpt_dict = json.loads(chkpt_doc_string)
        return chkpt_dict['checkpoints'][0]

    def get_repl_stat(self, repl_id, src_bkt="default", stat="data_replicated", timestamp=None):
        repl_id = repl_id.replace('/', '%2F')
        api = self.baseUrl + "pools/default/buckets/" + src_bkt + "/stats/replications%2F" \
              + repl_id + "%2F" + stat
        if timestamp:
            api += "?haveTStamp=" + timestamp
        status, content, header = self._http_request(api)
        if not status:
            raise XDCRException("Unable to retrieve {0} stat for replication {1}".
                                format(stat, repl_id))
        repl_stat = json.loads(content)
        samples = []
        for node in self.get_nodes():
            items = repl_stat["nodeStats"]["{0}:8091".format(node.ip)]
            samples.append(items)
        return samples

    """ Start of FTS rest apis"""
    @not_for_capella
    def set_fts_ram_quota(self, value):
        """set fts ram quota"""
        api = self.baseUrl + "pools/default"
        params = urllib.parse.urlencode({"ftsMemoryQuota": value})
        headers = self._create_headers()
        status, content, _ = self.urllib_request(api, verb="POST", params=params, headers=headers)
        if status:
            log.info("SUCCESS: FTS RAM quota set to {0}mb".format(value))
        else:
            raise Exception("Error setting fts ram quota: {0}".format(content))
        return status

    def set_maxConcurrentPartitionMovesPerNode(self, value):
        api = self.fts_baseUrl + "api/managerOptions"
        params = {"maxConcurrentPartitionMovesPerNode": str(value)}
        status, content, _ = self.urllib_request(api, verb="PUT", params=json.dumps(params, ensure_ascii=False))
        if status:
            log.info("SUCCESS: FTS maxConcurrentPartitionMovesPerNode set to {0}".format(value))
        return status

    @not_for_capella
    def set_disableFileTransferRebalance(self, value):
        api = self.fts_baseUrl + "api/managerOptions"
        params = {"disableFileTransferRebalance": str(value)}
        status, content, _ = self.urllib_request(api, verb="PUT", params=json.dumps(params, ensure_ascii=False))
        if status:
            log.info("SUCCESS: FTS disableFileTransferRebalance set to {0}".format(value))
        return status

    def set_maxFeedsPerDCPAgent(self, value):
        api = self.fts_baseUrl + "api/managerOptions"
        params = {"maxFeedsPerDCPAgent": str(value)}
        status, content, _ = self.urllib_request(api, verb="PUT", params=json.dumps(params, ensure_ascii=False))
        if status:
            log.info("SUCCESS: FTS maxFeedsPerDCPAgent set to {0}".format(value))
        return status

    def set_maxDCPAgents(self, value):
        api = self.fts_baseUrl + "api/managerOptions"
        params = {"maxDCPAgents": str(value)}
        status, content, _ = self.urllib_request(api, verb="PUT", params=json.dumps(params, ensure_ascii=False))
        if status:
            log.info("SUCCESS: FTS maxDCPAgents set to {0}".format(value))
        return status

    def create_fts_index(self, index_name, params, bucket="_default", scope="_default"):
        """create or edit fts index , returns {"status":"ok"} on success"""
        api = self.fts_baseUrl + "api/index/{0}".format(index_name)
        if self.is_elixir:
            if scope is None:
                scope = "_default"
            api = self.fts_baseUrl + "api/bucket/{0}/scope/{1}/index/{2}".format(bucket, scope, index_name)
        log.info(json.dumps(params))
        status, content, header = self.urllib_request(api, verb='PUT', params=json.dumps(params, ensure_ascii=False))
        if status:
            log.info("Index {0} created".format(index_name))
        else:
            raise Exception("Error creating index: {0}".format(content))
        return status

    def update_fts_index(self, index_name, index_def, bucket="_default", scope="_default"):
        api = self.fts_baseUrl + "api/index/{0}".format(index_name)
        if self.is_elixir:
            if scope is None:
                scope = "_default"
            api = self.fts_baseUrl + "api/bucket/{0}/scope/{1}/index/{2}".format(bucket, scope, index_name)
        log.info(json.dumps(index_def, indent=3))
        status, content, header = self.urllib_request(api, verb='PUT', params=json.dumps(index_def, ensure_ascii=False))
        if status:
            log.info("Index/alias {0} updated".format(index_name))
        else:
            raise Exception("Error updating index: {0}".format(content))
        return status

    def get_fts_index_definition(self, name, bucket="_default", scope="_default"):
        """ get fts index/alias definition """
        json_parsed = {}
        api = self.fts_baseUrl + "api/index/{0}".format(name)
        if self.is_elixir:
            if scope is None:
                scope = "_default"
            api = self.fts_baseUrl + "api/bucket/{0}/scope/{1}/index/{2}".format(bucket, scope, name)
        status, content, header = self.urllib_request(
            api)
        if status:
            json_parsed = json.loads(content)
        return status, json_parsed

    def get_cfg_stats(self):
        """ get fts cfg definition """
        json_parsed = {}
        api = self.fts_baseUrl + "api/cfg"
        status, content, header = self.urllib_request(
            api)
        if status:
            json_parsed = json.loads(content)
        return status, json_parsed

    def get_all_fts_index_definition(self, bucket="_default", scope="_default"):
        """ get fts index/alias definition for elixir"""
        json_parsed = {}
        api = self.fts_baseUrl + "api/index"
        if self.is_elixir:
            if scope is None:
                scope = "_default"
            api = self.fts_baseUrl + "api/bucket/{0}/scope/{1}/index".format(bucket, scope)
        status, content, header = self.urllib_request(
            api)
        if status:
            json_parsed = json.loads(content)
        return status, json_parsed

    def get_fts_index_doc_count(self, name, bucket="_default", scope="_default"):
        """ get number of docs indexed"""
        json_parsed = {}
        api = self.fts_baseUrl + "api/index/{0}/count".format(name)
        if self.is_elixir:
            if scope is None:
                scope = "_default"
            api = self.fts_baseUrl + "api/bucket/{0}/scope/{1}/index/{2}/count".format(bucket, scope, name)
        status, content, header = self.urllib_request(api)
        if status:
            json_parsed = json.loads(content)
        return json_parsed['count']

    def get_fts_index_uuid(self, name, bucket="_default", scope="_default"):
        """ Returns uuid of index/alias """
        json_parsed = {}
        api = self.fts_baseUrl + "api/index/{0}".format(name)
        if self.is_elixir:
            if scope is None:
                scope = "_default"
            api = self.fts_baseUrl + "api/bucket/{0}/scope/{1}/index/{2}".format(bucket, scope, name)
        status, content, header = self.urllib_request(api)
        if status:
            json_parsed = json.loads(content)
        return json_parsed['indexDef']['uuid']

    def get_fts_pindex_stats(self, timeout=30):
        """ Returns uuid of index/alias """
        json_parsed = {}
        api = self.fts_baseUrl + "api/stats"
        status, content, header = self.urllib_request(api)
        if status:
            json_parsed = json.loads(content)
        return json_parsed['pindexes']

    def delete_fts_index(self, name, bucket="_default", scope="_default"):
        """ delete fts index/alias """
        api = self.fts_baseUrl + "api/index/{0}".format(name)
        if self.is_elixir:
            if scope is None:
                scope = "_default"
            api = self.fts_baseUrl + "api/bucket/{0}/scope/{1}/index/{2}".format(bucket, scope, name)
        status, content, header = self.urllib_request(
            api,
            verb='DELETE')
        return status

    def delete_fts_index_extended_output(self, name, bucket="_default", scope="_default"):
        """ delete fts index/alias """
        api = self.fts_baseUrl + "api/index/{0}".format(name)
        if self.is_elixir:
            if scope is None:
                scope = "_default"
            api = self.fts_baseUrl + "api/bucket/{0}/scope/{1}/index/{2}".format(bucket, scope, name)
        status, content, header = self.urllib_request(
            api,
            verb='DELETE')
        return status, content, header

    def stop_fts_index_update(self, name):
        """ method to stop fts index from updating"""
        api = self.fts_baseUrl + "api/index/{0}/ingestControl/pause".format(name)
        log.info('calling api : {0}'.format(api))
        status, content, header = self.urllib_request(
            api,
            verb='POST')
        return status

    def resume_fts_index_update(self, name):
        """ method to stop fts index from updating"""
        api = self.fts_baseUrl + "api/index/{0}/ingestControl/resume".format(name)
        log.info('calling api : {0}'.format(api))
        status, content, header = self.urllib_request(
            api,
            verb='POST')
        return status

    def freeze_fts_index_partitions(self, name):
        """ method to freeze index partitions asignment"""
        api = self.fts_baseUrl+ "api/index/{0}/planFreezeControl/freeze".format(name)
        log.info('calling api : {0}'.format(api))
        status, content, header = self.urllib_request(
            api,
            verb='POST')
        return status

    def set_bleve_max_result_window(self, bmrw_value):
        """create or edit fts index , returns {"status":"ok"} on success"""
        api = self.fts_baseUrl + "api/managerOptions"
        params = {"bleveMaxResultWindow": str(bmrw_value)}
        log.info(json.dumps(params))
        status, content, header = self.urllib_request(api,
                                                     verb='PUT',
                                                     params=json.dumps(params, ensure_ascii=False))
        if status:
            log.info("Updated bleveMaxResultWindow")
        else:
            raise Exception("Error Updating bleveMaxResultWindow: {0}".format(content))
        return status

    def set_node_setting(self, setting_name, value):
        """create or edit fts index , returns {"status":"ok"} on success"""
        api = self.fts_baseUrl + "api/managerOptions"
        params = {str(setting_name): str(value)}
        log.info(json.dumps(params))
        status, content, header = self.urllib_request(api,
                                                     verb='PUT',
                                                     params=json.dumps(params, ensure_ascii=False))
        if status:
            log.info("Updated {0}".format(setting_name))
        else:
            raise Exception("Error Updating {0}: {1}".format(setting_name, content))
        return status

    def unfreeze_fts_index_partitions(self, name):
        """ method to freeze index partitions asignment"""
        api = self.fts_baseUrl+ "api/index/{0}/planFreezeControl/unfreeze".format(name)
        log.info('calling api : {0}'.format(api))
        status, content, header = self.urllib_request(
            api,
            verb='POST')
        return status

    def disable_querying_on_fts_index(self, name):
        """ method to disable querying on index"""
        api = self.fts_baseUrl + "api/index/{0}/queryControl/disallow".format(name)
        log.info('calling api : {0}'.format(api))
        status, content, header = self.urllib_request(
            api,
            verb='POST')
        return status

    def enable_querying_on_fts_index(self, name):
        """ method to enable querying on index"""
        api = self.fts_baseUrl + "api/index/{0}/queryControl/allow".format(name)
        log.info('calling api : {0}'.format(api))
        status, content, header = self.urllib_request(
            api,
            verb='POST')
        return status

    def run_fts_query(self, index_name, query_json, timeout=100, bucket="_default", scope="_default"):
        """Method run an FTS query through rest api"""
        api = self.fts_baseUrl + "api/index/{0}/query".format(index_name)
        if self.is_elixir:
            if scope is None:
                scope = "_default"
            api = self.fts_baseUrl + "api/bucket/{0}/scope/{1}/index/{2}/query".format(bucket, scope, index_name)
        headers = self._create_capi_headers()
        status, content, header = self.urllib_request(
            api,
            verb="POST",
            params=json.dumps(query_json, ensure_ascii=False).encode('utf8'),
            timeout=timeout)
        content = json.loads(content)
        if status:
            return content['total_hits'], content['hits'], content['took'], content['status']
        else:
            return -1, content['error'], -1, content['status']

    def run_fts_query_generalized(self, index_name, query_json, timeout=70):
        """Method run an FTS query through rest api"""
        api = self.fts_baseUrl + "api/index/{0}/query".format(index_name)
        headers = self._create_capi_headers()
        status, content, header = self.urllib_request(
            api,
            verb="POST",
            params=json.dumps(query_json, ensure_ascii=False).encode('utf8'))
        content = json.loads(content)
        return content

    def run_fts_query_with_facets(self, index_name, query_json):
        """Method run an FTS query through rest api"""
        api = self.fts_baseUrl + "api/index/{0}/query".format(index_name)
        status, content, header = self.urllib_request(
            api,
            verb="POST",
            params=json.dumps(query_json, ensure_ascii=False).encode('utf8'))

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

    @not_for_capella
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
        return True

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

    def get_num_rollback_stat(self, bucket):
        api = self.index_baseUrl + 'stats'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        num_rollback = json_parsed["MAINT_STREAM:{}:num_rollbacks".format(bucket)]
        return num_rollback

    def get_num_rollback_to_zero_stat(self, bucket):
        api = self.index_baseUrl + 'stats'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content)
        num_rollback = json_parsed["MAINT_STREAM:{}:num_rollbacks_to_zero".format(bucket)]
        return num_rollback

    def get_logs(self, last_n=10, contains_text=None):
        api = self.baseUrl + 'logs'
        status, content, header = self._http_request(api)
        json_parsed = json.loads(content.decode("utf-8","ignore"))
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
        http = httplib2.Http(disable_ssl_certificate_validation=True)
        n1ql_port = CbServer.n1ql_port
        protocol = "http"
        if CbServer.use_https:
            n1ql_port = str(CbServer.ssl_port_map.get(str(n1ql_port), str(n1ql_port)))
            protocol = "https"
        api = "%s://%s:%s/" % (protocol,server.ip, n1ql_port) + "admin/settings"
        body = {"completed-threshold": min_time}
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "POST", headers=headers, body=json.dumps(body))
        return response, content

    def set_completed_requests_max_entries(self, server, no_entries):
        http = httplib2.Http(disable_ssl_certificate_validation=True)
        n1ql_port = CbServer.n1ql_port
        protocol = "http"
        if CbServer.use_https:
            n1ql_port = str(CbServer.ssl_port_map.get(str(n1ql_port), str(n1ql_port)))
            protocol = "https"
        api = "%s://%s:%s/" % (protocol, server.ip, n1ql_port) + "admin/settings"
        body = {"completed-limit": no_entries}
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "POST", headers=headers, body=json.dumps(body))
        return response, content

    def set_profiling(self, server, setting):
        http = httplib2.Http(disable_ssl_certificate_validation=True)
        n1ql_port = CbServer.n1ql_port
        protocol = "http"
        if CbServer.use_https:
            n1ql_port = str(CbServer.ssl_port_map.get(str(n1ql_port), str(n1ql_port)))
            protocol = "https"
        api = "%s://%s:%s/" % (protocol, server.ip, n1ql_port) + "admin/settings"
        body = {"profile": setting}
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "POST", headers=headers, body=json.dumps(body))
        return response, content

    def set_query_servicers(self, server, setting, servicers="servicers"):
        http = httplib2.Http(disable_ssl_certificate_validation=True)
        n1ql_port = CbServer.n1ql_port
        protocol = "http"
        if CbServer.use_https:
            n1ql_port = str(CbServer.ssl_port_map.get(str(n1ql_port), str(n1ql_port)))
            protocol = "https"
        api = "%s://%s:%s/" % (protocol, server.ip, n1ql_port) + "admin/settings"
        body = {servicers: setting}
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "POST", headers=headers, body=json.dumps(body))
        return response, content

    def set_profiling_controls(self, server, setting):
        http = httplib2.Http(disable_ssl_certificate_validation=True)
        n1ql_port = CbServer.n1ql_port
        protocol = "http"
        if CbServer.use_https:
            n1ql_port = str(CbServer.ssl_port_map.get(str(n1ql_port), str(n1ql_port)))
            protocol = "https"
        api = "%s://%s:%s/" % (protocol, server.ip, n1ql_port) + "admin/settings"
        body = {"controls": setting}
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "POST", headers=headers, body=json.dumps(body))
        return response, content

    def get_query_admin_settings(self, server):
        http = httplib2.Http(disable_ssl_certificate_validation=True)
        n1ql_port = CbServer.n1ql_port
        protocol = "http"
        if CbServer.use_https:
            n1ql_port = str(CbServer.ssl_port_map.get(str(n1ql_port), str(n1ql_port)))
            protocol = "https"
        api = "%s://%s:%s/" % (protocol, server.ip, n1ql_port) + "admin/settings"
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "GET", headers=headers)
        result = json.loads(content)
        return result

    def get_query_vitals(self, server):
        http = httplib2.Http(disable_ssl_certificate_validation=True)
        n1ql_port = CbServer.n1ql_port
        protocol = "http"
        if CbServer.use_https:
            n1ql_port = str(CbServer.ssl_port_map.get(str(n1ql_port), str(n1ql_port)))
            protocol = "https"
        api = "%s://%s:%s/" % (protocol,server.ip, n1ql_port) + "admin/vitals"
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "GET", headers=headers)
        return response, content
    '''End Monitoring/Profiling Rest Calls'''

    def create_whitelist(self, server, whitelist):
        http = httplib2.Http(disable_ssl_certificate_validation=True)
        protocol = "http"
        if CbServer.use_https:
            protocol = "https"
            server.port = str(CbServer.ssl_port_map.get(str(server.port), str(server.port)))
        api = "%s://%s:%s/" % (protocol, server.ip, server.port) + "settings/querySettings/curlWhitelist"
        headers = self._create_headers_with_auth('Administrator', 'password')
        response, content = http.request(api, "POST", headers=headers, body=json.dumps(whitelist))
        return response, content

    def query_tool(self, query, port=8093, timeout=1300, query_params={}, is_prepared=False, named_prepare=None,
                   verbose = True, encoded_plan=None, servers=None):
        if timeout is None:
            timeout = 1300
        protocol = "http"
        if CbServer.use_https:
            port = str(CbServer.ssl_port_map.get(str(port), str(port)))
            protocol = "https"
        key = 'prepared' if is_prepared else 'statement'
        headers = None
        prepared = json.dumps(query)
        if is_prepared:
            if named_prepare and encoded_plan:
                http = httplib2.Http(disable_ssl_certificate_validation=True)
                if len(servers)>1:
                    url = "%s://%s:%s/query/service" % (protocol, servers[1].ip, port)
                else:
                    url = "%s://%s:%s/query/service" % (protocol, self.ip, port)

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
            api = "%s://%s:%s/query/service?%s" % (protocol, self.ip, port, params)
            log.info("%s"%api)
        else:
            params = {key : query}
            try:
              if 'creds' in query_params and query_params['creds']:
                headers = self._create_headers_with_auth(query_params['creds'][0]['user'],
                                                         query_params['creds'][0]['pass'])
                del query_params['creds']
            except Exception:
                traceback.print_exc()

            params.update(query_params)
            params = urllib.parse.urlencode(params)
            if verbose:
                log.info('query params : {0}'.format(params))
            api = "%s://%s:%s/query?%s" % (protocol, self.ip, port, params)

        if 'query_context' in query_params and query_params['query_context']:
            log.info(f"Running Query with query_context: {query_params['query_context']}")
        content = None
        try:
            status, content, header = self._http_request(api, 'POST', timeout=timeout, headers=headers)
        except Exception as ex:
            print("\nException error: ", str(ex))
            print("\napi: ", api)
            print("\nheaders: ", headers)

        try:
            return json.loads(content)
        except ValueError:
            return content

    def analytics_tool(self, query, port=8095, timeout=650, query_params={}, is_prepared=False, named_prepare=None,
                   verbose = True, encoded_plan=None, servers=None):
        protocol = "http"
        if CbServer.use_https:
            port = str(CbServer.ssl_port_map.get(str(port), str(port)))
            protocol = "https"
        key = 'prepared' if is_prepared else 'statement'
        headers = None
        content=""
        prepared = json.dumps(query)
        if is_prepared:
            if named_prepare and encoded_plan:
                http = httplib2.Http(disable_ssl_certificate_validation=True)
                if len(servers)>1:
                    url = "%s://%s:%s/query/service" % (protocol, servers[1].ip, port)
                else:
                    url = "%s://%s:%s/query/service" % (protocol, self.ip, port)

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

    def query_tool_stats(self, server):
        n1ql_port = CbServer.n1ql_port
        protocol = "http"
        if CbServer.use_https:
            n1ql_port = CbServer.ssl_n1ql_port
            protocol = "https"
        log.info('query n1ql stats')
        api = "%s://%s:%s/admin/stats" % (protocol, server.ip, str(n1ql_port))
        status, content, header = self._http_request(api, 'GET')
        log.info(content)
        try:
            return json.loads(content)
        except ValueError:
            return content

    def index_tool_stats(self, show_index_stats=True):
        log.info('index n1ql stats')
        port = CbServer.port
        protocol = "http"
        if CbServer.use_https:
            port = CbServer.ssl_port
            protocol = "https"
        api = "%s://%s:%s/indexStatus" % (protocol, self.ip, port)
        params = ""
        status, content, header = self._http_request(api, 'GET', params)
        if show_index_stats:
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

    def set_bucket_compressionMode(self, bucket="default", mode="passive"):
        api = self.baseUrl + "pools/default/buckets/" + bucket
        body = {'compressionMode': mode}
        params = urllib.parse.urlencode(body)
        headers = self._create_headers()
        status, content, header = self._http_request(api, 'POST', params=params, headers=headers)
        log.info("{0} with params: {1}".format(api, params))
        if not status:
            raise Exception("Unable to set compressionMode {0} for bucket {1}".format(mode, bucket))

    '''LDAP Rest API '''
    '''
    disableSaslauthdAuth - Function to clear saslauthdAuth settings
    Parameter - None
    Returns -
    status of saslauthdAuth clear command
    '''
    def disableSaslauthdAuth (self):
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

    '''MadHatter LDAP Group Support'''

    '''
        Assign group roles
    '''

    def add_group_role(self,group_name,description,roles,ldap_group_ref=None):
        api = self.baseUrl + "settings/rbac/groups/" + group_name
        if ldap_group_ref is not None:

            params = urllib.parse.urlencode({
                                        'description':'{0}'.format(description),
                                        'roles':'{0}'.format(roles),
                                        'ldap_group_ref':'{0}'.format(ldap_group_ref)
                                    })

        else:
            params = urllib.parse.urlencode({
                                        'description':'{0}'.format(description),
                                        'roles':'{0}'.format(roles)
                                    })
        status, content, header = self._http_request(api, 'PUT', params)
        log.info ("Status of Adding role to group command is {0}".format(status))
        return status, json.loads(content)

    def delete_group(self,group_name):
        api = self.baseUrl + "settings/rbac/groups/" + group_name
        status, content, header = self._http_request(api, 'DELETE')
        log.info ("Status of Delete role from CB is {0}".format(status))
        return status, json.loads(content)

    def get_group_list(self):
        api = self.baseUrl + "settings/rbac/groups/"
        status, content, header = self._http_request(api, 'GET')
        return status, json.loads(content)

    def get_group_details(self, group_name):
        api = self.baseUrl + "settings/rbac/groups/" + group_name
        status, content, header = self._http_request(api, 'GET')
        return status, json.loads(content)

    def add_user_group(self,group_name,user_name):
        api = self.baseUrl + "settings/rbac/users/local/" + user_name
        params = urllib.parse.urlencode({
                                    'groups':'{0}'.format(group_name)
                                })
        status, content, header = self._http_request(api, 'PUT', params)
        log.info ("Status of Adding role to group command is {0}".format(status))
        return status, json.loads(content)

    def get_user_group(self,user_name):
        api = self.baseUrl + "settings/rbac/users/local/" + user_name
        status, content, header = self._http_request(api, 'GET')
        log.info ("Status of Retrieving role from group command is {0}".format(status))
        return status, json.loads(content)

    def grp_invalidate_cache(self):
        api = self.baseUrl + "settings/invalidateLDAPCache/"
        status, content, header = self._http_request(api, 'POST')
        log.info("Status of Adding role to group command is {0}".format(status))
        return status, json.loads(content)

    def invalidate_ldap_cache(self):
        api = self.baseUrl + 'settings/invalidateLDAPCache'
        status, content, header = self._http_request(api, 'POST')
        log.info("Status of Invalidate LDAP Cached is {0}".format(status))
        return status, json.loads(content)


    def ldap_validate_conn(self):
        api = self.baseUrl + "settings/ldap/validate/connectivity"
        status, content, header = self._http_request(api, 'POST')
        log.info("Status of validate LDAP connectivity  command is {0}".format(status))
        return status, json.loads(content)

    def ldap_validate_authen(self, user_name, password='password'):
        api = self.baseUrl + "settings/ldap/validate/authentication"
        params = urllib.parse.urlencode({
            'auth_user': '{0}'.format(user_name),
            'auth_pass': '{0}'.format(password)
        })
        status, content, header = self._http_request(api, 'POST', params)
        log.info("Status of validate LDAP authetication command is {0}".format(status))
        return status, json.loads(content)

    def ldap_validate_grp_query(self, user):
        api = self.baseUrl + "settings/ldap/validate/groups_query"
        params = urllib.parse.urlencode({
                                    'groups_query_user':'{0}'.format(user)
                                })
        status, content, header = self._http_request(api, 'POST',params)
        log.info ("Status of validate group query command is {0}".format(status))
        return status, json.loads(content)

    def setup_ldap(self, data, extraparam):
        api = self.baseUrl + 'settings/ldap/'
        params = urllib.parse.urlencode(data)
        params = params + "&" + extraparam
        status, content, header = self._http_request(api, 'POST',params)
        log.info ("Status of Setting up LDAP command is {0}".format(status))
        return status, json.loads(content)

    def get_ldap_settings(self):
        api = self.baseUrl + 'settings/ldap/'
        status, content, header = self._http_request(api, 'GET')
        log.info ("Status of getting LDAP settings command is {0}".format(status))
        return status, json.loads(content)

    def disable_ldap(self):
        api = self.baseUrl + 'settings/ldap'
        params = urllib.parse.urlencode({'authenticationEnabled':'false'})
        status, content, header = self._http_request(api, 'POST', params)
        return status, content, header

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
    def setAuditSettings(self, enabled='true', rotateInterval=86400, logPath='/opt/couchbase/var/lib/couchbase/logs', services_to_disable=None):
        api = self.baseUrl + "settings/audit"

        params = {'rotateInterval':'{0}'.format(rotateInterval),
                  'auditdEnabled':'{0}'.format(enabled),
                  'logPath':'{0}'.format(logPath)}

        if services_to_disable:
            params['disabled'] = ",".join(services_to_disable)

        params = urllib.parse.urlencode(params)
        status, content, header = self._http_request(api, 'POST', params)
        log.info ("Value os status is {0}".format(status))
        log.info ("Value of content is {0}".format(content))
        if status:
            return status
        else:
            return status, json.loads(content)

    def get_audit_descriptors(self):
        api = self.baseUrl + "settings/audit/descriptors"
        status, content, header = self._http_request(api, 'GET', headers=self._create_capi_headers())
        return json.loads(content) if status else None

    def _set_secrets_password(self, new_password):
        api = self.baseUrl + "node/controller/changeMasterPassword"
        params = urllib.parse.urlencode({
            'newPassword': '{0}'.format(new_password.encode('utf-8').strip())
                                        })
        log.info("Params getting set is ---- {0}".format(params))
        params = params.replace('%24', '$')
        params = params.replace('%3D', '=')
        log.info("Params getting set is ---- {0}".format(params))
        status, content, header = self._http_request(api, 'POST', params)
        log.info("Status of set password command - {0}".format(status))
        log.info("Content of the response is {0}".format(content))
        log.info ("Header of the response is {0}".format(header))
        return status

    def get_secret_management_state(self):
        api = self.baseUrl + "nodes/self/secretsManagement/encryptionService"
        status, content, header = self._http_request(api, 'GET')
        log.info("Status of get secret management status: {}".format(status))
        log.info("Content of secret management status: {}".format(content))
        if not status:
            return status, None

        return status, json.loads(content)

    def set_encryption_setttings(self, encryption_payload):
        """
            Sets secret encryption settings:

            encryption_payload keys:
                keyEncrypted: Whether data keys should be encrypted by means of the master password.
                              The value can be either true (the default) or false.
                keyPath: Specifies whether the data keys are stored at the default or at a custom location.
                         The value can be either auto (which is the default) or custom. If the value is auto,
                         the default location is used. If the value is custom, the path provided by customKeyPath is used.
                         Note that this option is used only if keyStorageType has the value file.
                customKeyPath: A file-path that specifies a custom location at which the data keys are stored.
                               This file-path is used only if the value of keyPath is custom
                keyStorageType: The value can be file (which is the default) or script. If the value is file, the system
                                stores the data keys in a file. If the value is script, a user-specified script is called,
                                when keys are to be written, read, or deleted. These scripts must be specified by writeCmd,
                                readCmd, and deleteCmd, respectively.
                passwordSource: The value can be env (which is the default) or script. If the value is env, the master password
                                is read from the environment variable specified by passwordEnv. If the value is script,
                                the master password is provided by the script specified by passwordCmd.
                passwordEnv: The name of the environment variable from which the master password is read (provided that
                             the value of passwordSource is env). By default, the value of passwordEnv is CB_MASTER_PASSWORD.
                passwordCmd: The script to be executed for provision of the master password. This script is only executed
                             when the value of passwordSource is script.
                readCmd: The script to be executed for the reading of data keys (when the value of keyStorageType is script)
                writeCmd: The script to be executed for the writing of data keys (when the value of keyStorageType is script).
                deleteCmd: The script to be executed for the deletion of data keys (when the value of keyStorageType is script).

        """
        api = self.baseUrl + "node/controller/secretsManagement/encryptionService"
        params = json.dumps(encryption_payload)
        headers = self._create_capi_headers()
        status, content, header = self._http_request(api, 'POST', params, headers)
        log.info("Status of set secret encryption: {}".format(status))
        log.info("Content of set secret encryption: {}".format(content))
        if not status:
            return status, None

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
        url = "settings/rbac/roles"
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'GET')
        if not status:
            raise Exception(content)
        return json.loads(content)

    'Get list of current users and rols assigned to them'
    def retrieve_user_roles(self):
        url = "settings/rbac/users"
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
        url = "settings/rbac/users/local/" + user_id
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
        return authorization.decode('utf-8').replace("\n", "")

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
    Add External User
    '''
    def add_external_user(self,user_id,payload):
        url = "settings/rbac/users/external/" + user_id
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'PUT', payload)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
    Delete External User
    '''
    def delete_external_user(self,user_id):
        url = "settings/rbac/users/external/" + user_id
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'DELETE')
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
    Update user password
    '''
    def update_password(self, user_id, new_password, username="Administrator", password="password"):
        url = "settings/rbac/users/local/" + user_id
        api = self.baseUrl + url
        params = urllib.parse.urlencode({'password': new_password})
        authorization = self.get_authorization(username, password)
        headers = {'Content-type': 'application/x-www-form-urlencoded',
                   'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'PATCH', params, headers=headers)
        if not status:
            raise Exception(content)
        return content

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
           Eventing lifecycle operation
    '''

    def lifecycle_operation(self, name, operation, function_scope=None, username=None, password=None):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/functions/" + name +"/"+ operation
        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers)
        if not status:
            raise Exception(content)
        return content



    '''
        Save the Function so that it is visible in UI
    '''
    def save_function(self, name, body, function_scope=None):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/saveAppTempStore/?name=" + name
        if function_scope is not None:
            url += "&bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
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
    def deploy_function(self, name, body, function_scope=None):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/setApplication/?name=" + name
        if function_scope is not None:
            url += "&bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
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
    def get_all_functions(self, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
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
    def set_settings_for_function(self, name, body, function_scope=None, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = "api/v1/functions/" + name +"/settings"
        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))

        if not status:
            raise Exception(content)
        return content

    '''
            deploy the Function
    '''

    def deploy_function_by_name(self, name, function_scope=None, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = "api/v1/functions/" + name + "/settings"
        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        body = {"deployment_status": True, "processing_status": True}
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    '''
               pause the Function
    '''

    def pause_function_by_name(self, name, function_scope=None, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = "api/v1/functions/" + name + "/settings"
        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        body = {"deployment_status": True, "processing_status": False}
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
    def undeploy_function(self, name, function_scope=None, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = "api/v1/functions/" + name +"/settings"
        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
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
    def delete_all_function(self, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
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
    def delete_single_function(self, name, function_scope=None, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = "api/v1/functions/" + name
        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'DELETE', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Delete the Function from UI
    '''
    def delete_function_from_temp_store(self, name, function_scope=None):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/deleteAppTempStore/?name=" + name
        if function_scope is not None:
            url += "&bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'DELETE', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Delete the Function
    '''
    def delete_function(self, name, function_scope=None):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/deleteApplication/?name=" + name
        if function_scope is not None:
            url += "&bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'DELETE', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Export the Function
    '''
    def export_function(self, name, function_scope=None, username="Administrator", password="password"):
        export_map = {}
        authorization = self.get_authorization(username, password)
        url = "api/v1/export/" + name
        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
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
         Import the Function
    '''

    def import_function(self, body, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = "api/v1/import"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=body)

        if not status:
            raise Exception(content)
        return content

    '''
             Ensure that the eventing node is out of bootstrap node
    '''
    def get_deployed_eventing_apps(self, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = "getDeployedApps"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
            return list of eventing functions
    '''
    def get_list_of_eventing_functions(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/list/functions"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json',
                   'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET',
                                                     headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Ensure that the eventing node is out of bootstrap node
    '''

    def get_running_eventing_apps(self, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
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
    def get_composite_eventing_status(self, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = "api/v1/status"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if not status:
            raise Exception(content)
        return json.loads(content)

    '''
             Get Eventing processing stats
    '''
    def get_event_processing_stats(self, name, function_scope=None, eventing_map=None, username="Administrator", password="password"):
        if eventing_map is None:
            eventing_map = {}
        authorization = self.get_authorization(username, password)
        url = "getEventProcessingStats?name=" + name
        if function_scope is not None:
            url += "&bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
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
    def get_aggregate_event_processing_stats(self, name, function_scope=None, eventing_map=None, username="Administrator", password="password"):
        if eventing_map is None:
            eventing_map = {}
        authorization = self.get_authorization(username, password)
        url = "getAggEventProcessingStats?name=" + name
        if function_scope is not None:
            url += "&bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
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
    def get_event_execution_stats(self, name, function_scope=None, eventing_map=None, username="Administrator", password="password"):
        if eventing_map is None:
            eventing_map = {}
        authorization = self.get_authorization(username, password)
        url = "getExecutionStats?name=" + name
        if function_scope is not None:
            url += "&bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
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
    def get_event_failure_stats(self, name, function_scope=None, eventing_map=None, username="Administrator", password="password"):
        if eventing_map is None:
            eventing_map = {}
        authorization = self.get_authorization(username, password)
        url = "getFailureStats?name=" + name
        if function_scope is not None:
            url += "&bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
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
    def get_all_eventing_stats(self, seqs_processed=False, eventing_map=None, username="Administrator", password="password"):
        if eventing_map is None:
            eventing_map = {}
        authorization = self.get_authorization(username, password)
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
    def start_eventing_debugger(self, name, function_scope=None):
        authorization = self.get_authorization(self.username, self.password)
        url="pools/default"
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        url = "_p/event/startDebugger/?name=" + name
        if function_scope is not None:
            url += "&bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers, params=content)
        if not status:
            raise Exception(content)
        return content

    '''
            Stop debugger
    '''
    def stop_eventing_debugger(self, name, function_scope=None):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/stopDebugger/?name=" + name
        if function_scope is not None:
            url += "&bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
            Get debugger url
    '''
    def get_eventing_debugger_url(self, name, function_scope=None):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/getDebuggerUrl/?name=" + name
        if function_scope is not None:
            url += "&bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers)
        if not status:
            raise Exception(content)
        return content

    '''
         allow inter bucket recursion
    '''
    def allow_interbucket_recursion(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/api/v1/config"
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        body = "{\"allow_interbucket_recursion\": true}"
        status, content, header = self._http_request(api, 'POST', headers=headers, params=body)
        if not status:
            raise Exception(content)
        return content

    '''
             update eventing config
    '''
    def update_eventing_config(self,body):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/api/v1/config"
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers, params=body)
        if not status:
            raise Exception(content)
        return content

    '''
                GET eventing config
    '''
    def get_eventing_config(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "_p/event/api/v1/config"
        api = self.baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers, params='')
        if not status:
            raise Exception(content)
        return content

    '''
            update eventing config function wise
    '''
    def update_eventing_config_per_function(self, body, name, function_scope=None):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/functions/" + name + "/config"
        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    '''
            GET eventing config for single function
    '''
    def get_eventing_config_per_function(self, name, function_scope=None):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/functions/" + name + "/config"
        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers, params='')
        if not status:
            raise Exception(content)
        return content

    '''
            Update function appcode
    '''
    def update_function_appcode(self, body, name, function_scope=None):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/functions/" + name + "/appcode"
        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers, params=body)
        if not status:
            raise Exception(content)
        return content

    '''
            Get function appcode
    '''
    def get_function_appcode(self, name, function_scope=None):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/functions/" + name + "/appcode"
        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers, params='')
        if not status:
            raise Exception(content)
        return content

    '''
          Get eventing rebalance status
    '''
    def get_eventing_rebalance_status(self, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = "getAggRebalanceStatus"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if status:
            return content

    '''
              Get application logs
    '''
    def get_app_logs(self,handler_name, function_scope=None, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = "getAppLog?aggregate=true&name=" + handler_name
        if function_scope is not None:
            url += "&bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET', headers=headers)
        if status:
            return content

    def create_function(self, name, body, function_scope=None, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = "api/v1/functions/" + name
        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    def update_function(self, name, body, function_scope=None, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = "api/v1/functions/" + name
        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        body['appname']=name
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    def get_function_details(self, name, function_scope=None, username="Administrator", password="password"):
        authorization = self.get_authorization(username, password)
        url = "api/v1/functions/" + name
        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
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

    def set_eventing_retry(self, name, body, function_scope=None):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/functions/" + name + "/retry"
        if function_scope is not None:
            url += "?bucket={0}&scope={1}".format(function_scope["bucket"],
                                                  function_scope["scope"])
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json', 'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'POST', headers=headers,
                                                     params=json.dumps(body).encode("ascii", "ignore"))
        if not status:
            raise Exception(content)
        return content

    '''
            start tracing
    '''
    def start_tracing(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "startTracing"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json',
                   'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET',
                                                     headers=headers, params='')
        if not status:
            raise Exception(content)
        return content

    '''
            stop tracing
    '''
    def stop_tracing(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "stopTracing"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json',
                   'Authorization': 'Basic %s' % authorization}
        status, content, header = self._http_request(api, 'GET',
                                                     headers=headers, params='')
        if not status:
            raise Exception(content)
        return content

    '''
            enable curl
    '''
    def enable_curl(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/config"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json',
                   'Authorization': 'Basic %s' % authorization}
        body = '{"disable_curl": false}'
        status, content, header = self._http_request(api, 'POST',
                                                     headers=headers,
                                                     params=body)
        if not status:
            raise Exception(content)
        return content

    '''
            disable curl
    '''
    def disable_curl(self):
        authorization = self.get_authorization(self.username, self.password)
        url = "api/v1/config"
        api = self.eventing_baseUrl + url
        headers = {'Content-type': 'application/json',
                   'Authorization': 'Basic %s' % authorization}
        body = '{"disable_curl": true}'
        status, content, header = self._http_request(api, 'POST',
                                                     headers=headers,
                                                     params=body)
        if not status:
            raise Exception(content)
        return content

    def pause_bucket(self, blob_region, s3_path, bucket='default', rate_limiter=104857600):
        api = f'{self.baseUrl}controller/pause'
        params_dict = {}
        params_dict['bucket'] = bucket
        params_dict['blob_storage_region'] = blob_region
        params_dict['remote_path'] = s3_path
        params_dict['rate_limit'] = rate_limiter
        params = json.dumps(params_dict)
        log.info(f"Params: {params}")
        header = self._create_capi_headers()
        status, content, _ = self._http_request(api, 'POST', params, headers=header)
        if not status:
            raise Exception(content)
        json_parsed = json.loads(content)
        return status, json_parsed

    def pause_operation(self, bucket_name, blob_region, s3_bucket, pause_complete=True):
        hibernation_path = s3_bucket + "/" + bucket_name
        s3_path = 's3://' + hibernation_path
        result, content = self.pause_bucket(blob_region=blob_region, s3_path=s3_path, bucket=bucket_name)
        status = True
        if not result:
            raise Exception(content)
        if pause_complete:
            if not self.wait_bucket_hibernation(task='pause_bucket', operation='completed'):
                raise Exception(f"Pause operation on bucket {bucket_name} timed out")
        return status, content

    def stop_pause(self, bucket='default'):
        api = f'{self.baseUrl}controller/stopPause'
        params_dict = {}
        params_dict['bucket'] = bucket
        params = json.dumps(params_dict)
        log.info(f"Params: {params}")
        header = self._create_capi_headers()
        status, content, _ = self._http_request(api, 'POST', params, headers=header)
        if not status:
            raise Exception(content)
        json_parsed = json.loads(content)
        return status, json_parsed

    def stop_pause_operation(self, bucket='default'):
        result, content = self.stop_pause(bucket=bucket)
        self.wait_bucket_hibernation(task="pause_bucket", operation="stopped")

    def resume_bucket(self, blob_region, s3_path, bucket='default', rate_limiter=104857600):
        api = f'{self.baseUrl}controller/resume'
        params_dict = {}
        params_dict['bucket'] = bucket
        params_dict['blob_storage_region'] = blob_region
        params_dict['remote_path'] = s3_path
        params_dict['rate_limit'] = rate_limiter
        params = json.dumps(params_dict)
        log.info(f"Params: {params}")
        header = self._create_capi_headers()
        status, content, _ = self._http_request(api, 'POST', params, headers=header)
        if not status:
            raise Exception(content)
        json_parsed = json.loads(content)
        return status, json_parsed

    def resume_operation(self, bucket_name, blob_region, s3_bucket, resume_complete=True):
        hibernation_path = s3_bucket + "/" + bucket_name
        s3_path = 's3://' + hibernation_path
        result, content = self.resume_bucket(blob_region=blob_region, s3_path=s3_path, bucket=bucket_name)
        status = True
        if not result:
            raise Exception(content)
        if resume_complete:
            if not self.wait_bucket_hibernation(task='resume_bucket', operation='completed'):
                raise Exception(f"Resume operation on bucket {bucket_name} timed out")
        return status,content

    def stop_resume(self, bucket='default'):
        api = f"{self.baseUrl}controller/stopResume"
        params_dict = {}
        params_dict['bucket'] = bucket
        params = json.dumps(params_dict)
        log.info(f"Params: {params}")
        header = self._create_capi_headers()
        status, content, _ = self._http_request(api, 'POST', params, headers=header)
        if not status:
            raise Exception(f"stopResume for bucket {bucket} has failed. Content : {content}")
        json_parsed = json.loads(content)
        return status, json_parsed

    def stop_resume_operation(self, bucket='default'):
        result, content = self.stop_resume(bucket=bucket)
        self.wait_bucket_hibernation(task='resume_bucket', operation='stopped')

    def wait_bucket_hibernation(self, task, operation, timeout=600):
        status = self.get_hibernation_status(operation=task)
        if status == operation:
            return True
        timer = 0
        while timer <= timeout:
            status = self.get_hibernation_status(operation=task)
            if status == 'failed':
                raise Exception(f"Operation {operation} failed")
            elif status != operation:
                timer += 1
                time.sleep(1)
            else:
                return True
        raise Exception(f"Operation {operation} timed out")

    def get_hibernation_status(self, operation, tasktype='hibernation'):
        tasklist = self.ns_server_tasks()
        clustertasks = None
        retry = 0
        try:
            for task in tasklist:
                if task['type'] == tasktype:
                    if task['op'] != operation:
                        retry +=1
                        time.sleep(1)
                        if retry > 300:
                            break
                    elif task['op'] == operation:
                        clustertasks = task['status']
                        break
        except ValueError:
            pass
        if clustertasks is None:
            log.error(f"Operation {operation} not found in ns_server task endpoint \n {pprint.PrettyPrinter(width = 20).pprint(tasklist)}")
            raise Exception("Operation {operation} not found in ns_server task endpoint")
        return clustertasks

    def get_user(self, user_id):
        url = "settings/rbac/users/"
        api = self.baseUrl + url
        status, content, header = self._http_request(api, "GET")

        if content is not None:
            content_json = json.loads(content)

            for i in range(len(content_json)):
                user = content_json[i]
                if user.get('id') == user_id:
                    return user
        return {}

    """ From 6.5.0, enable IPv6 on cluster/node needs 2 settings
        default is set to IPv6
        We need to disable auto failover first, then set network version
        Then enable autofaiover again. """
    def enable_ip_version(self, afamily='ipv6', afamilyOnly='false'):
        log.info("Start enable {0} on this node {1}".format(afamily, self.baseUrl))
        self.update_autofailover_settings(False, 60)
        params = urllib.parse.urlencode({'afamily': afamily,
                                         'afamilyOnly': afamilyOnly,
                                         'nodeEncryption': 'off'})
        api = "{0}node/controller/enableExternalListener".format(self.baseUrl)
        status, content, header = self._http_request(api, 'POST', params)
        if status:
            params = urllib.parse.urlencode({'afamily': afamily,
                                             'afamilyOnly': afamilyOnly,
                                             'nodeEncryption': 'off'})
            api = "{0}node/controller/setupNetConfig".format(self.baseUrl)
            status, content, header = self._http_request(api, 'POST', params)
            if status:
                log.info("Done enable {0} on this node {1}".format(afamily, self.baseUrl))
            else:
                log.error("Failed to set 'setupNetConfig' on this node {0}"
                          .format(self.baseUrl))
                raise Exception(content)
        else:
            log.error("Failed to set 'enableExternalListener' on this node {0}"
                      .format(self.baseUrl))
            raise Exception(content)
        if afamilyOnly == 'true':
            api = "{0}node/controller/disableUnusedExternalListeners".format(self.baseUrl)
            status, _, _ = self._http_request(api, 'POST', params)
            if not status:
                log.error("Failed to set 'disableUnusedExternalListeners' on this node {0}"
                          .format(self.baseUrl))
        self.update_autofailover_settings(True, 60)

    def get_cluster_config_profile(self):
        api = self.baseUrl + "pools"
        header = self._create_capi_headers()
        status, content, _ = self._http_request(api, 'GET', headers=header)
        if not status:
            raise Exception(content)
        content_json = json.loads(content)
        log.info(content_json)
        if 'configProfile' in content_json:
            cluster_config_profile = content_json['configProfile']
        else:
            cluster_config_profile = 'default'
        return cluster_config_profile

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

    # Upload a root certificate
    def upload_cluster_ca(self, certificate):
        """ Upload a certificate the cluster

        This can be a root certificate or an intermediate certificate.
        """
        headers = self._create_capi_headers()
        headers['Content-Type'] = 'application/octet-stream'
        status, content, header = self._http_request(self.baseUrl + "controller/uploadClusterCA", 'POST', headers=headers, params=certificate)
        return status, content

    def set_min_tls_version(self, service='global', version='tlsv1.2'):
        """
        Sets the tls min version at per-service/global level
        """
        url = 'settings/security/tlsMinVersion'
        if service in ['data', 'fullTextSearch', 'index',
                       'eventing', 'query', 'analytics', 'backup', 'clusterManager']:
            url = 'settings/security/' + service + '/tlsMinVersion'
        api = self.baseUrl + url
        status, content, header = self._http_request(api, 'POST', params=version)
        return status, content

    def set_node_encryption_level(self, encryption_level):
        """
        Sets the node encryption at a global level
        encryption_level (str): Either 'strict', 'all' or 'control'.
        """
        url = 'settings/security'
        api = self.baseUrl + url
        params = f'clusterEncryptionLevel={encryption_level}'
        status, content, header = self._http_request(api, 'POST', params=params)
        return status, content

    def set_internal_password_rotation_interval(self, rotation_interval=1800000):
        api = self.baseUrl + "settings/security"
        payload = {
            'intCredsRotationInterval': rotation_interval
        }
        params = urllib.parse.urlencode(payload)
        status, content, header = self._http_request(api, 'POST', params)
        if not status:
            raise Exception("Failed to set internal password rotation interval: {}".format(content))

    def load_trusted_CAs(self):
        """
        Instructs the cluster to load trusted CAs(.pem files) from the node's inbox/CA folder
        """
        status, content, header = self._http_request(self.baseUrl +
                                                     "node/controller/loadTrustedCAs", 'POST')
        return status, content

    def reload_certificate(self, params=''):
        """ Reload certificate

        Call this function after uploading a certificate to the cluster to activate the new certificate.
        """
        headers = self._create_capi_headers()
        status, content, header = self._http_request(self.baseUrl + "node/controller/reloadCertificate",
                                                     'POST',
                                                     headers=headers,
                                                     params=params)
        return status, content

    def refresh_certificate(self, params=''):
        """
        Refresh certificate
        """
        headers = self._create_capi_headers()
        status, content, header = self._http_request(self.baseUrl + "controller/regenerateCertificate",
                                                     'POST',
                                                     headers=headers,
                                                     params=params)
        return status, content

    def refresh_credentials(self, params=''):
        """
        Refresh credentials
        """
        headers = self._create_capi_headers()
        status, content, header = self._http_request(self.baseUrl + "node/controller/rotateInternalCredentials",
                                                     'POST',
                                                     headers=headers,
                                                     params=params)
        return status, content

    def get_trusted_CAs(self):
        """
        Get all (default + uploaded) trusted CA certs information
        """
        status, content, _ = self._http_request(self.baseUrl
                                                     + "pools/default/trustedCAs", 'GET')
        if not status:
            msg = "Could not get trusted CAs; Failed with error %s" \
                  % (content)
            raise Exception(msg)
        return json.loads(content.decode('utf-8'))

    def delete_trusted_CA(self, ca_id):
        """

        Deletes a trusted CA from the cluster, given its ID
        """
        status, content, response = self._http_request(self.baseUrl
                                                     + "pools/default/trustedCAs/"
                                                     + str(ca_id),
                                                     'DELETE')
        return status, content, response

    def client_cert_auth(self, state, prefixes):
        """
        Args:
            state (str): Either 'enable', 'mandatory' or 'disable'.
            prefixes (list(dict)): A list of dicts of containing the keys 'path', 'prefix' and 'delimiter'
                e.g. {"path": .., "prefix": .., "delimiter", ..}
        """
        headers = self._create_capi_headers()
        params = json.dumps({'state': state, 'prefixes': prefixes})
        status, content, header = self._http_request(self.baseUrl + "settings/clientCertAuth", 'POST', headers=headers, params=params)
        return status, content

    def get_saml_settings(self):
        """
        Returns current saml settings as JSON
        """
        api = self.baseUrl + 'settings/saml'
        status, content, header = self._http_request(api, 'GET')
        return status, content, header

    def modify_saml_settings(self, saml_settings):
        """
        Modifies current saml settings. If some setting is not specified in POST, it is not modified
        """
        api = self.baseUrl + 'settings/saml'
        params = urllib.parse.urlencode(saml_settings)
        status, content, header = self._http_request(api, 'POST', params)
        return status, content, header

    def delete_saml_settings(self):
        """
        Removes all the settings. That's basically a reset to default
        """
        api = self.baseUrl + 'settings/saml'
        status, content, header = self._http_request(api, 'DELETE')
        return status, content, header

    def set_ui_session_timeout(self, timeout):
        """
        Set time in seconds until a browser session is closed
        """
        api = self.baseUrl + 'settings/security'
        params = urllib.parse.urlencode({"uiSessionTimeout": timeout})
        status, content, header = self._http_request(api, 'POST', params)
        return status, content, header


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
        self.port = CbServer.port
        if CbServer.use_https:
            self.port = CbServer.ssl_port
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
    def __init__(self, bucket_size='', name="", num_replicas=0, port=11211, master_id=None,
                 type='', eviction_policy="valueOnly", bucket_priority=None, uuid="", lww=False, maxttl=None,
                 bucket_storage=None, history_retention_bytes=4294967296, history_retention_secs=86400,
                 magma_key_tree_data_block_size=10096, magma_seq_tree_data_block_size=13107,
                 history_retention_collection_default=True, durabilityMinLevel=None,
                 numVBuckets=None):
        self.name = name
        self.port = port
        self.type = type
        self.nodes = None
        self.stats = None
        self.servers = []
        self.vbuckets = []
        self.forward_map = []
        self.numReplicas = num_replicas
        self.bucket_size = bucket_size
        self.kvs = {1:KVStore()}
        self.master_id = master_id
        self.numVBuckets = numVBuckets
        self.durability_level = durabilityMinLevel
        self.eviction_policy = eviction_policy
        self.bucket_priority = bucket_priority
        self.uuid = uuid
        self.lww = lww
        self.maxttl = maxttl
        self.bucket_storage = bucket_storage
        self.history_retention_collection_default = history_retention_collection_default
        self.history_retention_bytes = history_retention_bytes
        self.history_retention_secs = history_retention_secs
        self.magma_key_tree_data_block_size = magma_key_tree_data_block_size
        self.magma_seq_tree_data_block_size = magma_seq_tree_data_block_size


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
        self.recoveryType = ""
        self.version = ""
        self.os = ""
        self.ports = []
        self.availableStorage = []
        self.storage = []
        self.memoryQuota = 0
        self.memcached = 11210
        self.id = ""
        self.ip = ""
        self.internal_ip = ""
        self.rest_username = ""
        self.rest_password = ""
        self.port = 8091
        if CbServer.use_https:
            self.port = CbServer.ssl_port
        self.services = []
        self.storageTotalRam = 0

    @property
    def failed_over_state_a(self):
        """ The state in which a node is failed-over and is requesting a recovery type from the user
        """
        return self.clusterMembership == "inactiveFailed"

    @property
    def failed_over_state_b(self):
        """ The state in which a node is failed-over and the user has selected a recovery type
        """
        return self.clusterMembership == "inactiveAdded" and self.recoveryType

    @property
    def has_failed_over(self):
        """ Returns tree if a node is in the failed-over state
        """
        return self.failed_over_state_a or self.failed_over_state_b

    @property
    def complete_version(self):
        """ Returns the complete version of the node (e.g. 6.5.0)
        """
        return self.version.split('-')[0]

    @property
    def major_version(self):
        """ Returns the major version of the node (e.g. 6.5)
        """
        return self.complete_version.rsplit('.', 1)[0]

    @property
    def minor_version(self):
        """ Returns the minor version of the node (e.g. 0)
        """
        return self.complete_version.rsplit('.', 1)[1]

    @property
    def cluster_ip(self):
        return self.internal_ip or self.ip


class AutoFailoverSettings(object):
    def __init__(self):
        self.enabled = True
        self.timeout = 0
        self.count = 0
        self.failoverOnDataDiskIssuesEnabled = False
        self.failoverOnDataDiskIssuesTimeout = 0
        self.maxCount = 1


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
    def parse_index_status_response(self, parsed, return_system_query_scope=False):
        index_map = {}
        for map in parsed["indexes"]:
            if not return_system_query_scope and "`_system`.`_query`" in map['definition']:
                continue
            bucket_name = map['bucket']
            if bucket_name not in list(index_map.keys()):
                index_map[bucket_name] = {}
            index_name = map['index']
            index_map[bucket_name][index_name] = {}
            index_map[bucket_name][index_name]['status'] = map['status']
            index_map[bucket_name][index_name]['progress'] = str(map['progress'])
            index_map[bucket_name][index_name]['definition'] = map['definition']
            if 'partitioned' in map:
                index_map[bucket_name][index_name]['partitioned'] = map['partitioned']
            if len(map['hosts']) == 1:
                index_map[bucket_name][index_name]['hosts'] = map['hosts'][0]
            else:
                index_map[bucket_name][index_name]['hosts'] = map['hosts']
            index_map[bucket_name][index_name]['id'] = map['id']
        return index_map

    def parse_index_stats_response(self, parsed, index_map=None, return_system_query_scope=False):
        if index_map == None:
            index_map = {}
        for key in list(parsed.keys()):
            tokens = key.split(":")
            val = parsed[key]
            if len(tokens) == 3 and 'MAINT_STREAM' not in tokens[0] and 'INIT_STREAM' not in tokens[0]:
                bucket = tokens[0]
                index_name = tokens[1]
                stats_name = tokens[2]
                if bucket not in list(index_map.keys()):
                    index_map[bucket] = {}
                if index_name not in list(index_map[bucket].keys()):
                    index_map[bucket][index_name] = {}
                index_map[bucket][index_name][stats_name] = val
            elif len(tokens) == 5 and 'MAINT_STREAM' not in tokens[0] and 'INIT_STREAM' not in tokens[0]:
                bucket = tokens[0]
                scope_name = tokens[1]
                collection_name = tokens[2]
                index_name = tokens[3]
                stats_name = tokens[4]
                if not return_system_query_scope and "_system" == scope_name and "_query" ==collection_name:
                    continue
                keyspace = f'default:{bucket}.{scope_name}.{collection_name}'
                if keyspace not in list(index_map.keys()):
                    index_map[keyspace] = {}
                if index_name not in list(index_map[keyspace].keys()):
                    index_map[keyspace][index_name] = {}
                index_map[keyspace][index_name][stats_name] = val
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
        if 'recoveryType' in parsed:
            node.recoveryType = parsed['recoveryType']
        node.version = parsed['version']
        node.curr_items = 0
        if 'interestingStats' in parsed and 'curr_items' in parsed['interestingStats']:
            node.curr_items = parsed['interestingStats']['curr_items']
        node.port = parsed["hostname"][parsed["hostname"].rfind(":") + 1:]
        if CbServer.use_https:
            str_node_port = CbServer.ssl_port_map.get(str(node.port), str(node.port))
            if type(node.port) == int:
                node.port = int(str_node_port)
        node.os = parsed['os']

        if "services" in parsed:
            node.services = parsed["services"]

        if "otpNode" in parsed:
            node.id = parsed["otpNode"]
        if "hostname" in parsed:
            # should work for both: ipv4 and ipv6
            node.ip = parsed["hostname"].rsplit(":", 1)[0]

            if "alternateAddresses" in parsed and "external" in parsed["alternateAddresses"]:
                node.internal_ip = node.ip
                node.ip = parsed["alternateAddresses"]["external"]["hostname"]

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
            if "direct" in ports:
                node.memcached = ports["direct"]
                if CbServer.use_https:
                    node.memcached = int(CbServer.ssl_port_map.get(str(node.memcached), str(node.memcached)))

        if "storageTotals" in parsed:
            storageTotals = parsed["storageTotals"]
            if storageTotals.get("ram"):
                if storageTotals["ram"].get("total"):
                    ramKB = storageTotals["ram"]["total"]
                    node.storageTotalRam = ramKB//(1024*1024)
                    if node.mcdMemoryReserved == 0:
                        node.mcdMemoryReserved = node.storageTotalRam

                    if IS_CONTAINER:
                        # the storage total values are more accurate than
                        # mcdMemoryReserved - which is container host memory
                        node.mcdMemoryReserved = node.storageTotalRam * 0.70
        return node

    def parse_get_bucket_response(self, response):
        parsed = json.loads(response)
        return self.parse_get_bucket_json(parsed)

    def get_alt_addr_map(self, nodes):
        # map of kv ip:port to alt ip/host:port
        alt_addr_map = {}
        for node in nodes:
            if "alternateAddresses" in node and "external" in node["alternateAddresses"]:
                kv_host = node["hostname"].split(":")[0]
                kv_port = node["ports"]["direct"]

                alt_host = node["alternateAddresses"]["external"]["hostname"]
                alt_port = node["alternateAddresses"]["external"]["ports"]["kv"]

                alt_addr_map[f"{kv_host}:{kv_port}"] = f"{alt_host}:{alt_port}"

        return alt_addr_map

    def parse_get_bucket_json(self, parsed):
        bucket = Bucket()
        bucket.name = parsed['name']
        bucket.uuid = parsed['uuid']
        if 'bucketType' in parsed:
            bucket.type = parsed['bucketType']
        if 'storageBackend' in parsed:
            bucket.bucket_storage = parsed['storageBackend']
        if 'proxyPort' in parsed:
            bucket.port = parsed['proxyPort']
        bucket.nodes = list()
        if 'numVBuckets' in parsed:
            bucket.num_vbuckets = parsed['numVBuckets']
        if 'vBucketServerMap' in parsed:
            vBucketServerMap = parsed['vBucketServerMap']
            serverList = vBucketServerMap['serverList']
            if "nodes" in parsed and parsed["nodes"] is not None:
                alt_addr_map = self.get_alt_addr_map(parsed["nodes"])
                for i, server in enumerate(serverList):
                    if server in alt_addr_map:
                        serverList[i] = alt_addr_map[server]
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
        if 'durabilityLevel' in parsed:
            bucket.durability_level = parsed['durabilityLevel']
        if 'basicStats' in parsed:
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
            if 'uptime' in nodeDictionary:
                node.uptime = nodeDictionary['uptime']
            if 'memoryFree' in nodeDictionary:
                node.memoryFree = nodeDictionary['memoryFree']
            if 'memoryTotal' in nodeDictionary:
                node.memoryTotal = nodeDictionary['memoryTotal']
            if 'mcdMemoryAllocated' in nodeDictionary:
                node.mcdMemoryAllocated = nodeDictionary['mcdMemoryAllocated']
            if 'mcdMemoryReserved' in nodeDictionary:
                node.mcdMemoryReserved = nodeDictionary['mcdMemoryReserved']
            if 'status' in nodeDictionary:
                node.status = nodeDictionary['status']
            if 'hostname' in nodeDictionary:
                node.hostname = nodeDictionary['hostname']
            if 'clusterCompatibility' in nodeDictionary:
                node.clusterCompatibility = nodeDictionary['clusterCompatibility']
            if 'clusterMembership' in nodeDictionary:
                node.clusterCompatibility = nodeDictionary['clusterMembership']
            if 'version' in nodeDictionary:
                node.version = nodeDictionary['version']
            if 'os' in nodeDictionary:
                node.os = nodeDictionary['os']
            if "ports" in nodeDictionary:
                ports = nodeDictionary["ports"]
                if "direct" in ports:
                    node.memcached = ports["direct"]
                    if CbServer.use_https:
                        node.memcached = int(CbServer.ssl_port_map.get(str(node.memcached), str(node.memcached)))
            if "hostname" in nodeDictionary:
                value = str(nodeDictionary["hostname"])
                node.ip = value[:value.rfind(":")]
                node.port = int(value[value.rfind(":") + 1:])
                if CbServer.use_https:
                    node.port = int(CbServer.ssl_port_map.get(str(node.port), str(node.port)))
            if "otpNode" in nodeDictionary:
                node.id = nodeDictionary["otpNode"]
            bucket.nodes.append(node)
        return bucket
