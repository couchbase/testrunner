"""
Base class for FTS/CBFT/Couchbase Full Text Search
"""

import unittest
import time
import copy
import logger
import logging
import re

from couchbase_helper.cluster import Cluster
from membase.api.rest_client import RestConnection, Bucket
from membase.api.exception import ServerUnavailableException
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteUtilHelper
from testconstants import STANDARD_BUCKET_PORT
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase_helper.stats_tools import StatsCommon
from membase.helper.bucket_helper import BucketOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from TestInput import TestInputSingleton
from scripts.collect_server_info import cbcollectRunner
from couchbase_helper.documentgenerator import *

from couchbase_helper.documentgenerator import JsonDocGenerator
from lib.membase.api.exception import FTSException
from es_base import ElasticSearchBase

class RenameNodeException(FTSException):

    """Exception thrown when converting ip to hostname failed
    """

    def __init__(self, msg=''):
        FTSException.__init__(self, msg)


class RebalanceNotStopException(FTSException):

    """Exception thrown when stopping rebalance failed
    """

    def __init__(self, msg=''):
        FTSException.__init__(self, msg)


def raise_if(cond, ex):
    """Raise Exception if condition is True
    """
    if cond:
        raise ex

class OPS:
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    APPEND = "append"


class EVICTION_POLICY:
    VALUE_ONLY = "valueOnly"
    FULL_EVICTION = "fullEviction"


class BUCKET_PRIORITY:
    HIGH = "high"


class BUCKET_NAME:
    DEFAULT = "default"


class OS:
    WINDOWS = "windows"
    LINUX = "linux"
    OSX = "osx"


class COMMAND:
    SHUTDOWN = "shutdown"
    REBOOT = "reboot"


class STATE:
    RUNNING = "running"


class CHECK_AUDIT_EVENT:
    CHECK = False

class INDEX_DEFAULTS:

    BLEVE_MAPPING = {
                  "mapping": {
                    "default_mapping": {
                      "enabled": True,
                      "dynamic": True,
                      "default_analyzer": ""
                    },
                    "type_field": "type",
                    "default_type": "_default",
                    "default_analyzer": "standard",
                    "default_datetime_parser": "dateTimeOptional",
                    "default_field": "_all",
                    "byte_array_converter": "json",
                    "analysis": {}
                  },
                  "store": {
                    "kvStoreName": "forestdb"
                  }
              }

    ALIAS_DEFINITION = {"targets": {}}


    PLAN_PARAMS = {
                  "maxPartitionsPerPIndex": 32,
                  "numReplicas": 0,
                  "hierarchyRules": None,
                  "nodePlanParams": None,
                  "planFrozen": False
                  }

    SOURCE_CB_PARAMS = {
                      "authUser": "default",
                      "authPassword": "",
                      "authSaslUser": "",
                      "authSaslPassword": "",
                      "clusterManagerBackoffFactor": 0,
                      "clusterManagerSleepInitMS": 0,
                      "clusterManagerSleepMaxMS": 20000,
                      "dataManagerBackoffFactor": 0,
                      "dataManagerSleepInitMS": 0,
                      "dataManagerSleepMaxMS": 20000,
                      "feedBufferSizeBytes": 0,
                      "feedBufferAckThreshold": 0
                    }

    SOURCE_FILE_PARAMS = {
                          "regExps": [
                            ".txt$",
                            ".md$"
                          ],
                          "maxFileSize": 0,
                          "numPartitions": 0,
                          "sleepStartMS": 5000,
                          "backoffFactor": 1.5,
                          "maxSleepMS": 300000
                        }

    INDEX_DEFINITION = {
                          "type": "fulltext-index",
                          "name": "",
                          "uuid": "",
                          "params": {},
                          "sourceType": "couchbase",
                          "sourceName": "default",
                          "sourceUUID": "",
                          "sourceParams": SOURCE_CB_PARAMS,
                          "planParams": {}
                        }

class QUERY:

    JSON = {
              "indexName": "",
              "size": 10,
              "from": 0,
              "explain": False,
              "query": {},
              "fields": [],
              "ctl": {
                "consistency": {
                  "level": "",
                  "vectors": {}
                },
                "timeout": 0
              }
            }


# Event Definition:
# https://github.com/couchbase/goxdcr/blob/master/etc/audit_descriptor.json

class NodeHelper:
    _log = logger.Logger.get_logger()

    @staticmethod
    def disable_firewall(server):
        """Disable firewall to put restriction to replicate items in XDCR.
        @param server: server object to disable firewall
        @param rep_direction: replication direction unidirection/bidirection
        """
        shell = RemoteMachineShellConnection(server)
        shell.info = shell.extract_remote_info()

        if shell.info.type.lower() == "windows":
            output, error = shell.execute_command('netsh advfirewall set publicprofile state off')
            shell.log_command_output(output, error)
            output, error = shell.execute_command('netsh advfirewall set privateprofile state off')
            shell.log_command_output(output, error)
            # for details see RemoteUtilHelper.enable_firewall for windows
            output, error = shell.execute_command('netsh advfirewall firewall delete rule name="block erl.exe in"')
            shell.log_command_output(output, error)
            output, error = shell.execute_command('netsh advfirewall firewall delete rule name="block erl.exe out"')
            shell.log_command_output(output, error)
        else:
            o, r = shell.execute_command("iptables -F")
            shell.log_command_output(o, r)
            o, r = shell.execute_command(
                "/sbin/iptables -A INPUT -p tcp -i eth0 --dport 1000:65535 -j ACCEPT")
            shell.log_command_output(o, r)
            o, r = shell.execute_command(
                "/sbin/iptables -A INPUT -m state --state ESTABLISHED,RELATED -j ACCEPT")
            shell.log_command_output(o, r)
            # self.log.info("enabled firewall on {0}".format(server))
            o, r = shell.execute_command("/sbin/iptables --list")
            shell.log_command_output(o, r)
        shell.disconnect()

    @staticmethod
    def reboot_server(server, test_case, wait_timeout=60):
        """Reboot a server and wait for couchbase server to run.
        @param server: server object, which needs to be rebooted.
        @param test_case: test case object, since it has assert() function
                        which is used by wait_for_ns_servers_or_assert
                        to throw assertion.
        @param wait_timeout: timeout to whole reboot operation.
        """
        # self.log.info("Rebooting server '{0}'....".format(server.ip))
        shell = RemoteMachineShellConnection(server)
        if shell.extract_remote_info().type.lower() == OS.WINDOWS:
            o, r = shell.execute_command(
                "{0} -r -f -t 0".format(COMMAND.SHUTDOWN))
        elif shell.extract_remote_info().type.lower() == OS.LINUX:
            o, r = shell.execute_command(COMMAND.REBOOT)
        shell.log_command_output(o, r)
        # wait for restart and warmup on all server
        time.sleep(wait_timeout * 5)
        # disable firewall on these nodes
        NodeHelper.disable_firewall(server)
        # wait till server is ready after warmup
        ClusterOperationHelper.wait_for_ns_servers_or_assert(
            [server],
            test_case,
            wait_if_warmup=True)

    @staticmethod
    def enable_firewall(server):
        """Enable firewall
        @param server: server object to enable firewall
        @param rep_direction: replication direction unidirection/bidirection
        """
        RemoteUtilHelper.enable_firewall(
            server)

    @staticmethod
    def do_a_warm_up(server):
        """Warmp up server
        """
        shell = RemoteMachineShellConnection(server)
        shell.stop_couchbase()
        time.sleep(5)
        shell.start_couchbase()
        shell.disconnect()

    @staticmethod
    def wait_service_started(server, wait_time=120):
        """Function will wait for Couchbase service to be in
        running phase.
        """
        shell = RemoteMachineShellConnection(server)
        os_type = shell.extract_remote_info().distribution_type
        if os_type.lower() == 'windows':
            cmd = "sc query CouchbaseServer | grep STATE"
        else:
            cmd = "service couchbase-server status"
        now = time.time()
        while time.time() - now < wait_time:
            output, _ = shell.execute_command(cmd)
            if str(output).lower().find("running") != -1:
                # self.log.info("Couchbase service is running")
                return
            time.sleep(10)
        raise Exception(
            "Couchbase service is not running after {0} seconds".format(
                wait_time))

    @staticmethod
    def wait_warmup_completed(warmupnodes, bucket_names=["default"]):
        if isinstance(bucket_names, str):
            bucket_names = [bucket_names]
        start = time.time()
        for server in warmupnodes:
            for bucket in bucket_names:
                while time.time() - start < 150:
                    try:
                        mc = MemcachedClientHelper.direct_client(server, bucket)
                        if mc.stats()["ep_warmup_thread"] == "complete":
                            NodeHelper._log.info(
                                "Warmed up: %s items on %s on %s" %
                                (mc.stats("warmup")["ep_warmup_key_count"], bucket, server))
                            time.sleep(10)
                            break
                        elif mc.stats()["ep_warmup_thread"] == "running":
                            NodeHelper._log.info(
                                "Still warming up .. ep_warmup_key_count : %s" % (mc.stats("warmup")["ep_warmup_key_count"]))
                            continue
                        else:
                            NodeHelper._log.info(
                                "Value of ep_warmup_thread does not exist, exiting from this server")
                            break
                    except Exception as e:
                        NodeHelper._log.info(e)
                        time.sleep(10)
                if mc.stats()["ep_warmup_thread"] == "running":
                    NodeHelper._log.info(
                            "ERROR: ep_warmup_thread's status not complete")
                mc.close()


    @staticmethod
    def wait_node_restarted(
            server, test_case, wait_time=120, wait_if_warmup=False,
            check_service=False):
        """Wait server to be re-started
        """
        now = time.time()
        if check_service:
            NodeHelper.wait_service_started(server, wait_time)
            wait_time = now + wait_time - time.time()
        num = 0
        while num < wait_time / 10:
            try:
                ClusterOperationHelper.wait_for_ns_servers_or_assert(
                    [server], test_case, wait_time=wait_time - num * 10,
                    wait_if_warmup=wait_if_warmup)
                break
            except ServerUnavailableException:
                num += 1
                time.sleep(10)

    @staticmethod
    def kill_erlang(server):
        """Kill erlang process running on server.
        """
        NodeHelper._log.info("Killing erlang on server: {0}".format(server))
        shell = RemoteMachineShellConnection(server)
        os_info = shell.extract_remote_info()
        shell.kill_erlang(os_info)
        shell.disconnect()

    @staticmethod
    def kill_memcached(server):
        """Kill memcached process running on server.
        """
        shell = RemoteMachineShellConnection(server)
        shell.kill_erlang()
        shell.disconnect()

    @staticmethod
    def get_log_dir(node):
        """Gets couchbase log directory, even for cluster_run
        """
        _, dir = RestConnection(node).diag_eval('filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
        return str(dir)

    @staticmethod
    def check_fts_log(server, str):
        """ Checks if a string 'str' is present in goxdcr.log on server
            and returns the number of occurances
        """
        shell = RemoteMachineShellConnection(server)
        fts_log = NodeHelper.get_log_dir(server) + '/fts.log*'
        count, err = shell.execute_command("zgrep \"{0}\" {1} | wc -l".
                                        format(str, fts_log))
        if isinstance(count, list):
            count = int(count[0])
        else:
            count = int(count)
        NodeHelper._log.info(count)
        shell.disconnect()
        return count

    @staticmethod
    def rename_nodes(servers):
        """Rename server name from ip to their hostname
        @param servers: list of server objects.
        @return: dictionary whose key is server and value is hostname
        """
        hostnames = {}
        for server in servers:
            shell = RemoteMachineShellConnection(server)
            try:
                hostname = shell.get_full_hostname()
                rest = RestConnection(server)
                renamed, content = rest.rename_node(
                    hostname, username=server.rest_username,
                    password=server.rest_password)
                raise_if(
                    not renamed,
                    RenameNodeException(
                        "Server %s is not renamed! Hostname %s. Error %s" % (
                            server, hostname, content)
                    )
                )
                hostnames[server] = hostname
                server.hostname = hostname
            finally:
                shell.disconnect()
        return hostnames

    # Returns version like "x.x.x" after removing build number
    @staticmethod
    def get_cb_version(node):
        rest = RestConnection(node)
        version = rest.get_nodes_self().version
        return version[:version.rfind('-')]

    @staticmethod
    def get_cbcollect_info(server):
        """Collect cbcollectinfo logs for all the servers in the cluster.
        """
        path = TestInputSingleton.input.param("logs_folder", "/tmp")
        print "grabbing cbcollect from {0}".format(server.ip)
        path = path or "."
        try:
            cbcollectRunner(server, path).run()
            TestInputSingleton.input.test_params[
                "get-cbcollect-info"] = False
        except Exception as e:
            NodeHelper._log.error(
                "IMPOSSIBLE TO GRAB CBCOLLECT FROM {0}: {1}".format(
                    server.ip,
                    e))

    @staticmethod
    def collect_logs(server):
        """Grab cbcollect before we cleanup
        """
        NodeHelper.get_cbcollect_info(server)


class FloatingServers:

    """Keep Track of free servers, For Rebalance-in
    or swap-rebalance operations.
    """
    _serverlist = []


class FTSIndex:
    """
    To create a Full Text Search index :
    e.g., FTSIndex("beer_index", self._cluster, source_type = 'couchbase',
                    source_name = 'beer-sample', index_type = 'fulltext-index',
                    index_params = {'store' : 'forestdb'},
                    plan_params = {'maxPartitionsPerIndex' : 40}
                  )

    To create an FTS Alias:
        FTSIndex("beer_index", self._cluster, source_type = 'couchbase',
                 source_name = 'beer-sample', index_type = 'alias',
                 index_params = {'store' : 'forestdb'},
                 plan_params = {'maxPartitionsPerIndex' : 40}
                 )
    """
    def __init__(self, cluster, name, source_type='couchbase',
                 source_name=None, index_type='fulltext-index', index_params=None,
                 plan_params=None, source_params=None, source_uuid=None):

        """
         @param name : name of index/alias
         @param cluster : 'this' cluster object
         @param source_type : 'couchbase' or 'files'
         @param source_name : name of couchbase bucket
         @param index_type : 'fulltext-index' or 'fulltext-alias'
         @param index_params :  to specify advanced index mapping;
                                dictionary overiding params in
                                INDEX_DEFAULTS.BLEVE_MAPPING or
                                INDEX_DEFAULTS.ALIAS_DEFINITION depending on
                                index_type
         @param plan_params : dictionary overriding params defined in
                                INDEX_DEFAULTS.PLAN_PARAMS
         @param source_params: dictionary overriding params defined in
                                INDEX_DEFAULTS.SOURCE_CB_PARAMS or
                                INDEX_DEFAULTS.SOURCE_FILE_PARAMS
         @param source_uuid: UUID of the source, may not be used

        """

        self.__cluster = cluster
        self.__log = cluster.get_logger()
        self._source_type = source_type
        self._source_name = source_name
        self._one_time = False
        self.index_type = index_type
        self.rest = RestConnection(self.__cluster.get_random_fts_node())
        self.index_definition = INDEX_DEFAULTS.INDEX_DEFINITION
        self.name = self.index_definition['name'] = name
        self.es_custom_map = None
        self.smart_query_fields = None
        self.index_definition['type'] = self.index_type
        if self.index_type == "fulltext-alias":
            self.index_definition['sourceType'] = "nil"
            self.index_definition['sourceName'] = ""
        else:
            self.source_bucket = self.__cluster.get_bucket_by_name(source_name)
            self.index_definition['sourceType'] = self._source_type
            self.index_definition['sourceName'] = self._source_name

        self.dataset = TestInputSingleton.input.param("dataset", "emp")
        # for wiki docs, specify default analyzer as "simple"
        if self.index_type == "fulltext-index" and \
                (self.dataset == "wiki" or self.dataset == "all"):
            self.index_definition['params'] = self.build_custom_index_params(
                {"default_analyzer": "simple"})

        # Support for custom map
        self.custom_map = TestInputSingleton.input.param("custom_map", False)
        self.cm_id = TestInputSingleton.input.param("cm_id", 0)
        if self.custom_map:
            from custom_map_generator.map_generator import CustomMapGenerator
            cm_gen = CustomMapGenerator(seed=self.cm_id, dataset=self.dataset)
            fts_map, self.es_custom_map = cm_gen.get_map()
            self.smart_query_fields = cm_gen.get_smart_query_fields()
            print self.smart_query_fields
            self.index_definition['params'] = self.build_custom_index_params(
                fts_map)
            self.__log.info(json.dumps(self.index_definition["params"],
                                       indent=3))

        self.fts_queries = []

        if index_params:
            self.index_definition['params'] = \
                self.build_custom_index_params(index_params)

        if plan_params:
            self.index_definition['planParams'] = \
                self.build_custom_plan_params(plan_params)

        if source_params:
            self.index_definition['sourceParams'] = \
                self.build_source_params(source_params)

        if source_uuid:
            self.index_definition['sourceUUID'] = source_uuid

    def build_custom_index_params(self, index_params):
        if self.index_type == "fulltext-index":
            mapping = INDEX_DEFAULTS.BLEVE_MAPPING
            if not TestInputSingleton.input.param("default_map", False):
                mapping['mapping']['default_mapping']['enabled'] = False
            mapping['mapping'].update(index_params)
        else:
            mapping = INDEX_DEFAULTS.ALIAS_DEFINITION
            mapping.update(index_params)
        return mapping

    def build_custom_plan_params(self, plan_params):
        plan = INDEX_DEFAULTS.PLAN_PARAMS
        plan.update(plan_params)
        return plan

    def build_source_params(self, source_params):
        if self._source_type == "couchbase":
            src_params = INDEX_DEFAULTS.SOURCE_CB_PARAMS
        else:
            src_params = INDEX_DEFAULTS.SOURCE_FILE_PARAMS
        src_params.update(source_params)
        return src_params

    def create(self):
        self.__log.info("Checking if index already exists ...")
        status, _ = self.rest.get_fts_index_definition(self.name)
        if status != 400:
            self.rest.delete_fts_index(self.name)
        self.__log.info("Creating {0} {1} on {2}".format(
            self.index_type,
            self.name,
            self.rest.ip))
        self.rest.create_fts_index(self.name, self.index_definition)

    def update(self):
        self.__log.info("Updating {0} {1} on {2}".format(
            self.index_type,
            self.name,
            self.rest.ip))
        self.rest.update_fts_index(self.name, self.index_definition)

    def delete(self):
        self.__log.info("Deleting {0} {1} on {2}".format(
            self.index_type,
            self.name,
            self.rest.ip))
        status = self.rest.delete_fts_index(self.name)
        if status:
            self.__log.info("{0} deleted".format(self.name))

    def get_index_defn(self):
        return self.rest.get_fts_index_definition(self.name)

    def get_max_partitions_pindex(self):
        _, defn = self.get_index_defn()
        return int(defn['indexDef']['planParams']['maxPartitionsPerPIndex'])

    def clone(self, clone_name):
        pass

    def get_indexed_doc_count(self):
        return self.rest.get_fts_index_doc_count(self.name)

    def get_src_bucket_doc_count(self):
        return self.__cluster.get_doc_count_in_bucket(self.source_bucket)

    def get_uuid(self):
        return self.rest.get_fts_index_uuid(self.name)

    def construct_cbft_query_json(self, query, fields=None, timeout=None):

        max_matches = TestInputSingleton.input.param("query_max_matches", 10000000)
        query_json = QUERY.JSON
        # query is a unicode dict
        query_json['query'] = query
        query_json['indexName'] = self.name
        if max_matches:
            query_json['size'] = int(max_matches)
        if timeout:
            query_json['timeout'] = int(timeout)
        if fields:
            query_json['fields'] = fields
        return query_json

    def execute_query(self, query, zero_results_ok=True, expected_hits=None):
        """
        Takes a query dict, constructs a json, runs and returns results
        """
        query_dict = self.construct_cbft_query_json(query)
        try:
            doc_ids = []
            hits, matches, time_taken =\
                self.__cluster.run_fts_query(self.name, query_dict)
            if hits:
                for doc in matches:
                    doc_ids.append(doc['id'])
            if int(hits) == 0 and not zero_results_ok:
                raise FTSException("No docs returned for query : %s" %query_dict)
            if expected_hits and expected_hits != hits:
                raise FTSException("Expected hits: %s, fts returned: %s"
                                   % (expected_hits, hits))
            return hits, doc_ids, time_taken
        except ServerUnavailableException:
            # query time outs
            raise ServerUnavailableException
        except Exception as e:
            self.__log.error("Error running query: %s" % e)

class CouchbaseCluster:

    def __init__(self, name, nodes, log, use_hostname=False):
        """
        @param name: Couchbase cluster name. e.g C1, C2 to distinguish in logs.
        @param nodes: list of server objects (read from ini file).
        @param log: logger object to print logs.
        @param use_hostname: True if use node's hostname rather ip to access
                        node else False.
        """
        self.__name = name
        self.__nodes = nodes
        self.__log = log
        self.__mem_quota = 0
        self.__use_hostname = use_hostname
        self.__master_node = nodes[0]
        self.__design_docs = []
        self.__buckets = []
        self.__hostnames = {}
        self.__fail_over_nodes = []
        self.__data_verified = True
        self.__remote_clusters = []
        self.__clusterop = Cluster()
        self._kv_gen = {}
        self.__indexes = []
        self.__fts_nodes = []
        self.__non_fts_nodes = []
        self.__separate_nodes_on_services()

    def __str__(self):
        return "Couchbase Cluster: %s, Master Ip: %s" % (
            self.__name, self.__master_node.ip)

    def get_node(self, ip, port):
        for node in self.__nodes:
            if ip == node.ip and port == node.port:
                return node

    def get_logger(self):
        return self.__log

    def __separate_nodes_on_services(self):
        self.__fts_nodes = []
        self.__non_fts_nodes = []
        service_map = RestConnection(self.__master_node).get_nodes_services()
        for node_ip, services in service_map.iteritems():
            node = self.get_node(node_ip.split(':')[0], node_ip.split(':')[1])
            if "fts" in services:
                self.__fts_nodes.append(node)
            else:
                self.__non_fts_nodes.append(node)

    def get_fts_nodes(self):
        return self.__fts_nodes

    def get_non_fts_nodes(self):
        return self.__non_fts_nodes

    def __stop_rebalance(self):
        rest = RestConnection(self.__master_node)
        if rest._rebalance_progress_status() == 'running':
            self.__log.warning(
                "rebalancing is still running, test should be verified")
            stopped = rest.stop_rebalance()
            raise_if(
                not stopped,
                RebalanceNotStopException("unable to stop rebalance"))

    def __init_nodes(self):
        """Initialize all nodes. Rename node to hostname
        if needed by test.
        """
        tasks = []
        for node in self.__nodes:
            tasks.append(
                self.__clusterop.async_init_node(
                    node))
        for task in tasks:
            mem_quota = task.result()
            if mem_quota < self.__mem_quota or self.__mem_quota == 0:
                self.__mem_quota = mem_quota
        if self.__use_hostname:
            self.__hostnames.update(NodeHelper.rename_nodes(self.__nodes))

    def get_host_names(self):
        return self.__hostnames

    def get_master_node(self):
        return self.__master_node

    def get_indexes(self):
        return self.__indexes

    def get_random_node(self):
        return self.__nodes[random.randint(0, len(self.__nodes)-1)]

    def get_random_fts_node(self):
        if not self.__fts_nodes:
            self.__separate_nodes_on_services()
        if len(self.__fts_nodes) == 1:
            return self.__fts_nodes[0]
        return self.__fts_nodes[random.randint(0, len(self.__fts_nodes)-1)]

    def get_random_non_fts_node(self):
        return self.__non_fts_nodes[random.randint(0, len(self.__fts_nodes)-1)]

    def get_mem_quota(self):
        return self.__mem_quota

    def get_nodes(self):
        return self.__nodes

    def get_name(self):
        return self.__name

    def get_cluster(self):
        return self.__clusterop

    def get_kv_gen(self):
        raise_if(
            self._kv_gen is None,
            FTSException(
                "KV store is empty on couchbase cluster: %s" %
                self))
        return self._kv_gen

    def init_cluster(self, cluster_services, available_nodes):
        """Initialize cluster.
        1. Initialize all nodes.
        2. Add all nodes to the cluster based on services list
        @param cluster_services: list of cluster node services
        @param available_nodes: extra nodes available to be added
        """
        self.__log.info("Initializing Cluster ...")

        if len(cluster_services)-1 > len(available_nodes):
            raise FTSException("Only %s nodes present for given cluster"
                                "configuration %s"
                               % (len(available_nodes)+1, cluster_services))
        self.__init_nodes()
        if available_nodes:
            nodes_to_add = []
            node_services = []
            for index, node_service in enumerate(cluster_services):
                if index == 0:
                    # first node is always a data/kv node
                    continue
                self.__log.info("%s will be configured with services %s" %(
                                                    available_nodes[index-1].ip,
                                                    node_service))
                nodes_to_add.append(available_nodes[index-1])
                node_services.append(node_service)
            try:
                self.__clusterop.async_rebalance(
                        self.__nodes,
                        nodes_to_add,
                        [],
                        use_hostnames=self.__use_hostname,
                        services=node_services).result()
            except Exception as e:
                    raise FTSException("Unable to initialize cluster with config "
                                        "%s: %s" %(cluster_services, e))
            self.__nodes += nodes_to_add
        self.__separate_nodes_on_services()


    def cleanup_cluster(
            self,
            test_case,
            from_rest=False,
            cluster_shutdown=True):
        """Cleanup cluster.
        1. Remove all remote cluster references.
        2. Remove all replications.
        3. Remove all buckets.
        @param test_case: Test case object.
        @param test_failed: True if test failed else False.
        @param cluster_run: True if test execution is single node cluster run else False.
        @param cluster_shutdown: True if Task (task.py) Scheduler needs to shutdown else False
        """
        try:
            if self.get_indexes():
                self.delete_all_fts_indexes()
            self.__log.info("removing nodes from cluster ...")
            self.__stop_rebalance()
            self.__log.info("cleanup {0}".format(self.__nodes))
            for node in self.__nodes:
                BucketOperationHelper.delete_all_buckets_or_assert(
                    [node],
                    test_case)
                force_eject = TestInputSingleton.input.param(
                    "forceEject",
                    False)
                if force_eject and node != self.__master_node:
                    try:
                        rest = RestConnection(node)
                        rest.force_eject_node()
                    except BaseException as e:
                        self.__log.error(e)
                else:
                    ClusterOperationHelper.cleanup_cluster([node])
                ClusterOperationHelper.wait_for_ns_servers_or_assert(
                    [node],
                    test_case)
        finally:
            if cluster_shutdown:
                self.__clusterop.shutdown(force=True)

    def create_sasl_buckets(
            self, bucket_size, num_buckets=1, num_replicas=1,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_priority=BUCKET_PRIORITY.HIGH):
        """Create sasl buckets.
        @param bucket_size: size of the bucket.
        @param num_buckets: number of buckets to create.
        @param num_replicas: number of replicas (1-3).
        @param eviction_policy: valueOnly etc.
        @param bucket_priority: high/low etc.
        """
        bucket_tasks = []
        for i in range(num_buckets):
            name = "sasl_bucket_" + str(i + 1)
            bucket_tasks.append(self.__clusterop.async_create_sasl_bucket(
                self.__master_node,
                name,
                'password',
                bucket_size,
                num_replicas,
                eviction_policy=eviction_policy,
                bucket_priority=bucket_priority))
            self.__buckets.append(
                Bucket(
                    name=name, authType="sasl", saslPassword="password",
                    num_replicas=num_replicas, bucket_size=bucket_size,
                    eviction_policy=eviction_policy,
                    bucket_priority=bucket_priority
                ))

        for task in bucket_tasks:
            task.result()

    def create_standard_buckets(
            self, bucket_size, name=None, num_buckets=1,
            port=None, num_replicas=1,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_priority=BUCKET_PRIORITY.HIGH):
        """Create standard buckets.
        @param bucket_size: size of the bucket.
        @param num_buckets: number of buckets to create.
        @param num_replicas: number of replicas (1-3).
        @param eviction_policy: valueOnly etc.
        @param bucket_priority: high/low etc.
        """
        bucket_tasks = []
        start_port = STANDARD_BUCKET_PORT
        if port:
            start_port = port

        for i in range(num_buckets):
            if not (num_buckets == 1 and name):
                name = "standard_bucket_" + str(i + 1)
            bucket_tasks.append(self.__clusterop.async_create_standard_bucket(
                self.__master_node,
                name,
                start_port + i,
                bucket_size,
                num_replicas,
                eviction_policy=eviction_policy,
                bucket_priority=bucket_priority))
            self.__buckets.append(
                Bucket(
                    name=name,
                    authType=None,
                    saslPassword=None,
                    num_replicas=num_replicas,
                    bucket_size=bucket_size,
                    port=start_port + i,
                    eviction_policy=eviction_policy,
                    bucket_priority=bucket_priority
                ))

        for task in bucket_tasks:
            task.result()

    def create_default_bucket(
            self, bucket_size, num_replicas=1,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_priority=BUCKET_PRIORITY.HIGH):
        """Create default bucket.
        @param bucket_size: size of the bucket.
        @param num_replicas: number of replicas (1-3).
        @param eviction_policy: valueOnly etc.
        @param bucket_priority: high/low etc.
        """
        self.__clusterop.create_default_bucket(
            self.__master_node,
            bucket_size,
            num_replicas,
            eviction_policy=eviction_policy,
            bucket_priority=bucket_priority
        )
        self.__buckets.append(
            Bucket(
                name=BUCKET_NAME.DEFAULT,
                authType="sasl",
                saslPassword="",
                num_replicas=num_replicas,
                bucket_size=bucket_size,
                eviction_policy=eviction_policy,
                bucket_priority=bucket_priority
            ))

    def create_fts_index(self, name, source_type='couchbase',
                         source_name=None, index_type='fulltext-index',
                         index_params=None, plan_params=None,
                         source_params=None, source_uuid=None):
        """Create fts index/alias
        @param node: Node on which index is created
        @param name: name of the index/alias
        @param source_type : 'couchbase' or 'files'
        @param source_name : name of couchbase bucket or "" for alias
        @param index_type : 'fulltext-index' or 'fulltext-alias'
        @param index_params :  to specify advanced index mapping;
                                dictionary overiding params in
                                INDEX_DEFAULTS.BLEVE_MAPPING or
                                INDEX_DEFAULTS.ALIAS_DEFINITION depending on
                                index_type
        @param plan_params : dictionary overriding params defined in
                                INDEX_DEFAULTS.PLAN_PARAMS
        @param source_params: dictionary overriding params defined in
                                INDEX_DEFAULTS.SOURCE_CB_PARAMS or
                                INDEX_DEFAULTS.SOURCE_FILE_PARAMS
        @param source_uuid: UUID of the source, may not be used
        """
        index = FTSIndex(
            self,
            name,
            source_type,
            source_name,
            index_type,
            index_params,
            plan_params,
            source_params,
            source_uuid
        )
        index.create()
        self.__indexes.append(index)
        return index

    def get_fts_index_by_name(self, name):
        """ Returns an FTSIndex object with the given name """
        for index in self.__indexes:
            if index.name == name:
                return index

    def delete_fts_index(self, name):
        """ Delete an FTSIndex object with the given name from a given node """
        for index in self.__indexes:
            if index.name == name:
                index.delete()

    def delete_all_fts_indexes(self):
        """ Delete all FTSIndexes from a given node """
        for index in self.__indexes:
            index.delete()

    def run_fts_query(self, index_name, query_dict, node=None):
        """ Runs a query defined in query_json against an index/alias and
        a specific node

        @return total_hits : total hits for the query,
        @return hit_list : list of docs that match the query

        """
        if not node:
            node = self.get_random_fts_node()
        self.__log.info("Running query %s on node: %s"
                        % (json.dumps(query_dict, ensure_ascii=False),
                           node.ip))
        total_hits, hit_list, time_taken = \
            RestConnection(node).run_fts_query(index_name, query_dict)
        return total_hits, hit_list, time_taken

    def get_buckets(self):
        return self.__buckets

    def get_bucket_by_name(self, bucket_name):
        """Return the bucket with given name
        @param bucket_name: bucket name.
        @return: bucket object
        """
        for bucket in RestConnection(self.__master_node).get_buckets():
            if bucket.name == bucket_name:
                return bucket

        raise Exception(
            "Bucket with name: %s not found on the cluster" %
            bucket_name)

    def get_doc_count_in_bucket(self, bucket):
        return RestConnection(self.__master_node).get_active_key_count(bucket)

    def delete_bucket(self, bucket_name):
        """Delete bucket with given name
        @param bucket_name: bucket name (string) to delete
        """
        bucket_to_remove = self.get_bucket_by_name(bucket_name)
        self.__clusterop.bucket_delete(
            self.__master_node,
            bucket_to_remove.name)
        self.__buckets.remove(bucket_to_remove)

    def delete_all_buckets(self):
        for bucket_to_remove in self.__buckets:
            self.__clusterop.bucket_delete(
                self.__master_node,
                bucket_to_remove.name)
            self.__buckets.remove(bucket_to_remove)

    def flush_buckets(self, buckets=[]):
        buckets = buckets or self.__buckets
        tasks = []
        for bucket in buckets:
            tasks.append(self.__clusterop.async_bucket_flush(
                self.__master_node,
                bucket))
        [task.result() for task in tasks]

    def async_load_bucket(self, bucket, num_items, exp=0,
                          kv_store=1, flag=0, only_store_hash=True,
                          batch_size=1000, pause_secs=1, timeout_secs=30):
        """Load data asynchronously on given bucket. Function don't wait for
        load data to finish, return immidiately.
        @param bucket: bucket where to load data.
        @param num_items: number of items to load
        @param value_size: size of the one item.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        @return: task object
        """
        seed = "%s-key-" % self.__name
        self._kv_gen[OPS.CREATE] = JsonDocGenerator(seed,
                                                     encoding="utf-8",
                                                     start=0,
                                                     end=num_items)

        gen = copy.deepcopy(self._kv_gen[OPS.CREATE])
        task = self.__clusterop.async_load_gen_docs(
            self.__master_node, bucket.name, gen, bucket.kvs[kv_store],
            OPS.CREATE, exp, flag, only_store_hash, batch_size, pause_secs,
            timeout_secs)
        return task

    def load_bucket(self, bucket, num_items, value_size=512, exp=0,
                    kv_store=1, flag=0, only_store_hash=True,
                    batch_size=1000, pause_secs=1, timeout_secs=30):
        """Load data synchronously on given bucket. Function wait for
        load data to finish.
        @param bucket: bucket where to load data.
        @param num_items: number of items to load
        @param value_size: size of the one item.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        """
        task = self.async_load_bucket(bucket, num_items, value_size, exp,
                                      kv_store, flag, only_store_hash,
                                      batch_size, pause_secs, timeout_secs)
        task.result()

    def async_load_all_buckets(self, num_items, exp=0,
                               kv_store=1, flag=0, only_store_hash=True,
                               batch_size=1000, pause_secs=1, timeout_secs=30):
        """Load data asynchronously on all buckets of the cluster.
        Function don't wait for load data to finish, return immidiately.
        @param num_items: number of items to load
        @param value_size: size of the one item.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        @return: task objects list
        """
        prefix = "%s-" % self.__name
        self._kv_gen[OPS.CREATE] = JsonDocGenerator(prefix,
                                                     encoding="utf-8",
                                                     start=0,
                                                     end=num_items)
        tasks = []
        for bucket in self.__buckets:
            gen = copy.deepcopy(self._kv_gen[OPS.CREATE])
            tasks.append(
                self.__clusterop.async_load_gen_docs(
                    self.__master_node, bucket.name, gen, bucket.kvs[kv_store],
                    OPS.CREATE, exp, flag, only_store_hash, batch_size,
                    pause_secs, timeout_secs)
            )
        return tasks

    def load_all_buckets(self, num_items, value_size=512, exp=0,
                         kv_store=1, flag=0, only_store_hash=True,
                         batch_size=1000, pause_secs=1, timeout_secs=30):
        """Load data synchronously on all buckets. Function wait for
        load data to finish.
        @param num_items: number of items to load
        @param value_size: size of the one item.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        """
        tasks = self.async_load_all_buckets(
            num_items, exp, kv_store, flag, only_store_hash,
            batch_size, pause_secs, timeout_secs)
        for task in tasks:
            task.result()

    def load_all_buckets_from_generator(self, kv_gen, ops=OPS.CREATE, exp=0,
                                        kv_store=1, flag=0, only_store_hash=True,
                                        batch_size=1000, pause_secs=1, timeout_secs=30):
        """Load data synchronously on all buckets. Function wait for
        load data to finish.
        @param gen: BlobGenerator() object
        @param ops: OPS.CREATE/UPDATE/DELETE/APPEND.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        """
        tasks = self.async_load_all_buckets_from_generator(kv_gen)
        for task in tasks:
            task.result()

    def async_load_all_buckets_from_generator(self, kv_gen, ops=OPS.CREATE, exp=0,
                                              kv_store=1, flag=0, only_store_hash=True,
                                              batch_size=5000, pause_secs=1, timeout_secs=30):
        """Load data asynchronously on all buckets. Function wait for
        load data to finish.
        @param gen: BlobGenerator() object
        @param ops: OPS.CREATE/UPDATE/DELETE/APPEND.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        """
        if ops not in self._kv_gen:
            self._kv_gen[ops] = kv_gen

        tasks = []
        for bucket in self.__buckets:
            kv_gen = copy.deepcopy(self._kv_gen[ops])
            tasks.append(
                self.__clusterop.async_load_gen_docs(
                    self.__master_node, bucket.name, kv_gen,
                    bucket.kvs[kv_store], ops, exp, flag,
                    only_store_hash, batch_size, pause_secs, timeout_secs)
            )
        return tasks

    def async_load_bucket_from_generator(self, bucket, kv_gen, ops=OPS.CREATE, exp=0,
                                              kv_store=1, flag=0, only_store_hash=True,
                                              batch_size=5000, pause_secs=1, timeout_secs=30):
        """Load data asynchronously on all buckets. Function wait for
        load data to finish.
        @param bucket: pass object of bucket to load into
        @param gen: BlobGenerator() object
        @param ops: OPS.CREATE/UPDATE/DELETE/APPEND.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        """

        task = []
        task.append(
            self.__clusterop.async_load_gen_docs(
                self.__master_node, bucket.name, kv_gen,
                bucket.kvs[kv_store], ops, exp, flag,
                only_store_hash, batch_size, pause_secs, timeout_secs)
        )
        return task


    def load_all_buckets_till_dgm(self, active_resident_threshold, items=0,
                                  exp=0, kv_store=1, flag=0,
                                  only_store_hash=True, batch_size=1000,
                                  pause_secs=1, timeout_secs=30):
        """Load data synchronously on all buckets till dgm (Data greater than memory)
        for given active_resident_threshold
        @param active_resident_threshold: Dgm threshold.
        @param value_size: size of the one item.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        """
        items = int(items)
        self.__log.info("First loading \"items\" {0} number keys to handle "
                      "update/deletes in dgm cases".format(items))
        self.load_all_buckets(items)

        self.__log.info("Now loading extra keys to reach dgm limit")
        seed = "%s-key-" % self.__name
        end = 0
        for bucket in self.__buckets:
            current_active_resident = StatsCommon.get_stats(
                [self.__master_node],
                bucket,
                '',
                'vb_active_perc_mem_resident')[self.__master_node]
            start = items
            end = start + batch_size * 10
            while int(current_active_resident) > active_resident_threshold:
                self.__log.info("loading %s keys ..." % (end-start))

                kv_gen = JsonDocGenerator(seed,
                                          encoding="utf-8",
                                          start=start,
                                          end=end)

                tasks = []
                tasks.append(self.__clusterop.async_load_gen_docs(
                    self.__master_node, bucket.name, kv_gen, bucket.kvs[kv_store],
                    OPS.CREATE, exp, flag, only_store_hash, batch_size,
                    pause_secs, timeout_secs))

                for task in tasks:
                    task.result()
                start = end
                end = start + batch_size * 10
                current_active_resident = StatsCommon.get_stats(
                    [self.__master_node],
                    bucket,
                    '',
                    'vb_active_perc_mem_resident')[self.__master_node]
                self.__log.info(
                    "Current resident ratio: %s, desired: %s bucket %s" % (
                        current_active_resident,
                        active_resident_threshold,
                        bucket.name))
            self.__log.info("Loaded a total of %s keys into bucket %s"
                            % (end,bucket.name))
        self._kv_gen[OPS.CREATE] = JsonDocGenerator(seed,
                                                    encoding="utf-8",
                                                    start=0,
                                                    end=end)

    def update_bucket(self, bucket, fields_to_update=None, exp=0,
                    kv_store=1, flag=0, only_store_hash=True,
                    batch_size=1000, pause_secs=1, timeout_secs=30):
        """Load data synchronously on given bucket. Function wait for
        load data to finish.
        @param bucket: bucket where to load data.
        @param fields_to_update: list of fields to update in loaded JSON
        @param value_size: size of the one item.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        """
        self.__log.info("Updating fields %s in bucket %s" %(fields_to_update,
                                                            bucket.name))
        task = self.async_update_bucket(bucket, fields_to_update=fields_to_update,
                                        exp=exp, kv_store=kv_store, flag=flag,
                                        only_store_hash=only_store_hash,
                                        batch_size=batch_size,
                                        pause_secs=pause_secs,
                                        timeout_secs=timeout_secs)
        task.result()


    def async_update_bucket(self, bucket, fields_to_update=None, exp=0,
                          kv_store=1, flag=0, only_store_hash=True,
                          batch_size=1000, pause_secs=1, timeout_secs=30):
        """Update data asynchronously on given bucket. Function don't wait for
        load data to finish, return immediately.
        @param bucket: bucket where to load data.
        @param fields_to_update: list of fields to update in loaded JSON
        @param value_size: size of the one item.
        @param exp: expiration value.
        @param kv_store: kv store index.
        @param flag:
        @param only_store_hash: True to store hash of item else False.
        @param batch_size: batch size for load data at a time.
        @param pause_secs: pause for next batch load.
        @param timeout_secs: timeout
        @return: task object
        """
        perc = 30
        self._kv_gen[OPS.UPDATE] = copy.deepcopy(self._kv_gen[OPS.CREATE])
        self._kv_gen[OPS.UPDATE].start = 0
        self._kv_gen[OPS.UPDATE].end = int(self._kv_gen[OPS.CREATE].end
                                                        * (float)(perc)/100)
        self._kv_gen[OPS.UPDATE].update(fields_to_update=fields_to_update)

        task = self.__clusterop.async_load_gen_docs(
            self.__master_node, bucket.name, self._kv_gen[OPS.UPDATE],
            bucket.kvs[kv_store],OPS.UPDATE, exp, flag, only_store_hash,
            batch_size, pause_secs,timeout_secs)
        return task

    def update_delete_data(
            self, op_type, fields_to_update=None, perc=30, expiration=0,
            wait_for_expiration=True):
        """Perform update/delete operation on all buckets. Function wait
        operation to finish.
        @param op_type: OPS.CREATE/OPS.UPDATE/OPS.DELETE
        @param fields_to_update: list of fields to be updated in the JSON
        @param perc: percentage of data to be deleted or created
        @param expiration: time for expire items
        @param wait_for_expiration: True if wait for expire of items after
        update else False
        """
        tasks = self.async_update_delete(op_type, fields_to_update, perc, expiration)

        [task.result() for task in tasks]

        if wait_for_expiration and expiration:
            self.__log.info("Waiting for expiration of updated items")
            time.sleep(expiration)

    def async_update_delete(
            self, op_type, fields_to_update=None, perc=30, expiration=0,
            kv_store=1):
        """Perform update/delete operation on all buckets. Function don't wait
        operation to finish.
        @param op_type: OPS.CREATE/OPS.UPDATE/OPS.DELETE
        @param fields_to_update: list of fields to be updated in JSON
        @param perc: percentage of data to be deleted or created
        @param expiration: time for expire items
        @return: task object list
        """
        raise_if(
            OPS.CREATE not in self._kv_gen,
            FTSException(
                "Data is not loaded in cluster.Load data before update/delete")
        )
        tasks = []
        for bucket in self.__buckets:
            if op_type == OPS.UPDATE:
                self._kv_gen[OPS.UPDATE] = copy.deepcopy(self._kv_gen[OPS.CREATE])
                self._kv_gen[OPS.UPDATE].start = 0
                self._kv_gen[OPS.UPDATE].end = int(self._kv_gen[OPS.CREATE].end
                                                                * (float)(perc)/100)
                self._kv_gen[OPS.UPDATE].update(fields_to_update=fields_to_update)
                gen = self._kv_gen[OPS.UPDATE]
            elif op_type == OPS.DELETE:
                self._kv_gen[OPS.DELETE] = JsonDocGenerator(
                                                self._kv_gen[OPS.CREATE].name,
                                                op_type= OPS.DELETE,
                                                encoding="utf-8",
                                                start=int((self._kv_gen[OPS.CREATE].end)
                                                          * (float)(100 - perc) / 100),
                                                end=self._kv_gen[OPS.CREATE].end)
                gen = copy.deepcopy(self._kv_gen[OPS.DELETE])
            else:
                raise FTSException("Unknown op_type passed: %s" % op_type)

            self.__log.info("At bucket '{0}' @ {1}: operation: {2}, key range {3} - {4}".
                       format(bucket.name, self.__name, op_type, gen.start, gen.end-1))
            tasks.append(
                self.__clusterop.async_load_gen_docs(
                    self.__master_node,
                    bucket.name,
                    gen,
                    bucket.kvs[kv_store],
                    op_type,
                    expiration,
                    batch_size=1000)
            )
        return tasks

    def async_run_fts_query_compare(self, fts_index, es, query_index, es_index_name=None):
        """
        Asynchronously run query against FTS and ES and compare result
        note: every task runs a single query
        """
        task = self.__clusterop.async_run_fts_query_compare(fts_index=fts_index,
                                                            es_instance=es,
                                                            query_index=query_index,
                                                            es_index_name= es_index_name)
        return task

    def run_expiry_pager(self, val=10):
        """Run expiry pager process and set interval to 10 seconds
        and wait for 10 seconds.
        @param val: time in seconds.
        """
        for bucket in self.__buckets:
            ClusterOperationHelper.flushctl_set(
                self.__master_node,
                "exp_pager_stime",
                val,
                bucket)
            self.__log.info("wait for expiry pager to run on all these nodes")
        time.sleep(val)

    def disable_compaction(self, bucket=BUCKET_NAME.DEFAULT):
        """Disable view compaction
        @param bucket: bucket name.
        """
        new_config = {"viewFragmntThresholdPercentage": None,
                      "dbFragmentThresholdPercentage": None,
                      "dbFragmentThreshold": None,
                      "viewFragmntThreshold": None}
        self.__clusterop.modify_fragmentation_config(
            self.__master_node,
            new_config,
            bucket)


    def __async_rebalance_out(self, master=False, num_nodes=1):
        """Rebalance-out nodes from Cluster
        @param master: True if rebalance-out master node only.
        @param num_nodes: number of nodes to rebalance-out from cluster.
        """
        raise_if(
            len(self.__nodes) <= num_nodes,
            FTSException(
                "Cluster needs:{0} nodes for rebalance-out, current: {1}".
                format((num_nodes + 1), len(self.__nodes)))
        )
        if master:
            to_remove_node = [self.__master_node]
        else:
            to_remove_node = self.__nodes[-num_nodes:]
        self.__log.info(
            "Starting rebalance-out nodes:{0} at {1} cluster {2}".format(
                to_remove_node, self.__name, self.__master_node.ip))
        task = self.__clusterop.async_rebalance(
            self.__nodes,
            [],
            to_remove_node)

        [self.__nodes.remove(node) for node in to_remove_node]

        if master:
            self.__master_node = self.__nodes[0]

        return task

    def async_rebalance_out_master(self):
        return self.__async_rebalance_out(master=True)

    def async_rebalance_out(self, num_nodes=1):
        return self.__async_rebalance_out(num_nodes=num_nodes)

    def rebalance_out_master(self):
        task = self.__async_rebalance_out(master=True)
        task.result()

    def rebalance_out(self, num_nodes=1):
        task = self.__async_rebalance_out(num_nodes=num_nodes)
        task.result()

    def async_rebalance_in(self, num_nodes=1, services=None):
        """Rebalance-in nodes into Cluster asynchronously
        @param num_nodes: number of nodes to rebalance-in to cluster.
        """
        raise_if(
            len(FloatingServers._serverlist) < num_nodes,
            FTSException(
                "Number of free nodes: {0} is not preset to add {1} nodes.".
                format(len(FloatingServers._serverlist), num_nodes))
        )
        to_add_node = []
        for _ in range(num_nodes):
            to_add_node.append(FloatingServers._serverlist.pop())
        self.__log.info(
            "Starting rebalance-in nodes:{0} at {1} cluster {2}".format(
                to_add_node, self.__name, self.__master_node.ip))
        task = self.__clusterop.async_rebalance(self.__nodes, to_add_node, [],
                                                services=services)
        self.__nodes.extend(to_add_node)
        return task

    def rebalance_in(self, num_nodes=1, services=None):
        """Rebalance-in nodes
        @param num_nodes: number of nodes to add to cluster.
        """
        task = self.async_rebalance_in(num_nodes, services=services)
        task.result()

    def __async_swap_rebalance(self, master=False, services=None):
        """Swap-rebalance nodes on Cluster
        @param master: True if swap-rebalance master node else False.
        """
        if master:
            to_remove_node = [self.__master_node]
        else:
            to_remove_node = [self.__nodes[-1]]

        to_add_node = [FloatingServers._serverlist.pop()]

        self.__log.info(
            "Starting swap-rebalance [remove_node:{0}] -> [add_node:{1}] at"
            " {2} cluster {3}"
            .format(to_remove_node[0].ip, to_add_node[0].ip, self.__name,
                    self.__master_node.ip))
        task = self.__clusterop.async_rebalance(
            self.__nodes,
            to_add_node,
            to_remove_node,
            services=services)

        [self.__nodes.remove(node) for node in to_remove_node]
        self.__nodes.extend(to_add_node)

        if master:
            self.__master_node = self.__nodes[0]

        return task

    def async_swap_rebalance_master(self, services=None):
        """
           Returns without waiting for swap rebalance to complete
        """
        return self.__async_swap_rebalance(master=True, services=services)

    def async_swap_rebalance(self, services=None):
        return self.__async_swap_rebalance(services=services)

    def swap_rebalance_master(self, services=None):
        """Swap rebalance master node and wait
        """
        task = self.__async_swap_rebalance(master=True, services=services)
        task.result()

    def swap_rebalance(self, services=None):
        """Swap rebalance non-master node
        """
        task = self.__async_swap_rebalance(services=services)
        task.result()

    def __async_failover(self, master=False, num_nodes=1, graceful=False):
        """Failover nodes from Cluster
        @param master: True if failover master node only.
        @param num_nodes: number of nodes to rebalance-out from cluster.
        @param graceful: True if graceful failover else False.
        """
        raise_if(
            len(self.__nodes) <= 1,
            FTSException(
                "More than 1 node required in cluster to perform failover")
        )
        if master:
            self.__fail_over_nodes = [self.__master_node]
        else:
            self.__fail_over_nodes = self.__nodes[-num_nodes:]

        self.__log.info(
            "Starting failover for nodes:{0} at {1} cluster {2}".format(
                self.__fail_over_nodes, self.__name, self.__master_node.ip))
        task = self.__clusterop.async_failover(
            self.__nodes,
            self.__fail_over_nodes,
            graceful)

        return task

    def async_failover(self, num_nodes=1, graceful=False):
        return self.__async_failover(num_nodes=num_nodes, graceful=graceful)

    def failover_and_rebalance_master(self, graceful=False, rebalance=True):
        """Failover master node
        @param graceful: True if graceful failover else False
        @param rebalance: True if do rebalance operation after failover.
        """
        task = self.__async_failover(master=True, graceful=graceful)
        task.result()
        if graceful:
            # wait for replica update
            time.sleep(60)
            # use rebalance stats to monitor failover
            RestConnection(self.__master_node).monitorRebalance()
        if rebalance:
            self.rebalance_failover_nodes()
        self.__master_node = self.__nodes[0]

    def failover_and_rebalance_nodes(self, num_nodes=1, graceful=False,
                                     rebalance=True):
        """ Failover non-master nodes
        @param num_nodes: number of nodes to failover.
        @param graceful: True if graceful failover else False
        @param rebalance: True if do rebalance operation after failover.
        """
        task = self.__async_failover(
            master=False,
            num_nodes=num_nodes,
            graceful=graceful)
        task.result()
        if graceful:
            time.sleep(60)
            # use rebalance stats to monitor failover
            RestConnection(self.__master_node).monitorRebalance()
        if rebalance:
            self.rebalance_failover_nodes()

    def rebalance_failover_nodes(self):
        self.__clusterop.rebalance(self.__nodes, [], self.__fail_over_nodes)
        [self.__nodes.remove(node) for node in self.__fail_over_nodes]
        self.__fail_over_nodes = []

    def add_back_node(self, recovery_type=None, services=None):
        """add-back failed-over node to the cluster.
            @param recovery_type: delta/full
        """
        raise_if(
            len(self.__fail_over_nodes) < 1,
            FTSException("No failover nodes available to add_back")
        )
        rest = RestConnection(self.__master_node)
        server_nodes = rest.node_statuses()
        for failover_node in self.__fail_over_nodes:
            for server_node in server_nodes:
                if server_node.ip == failover_node.ip:
                    rest.add_back_node(server_node.id)
                    if recovery_type:
                        rest.set_recovery_type(
                            otpNode=server_node.id,
                            recoveryType=recovery_type)
        for node in self.__fail_over_nodes:
            if node not in self.__nodes:
                self.__nodes.append(node)
        self.__clusterop.rebalance(self.__nodes, [], [], services=services)
        self.__fail_over_nodes = []

    def warmup_node(self, master=False):
        """Warmup node on cluster
        @param master: True if warmup master-node else False.
        """
        from random import randrange

        if master:
            warmup_node = self.__master_node

        else:
            warmup_node = self.__nodes[
                randrange(
                    1, len(
                        self.__nodes))]
        NodeHelper.do_a_warm_up(warmup_node)
        return warmup_node

    def reboot_one_node(self, test_case, master=False):
        from random import randrange

        if master:
            reboot_node = self.__master_node

        else:
            reboot_node = self.__nodes[
                randrange(
                    1, len(
                        self.__nodes))]
        NodeHelper.reboot_server(reboot_node, test_case)
        return reboot_node

    def restart_couchbase_on_all_nodes(self):
        for node in self.__nodes:
            NodeHelper.do_a_warm_up(node)

        NodeHelper.wait_warmup_completed(self.__nodes)


    def wait_for_flusher_empty(self, timeout=60):
        """Wait for disk queue to completely flush.
        """
        tasks = []
        for node in self.__nodes:
            for bucket in self.__buckets:
                tasks.append(
                    self.__clusterop.async_wait_for_stats(
                        [node],
                        bucket,
                        '',
                        'ep_queue_size',
                        '==',
                        0))
        for task in tasks:
            task.result(timeout)


class FTSBaseTest(unittest.TestCase):

    def setUp(self):
        unittest.TestCase.setUp(self)
        self._input = TestInputSingleton.input
        self.elastic_node = self._input.elastic
        self.log = logger.Logger.get_logger()
        self.__init_logger()
        self.__cluster_op = Cluster()
        self.__init_parameters()

        self.log.info(
            "==== FTSbasetests setup is started for test #{0} {1} ===="
            .format(self.__case_number, self._testMethodName))

        # workaround for MB-16794
        #self.sleep(30, "working around MB-16794")

        self.__setup_for_test()

        self.log.info(
            "==== FTSbasetests setup is finished for test #{0} {1} ===="
            .format(self.__case_number, self._testMethodName))

    def __is_test_failed(self):
        return (hasattr(self, '_resultForDoCleanups')
                and len(self._resultForDoCleanups.failures
                        or self._resultForDoCleanups.errors)) \
            or (hasattr(self, '_exc_info')
                and self._exc_info()[1] is not None)

    def __is_cleanup_not_needed(self):
        return ((self.__is_test_failed() and
               self._input.param("stop-on-failure", False)) or
               self._input.param("skip-cleanup", False))


    def __is_cluster_run(self):
        return len(set([server.ip for server in self._input.servers])) == 1

    def tearDown(self):
        """Clusters cleanup"""
        if self._input.param("negative_test", False):
            if hasattr(self, '_resultForDoCleanups') \
                and len(self._resultForDoCleanups.failures
                        or self._resultForDoCleanups.errors):
                self._resultForDoCleanups.failures = []
                self._resultForDoCleanups.errors = []
                self.log.info("This is marked as a negative test and contains "
                              "errors as expected, hence not failing it")
            else:
                raise FTSException("Negative test passed!")

        if self._input.param("get-fts-diags", True) and self.__is_test_failed():
            self.grab_fts_diag()

        # collect logs before tearing down clusters
        if self._input.param("get-cbcollect-info", False) and \
                self.__is_test_failed():
            for server in self._input.servers:
                self.log.info("Collecting logs @ {0}".format(server.ip))
                NodeHelper.collect_logs(server)

        try:
            if self.__is_cleanup_not_needed():
                self.log.warn("CLEANUP WAS SKIPPED")
                return
            self.log.info(
                "====  FTSbasetests cleanup is started for test #{0} {1} ===="
                .format(self.__case_number, self._testMethodName))
            self._cb_cluster.cleanup_cluster(self)
            if self.compare_es:
                self.teardown_es()
            self.log.info(
                "====  FTSbasetests cleanup is finished for test #{0} {1} ==="
                .format(self.__case_number, self._testMethodName))
        finally:
            self.__cluster_op.shutdown(force=True)
            unittest.TestCase.tearDown(self)

    def __init_logger(self):
        if self._input.param("log_level", None):
            self.log.setLevel(level=0)
            for hd in self.log.handlers:
                if str(hd.__class__).find('FileHandler') != -1:
                    hd.setLevel(level=logging.DEBUG)
                else:
                    hd.setLevel(
                        level=getattr(
                            logging,
                            self._input.param(
                                "log_level",
                                None)))

    def __setup_for_test(self):
        use_hostanames = self._input.param("use_hostnames", False)
        no_buckets = self._input.param("no_buckets", False)
        master =  self._input.servers[0]
        first_node = copy.deepcopy(master)
        self._cb_cluster = CouchbaseCluster("C1",
                                             [first_node],
                                             self.log,
                                             use_hostanames)
        self.__cleanup_previous()
        if self.compare_es:
            self.setup_es()
        self._cb_cluster.init_cluster(self._cluster_services,
                                      self._input.servers[1:])
        self.__set_free_servers()
        if not no_buckets:
            self.__create_buckets()
        self._master = self._cb_cluster.get_master_node()


    def construct_serv_list(self,serv_str):
        """
            Constructs a list of node services
            to rebalance into cluster
            @param serv_str: like "D,D+F,I+Q,F" where the letters
                             stand for services defined in serv_dict
            @return services_list: like ['kv', 'kv,fts', 'index,n1ql','index']
        """
        serv_dict = {'D': 'kv','F': 'fts','I': 'index','Q': 'n1ql'}
        for letter, serv in serv_dict.iteritems():
            serv_str = serv_str.replace(letter, serv)
        services_list = re.split('[-,:]', serv_str)
        for index, serv in enumerate(services_list):
           services_list[index] = serv.replace('+', ',')
        return services_list

    def __init_parameters(self):
        self.__case_number = self._input.param("case_number", 0)
        self.__num_sasl_buckets = self._input.param("sasl_buckets", 0)
        self.__num_stand_buckets = self._input.param("standard_buckets", 0)
        self.__eviction_policy = self._input.param("eviction_policy",'valueOnly')
        self.__mixed_priority = self._input.param("mixed_priority", None)

        self.__fail_on_errors = self._input.param("fail_on_errors", False)
        # simply append to this list, any error from log we want to fail test on
        self.__report_error_list = []
        if self.__fail_on_errors:
            self.__report_error_list = [""]

        # for format {ip1: {"panic": 2}}
        self.__error_count_dict = {}
        if len(self.__report_error_list) > 0:
            self.__initialize_error_count_dict()

        # Public init parameters - Used in other tests too.
        # Move above private to this section if needed in future, but
        # Ensure to change other tests too.

        # TODO: when FTS build is available, change the following to "D,D+F,F"
        self._cluster_services = \
            self.construct_serv_list(self._input.param("cluster", "D,D+F,F"))
        self._num_replicas = self._input.param("replicas", 1)
        self._create_default_bucket = self._input.param("default_bucket",True)
        self._num_items = self._input.param("items", 1000)
        self._value_size = self._input.param("value_size", 512)
        self._poll_timeout = self._input.param("poll_timeout", 120)
        self._update = self._input.param("update", False)
        self._delete = self._input.param("delete", False)
        self._perc_upd = self._input.param("upd", 30)
        self._perc_del = self._input.param("del", 30)
        self._expires = self._input.param("expires", 0)
        self._wait_for_expiration = self._input.param(
            "wait_for_expiration",
            True)
        self._warmup = self._input.param("warm", "")
        self._rebalance = self._input.param("rebalance", "")
        self._failover = self._input.param("failover", "")
        self._wait_timeout = self._input.param("timeout", 60)
        self._disable_compaction = self._input.param("disable_compaction","")
        self._item_count_timeout = self._input.param("item_count_timeout", 300)
        self._dgm_run = self._input.param("dgm_run", False)
        self._active_resident_threshold = \
            self._input.param("active_resident_threshold", 100)
        CHECK_AUDIT_EVENT.CHECK = self._input.param("verify_audit", 0)
        self._max_verify = self._input.param("max_verify", 100000)
        self._num_vbuckets = self._input.param("vbuckets", 1024)
        self.lang = self._input.param("lang", "EN")
        self.encoding = self._input.param("encoding", "utf-8")
        self.analyzer = self._input.param("analyzer", None)
        self.index_replicas = self._input.param("index_replicas", None)
        self.index_kv_store = self._input.param("kvstore", None)
        self.partitions_per_pindex = \
            self._input.param("max_partitions_pindex", 32)
        self.upd_del_fields = self._input.param("upd_del_fields", None)
        self.num_queries = self._input.param("num_queries", 1)
        self.query_types = (self._input.param("query_types", "match")).split(',')
        self.index_per_bucket = self._input.param("index_per_bucket", 1)
        self.dataset = self._input.param("dataset", "emp")
        self.sample_query = {"match": "Safiya Morgan", "field": "name"}
        self.compare_es = self._input.param("compare_es", False)
        if self.compare_es:
            if not self.elastic_node:
                self.fail("For ES result validation, pls add in the"
                          " [elastic] section in your ini file,"
                          " else set \"compare_es\" as False")
            self.es = ElasticSearchBase(self.elastic_node, self.log)
            if not self.es.is_running():
                self.fail("Could not reach Elastic Search server on %s"
                          % self.elastic_node.ip)
        else:
            self.es = None
        self.create_gen = None

    def __initialize_error_count_dict(self):
        """
            initializes self.__error_count_dict with ip, error and err count
            like {ip1: {"panic": 2, "KEY_ENOENT":3}}
        """
        for node in self._input.servers:
            self.__error_count_dict[node.ip] = {}
            for error in self.__report_error_list:
                self.__error_count_dict[node.ip][error] = \
                    NodeHelper.check_fts_log(node, error)
        self.log.info(self.__error_count_dict)

    def __cleanup_previous(self):
        self._cb_cluster.cleanup_cluster(self, cluster_shutdown=False)

    def __set_free_servers(self):
        total_servers = self._input.servers
        cluster_nodes = self._cb_cluster.get_nodes()
        FloatingServers._serverlist = [
            server for server in total_servers if server not in cluster_nodes]

    def __calculate_bucket_size(self, cluster_quota, num_buckets):
        dgm_run = self._input.param("dgm_run", 0)
        if dgm_run:
            # buckets cannot be created if size<100MB
            bucket_size = 256
        else:
            bucket_size = int((float(cluster_quota) - 500)/float(num_buckets))
        return bucket_size

    def __create_buckets(self):
        # if mixed priority is set by user, set high priority for sasl and
        # standard buckets
        if self.__mixed_priority:
            bucket_priority = 'high'
        else:
            bucket_priority = None
        num_buckets = self.__num_sasl_buckets + \
            self.__num_stand_buckets + int(self._create_default_bucket)

        total_quota = self._cb_cluster.get_mem_quota()
        bucket_size = self.__calculate_bucket_size(
                total_quota,
                num_buckets)

        if self._create_default_bucket:
            self._cb_cluster.create_default_bucket(
                    bucket_size,
                    self._num_replicas,
                    eviction_policy=self.__eviction_policy,
                    bucket_priority=bucket_priority)

        self._cb_cluster.create_sasl_buckets(
            bucket_size, num_buckets=self.__num_sasl_buckets,
            num_replicas=self._num_replicas,
            eviction_policy=self.__eviction_policy,
            bucket_priority=bucket_priority)

        self._cb_cluster.create_standard_buckets(
            bucket_size, num_buckets=self.__num_stand_buckets,
            num_replicas=self._num_replicas,
            eviction_policy=self.__eviction_policy,
            bucket_priority=bucket_priority)

    def create_buckets_on_cluster(self):
        # if mixed priority is set by user, set high priority for sasl and
        # standard buckets
        if self.__mixed_priority:
            bucket_priority = 'high'
        else:
            bucket_priority = None
        num_buckets = self.__num_sasl_buckets + \
            self.__num_stand_buckets + int(self._create_default_bucket)

        total_quota = self._cb_cluster.get_mem_quota()
        bucket_size = self.__calculate_bucket_size(
            total_quota,
            num_buckets)

        if self._create_default_bucket:
            self._cb_cluster.create_default_bucket(
                bucket_size,
                self._num_replicas,
                eviction_policy=self.__eviction_policy,
                bucket_priority=bucket_priority)

        self._cb_cluster.create_sasl_buckets(
            bucket_size, num_buckets=self.__num_sasl_buckets,
            num_replicas=self._num_replicas,
            eviction_policy=self.__eviction_policy,
            bucket_priority=bucket_priority)

        self._cb_cluster.create_standard_buckets(
            bucket_size, num_buckets=self.__num_stand_buckets,
            num_replicas=self._num_replicas,
            eviction_policy=self.__eviction_policy,
            bucket_priority=bucket_priority)


    def load_employee_dataset(self, num_items=None):
        """
            Loads the default JSON dataset
            see JsonDocGenerator in documentgenerator.py
        """
        if not num_items:
            num_items = self._num_items
        if not self._dgm_run:
            self._cb_cluster.load_all_buckets(num_items, self._value_size)

        else:
            self._cb_cluster.load_all_buckets_till_dgm(
                active_resident_threshold=self._active_resident_threshold,
                items=self._num_items)

    def load_utf16_data(self, num_keys=None):
        """
        Loads the default JSON dataset in utf-16 format
        """
        if not num_keys:
            num_keys = self._num_items

        gen = JsonDocGenerator("C1",
                               encoding="utf-16",
                               start=0,
                               end=num_keys)
        self._cb_cluster.load_all_buckets_from_generator(gen)


    def load_wiki(self, num_keys=None, lang="EN", encoding="utf-8"):
        """
        Loads the Wikipedia dump.
        Languages supported : EN(English)/ES(Spanish)/DE(German)/FR(French)
        """
        if not num_keys:
            num_keys = self._num_items

        gen = WikiJSONGenerator("wiki",
                                  lang=lang,
                                  encoding=encoding,
                                  start=0,
                                  end=num_keys)
        self._cb_cluster.load_all_buckets_from_generator(gen)

    def perform_update_delete(self, fields_to_update=None):
        """
          Call this method to perform updates/deletes on your cluster.
          It checks if update=True or delete=True params were passed in
          the test.
          @param fields_to_update - list of fields to update in JSON
        """
        # UPDATES
        if self._update:
            self.log.info("Updating keys @ {0}".format(self._cb_cluster.get_name()))
            self._cb_cluster.update_delete_data(
                OPS.UPDATE,
                fields_to_update=fields_to_update,
                perc=self._perc_upd,
                expiration=self._expires,
                wait_for_expiration=self._wait_for_expiration)

        # DELETES
        if self._delete:
            self.log.info("Deleting keys @ {0}".format(self._cb_cluster.get_name()))
            self._cb_cluster.update_delete_data(OPS.DELETE, perc=self._perc_del)

    def async_perform_update_delete(self, fields_to_update=None):
        """
          Call this method to perform updates/deletes on your cluster.
          It checks if update=True or delete=True params were passed in
          the test.
          @param fields_to_update - list of fields to update in JSON
        """
        tasks = []
        # UPDATES
        if self._update:
            self.log.info("Updating keys @ {0} with expiry={1}".
                          format(self._cb_cluster.get_name(), self._expires))
            tasks.extend(self._cb_cluster.async_update_delete(
                OPS.UPDATE,
                fields_to_update=fields_to_update,
                perc=self._perc_upd,
                expiration=self._expires))

        [task.result() for task in tasks]
        if tasks:
            self.log.info("Batched updates loaded to cluster(s)")

        tasks = []
        # DELETES
        if self._delete:
            self.log.info("Deleting keys @ {0}".format(self._cb_cluster.get_name()))
            tasks.extend(
                self._cb_cluster.async_update_delete(
                    OPS.DELETE,
                    perc=self._perc_del))

        [task.result() for task in tasks]
        if tasks:
            self.log.info("Batched deletes sent to cluster(s)")

        if self._wait_for_expiration and self._expires:
            self.sleep(
                self._expires,
                "Waiting for expiration of updated items")
            self._cb_cluster.run_expiry_pager()



    def print_panic_stacktrace(self, node):
        """ Prints panic stacktrace from goxdcr.log*
        """
        shell = RemoteMachineShellConnection(node)
        result, err = shell.execute_command("zgrep -A 40 'panic:' {0}/fts.log*".
                            format(NodeHelper.get_log_dir(node)))
        for line in result:
            self.log.info(line)
        shell.disconnect()

    def check_errors_in_fts_logs(self):
        """
        checks if new errors from self.__report_error_list
        were found on any of the goxdcr.logs
        """
        error_found_logger = []
        for node in self._input.servers:
            for error in self.__report_error_list:
                new_error_count = NodeHelper.check_fts_log(node, error)
                self.log.info("Initial {0} count on {1} :{2}, now :{3}".
                            format(error,
                                node.ip,
                                self.__error_count_dict[node.ip][error],
                                new_error_count))
                if (new_error_count  > self.__error_count_dict[node.ip][error]):
                    error_found_logger.append("{0} found on {1}".format(error,
                                                                    node.ip))
                    if "panic" in error:
                        self.print_panic_stacktrace(node)
        if error_found_logger:
            self.log.error(error_found_logger)
        return error_found_logger

    def sleep(self, timeout=1, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    def wait_for_indexing_complete(self):
        """
        Wait for index_count for any index to stabilize
        """
        index_doc_count = 0
        retry = self._input.param("index_retry", 5)
        start_time = time.time()
        for index in self._cb_cluster.get_indexes():
            if index.index_type == "alias":
                continue
            retry_count = retry
            prev_count = 0
            while retry_count > 0:
                index_doc_count = index.get_indexed_doc_count()
                if not self.compare_es:
                    self.log.info("Docs in bucket = %s, docs in FTS index '%s': %s"
                                        %(index.get_src_bucket_doc_count(),
                                        index.name,
                                        index.get_indexed_doc_count()))
                else:
                    self.es.update_index('es_index')
                    self.log.info("Docs in bucket = %s, docs in FTS index '%s':"
                                  " %s, docs in ES index: %s "
                                % (index.get_src_bucket_doc_count(),
                                index.name,
                                index.get_indexed_doc_count(),
                                self.es.get_index_count('es_index')))

                if prev_count < index_doc_count or prev_count > index_doc_count:
                    prev_count = index_doc_count
                    retry_count = retry
                else:
                    retry_count -= 1
                time.sleep(10)
        self.log.info("FTS indexed %s docs in %s mins, now breaking loop after 60s of inactivity"
                      % (index_doc_count, round(float((time.time()-60-start_time)/60), 2)))

    def construct_plan_params(self):
        plan_params = {}
        if self.index_replicas:
            plan_params['numReplicas'] = self.index_replicas
        if self.partitions_per_pindex:
            plan_params['maxPartitionsPerPIndex'] = self.partitions_per_pindex
        return plan_params

    def is_index_partitioned_balanced(self, index):
        """
        Perform some plan validation to make sure the index is
        partitioned and balanced on all nodes.
        Check the following -
        1. if number of pindexes = num_vbuckets/max_partitions_per_pindex
        2. if each pindex is servicing not more than max_partitions_per_pindex
        3. if index is distributed - present on all fts nodes, almost equally?
        4. if index balanced - every fts node services almost equal num of vbs?
        """
        _, defn = index.get_index_defn()
        start_time = time.time()
        while 'planPIndexes' not in  defn:
            if time.time() - start_time > 180:
                self.fail("planPIndexes=null for index {0} even after 3 mins"
                          .format(index.name))
            self.sleep(5, "No pindexes found, waiting for index to get created")
            _, defn = index.get_index_defn()

        # check 1 - test number of pindexes
        partitions_per_pindex = index.get_max_partitions_pindex()
        exp_num_pindexes = self._num_vbuckets/partitions_per_pindex
        if self._num_vbuckets % partitions_per_pindex:
            import math
            exp_num_pindexes = math.ceil(
            self._num_vbuckets/partitions_per_pindex + 0.5)
        if len(defn['planPIndexes']) != exp_num_pindexes:
            self.fail("Number of pindexes for %s is %s while"
                      " expected value is %s" %(index.name,
                                                len(defn['planPIndexes']),
                                                exp_num_pindexes))
        self.log.info("Validated: Number of PIndexes = %s" % len(defn['planPIndexes']))

        # check 2 - each pindex servicing "partitions_per_pindex" vbs
        num_fts_nodes = len(self._cb_cluster.get_fts_nodes())
        nodes_partitions = {}
        for pindex in defn['planPIndexes']:
            node = pindex['nodes'].keys()[0]
            if node not in nodes_partitions.keys():
                nodes_partitions[node] = {'pindex_count': 0, 'partitions':[]}
            nodes_partitions[node]['pindex_count'] += 1
            for partition in pindex['sourcePartitions'].split(','):
                nodes_partitions[node]['partitions'].append(partition)
            if len(pindex['sourcePartitions'].split(',')) > \
                    partitions_per_pindex:
                self.fail("sourcePartitions for pindex %s more than "
                          "max_partitions_per_pindex %s" %
                          (pindex['name'], partitions_per_pindex))
        self.log.info("Validated: Every pIndex serves %s partitions or lesser" %
                          partitions_per_pindex)

        # check 3 - distributed - pindex present on all fts nodes?
        if len(nodes_partitions.keys()) != num_fts_nodes:
            self.fail("Index is not properly distributed, pindexes spread across"
                      " %s while fts nodes are %s"
                      % (nodes_partitions.keys(),
                        self._cb_cluster.get_fts_nodes()))
        self.log.info("Validated: pIndexes are distributed across %s " %
                      nodes_partitions.keys())

        # check 4 - balance check(almost equal no of pindexes on all fts nodes)
        exp_partitions_per_node = self._num_vbuckets/num_fts_nodes
        self.log.info("Expecting num of partitions in each node in range %s-%s"
                      % (exp_partitions_per_node - partitions_per_pindex,
                         exp_partitions_per_node + partitions_per_pindex))

        for node, pindex_partitions in nodes_partitions.iteritems():
            if abs(len(pindex_partitions['partitions']) - \
                    exp_partitions_per_node) > partitions_per_pindex:
                self.fail("The source partitions are not evenly distributed "
                          "among nodes, seeing %s on %s"
                          % (len(pindex_partitions['partitions']), node.ip))
            self.log.info("Validated: Node %s houses %s pindexes which serve"
                          " %s partitions" %
                          (node,
                           pindex_partitions['pindex_count'],
                           len(pindex_partitions['partitions'])))
        return True

    def generate_random_queries(self, index, num_queries=1, query_type=["match"],
                              seed=0):
        """
         Calls FTS-ES Query Generator for employee dataset
         @param num_queries: number of queries to return
         @query_type: a list of different types of queries to generate
                      like: query_type=["match", "match_phrase","bool",
                                        "conjunction", "disjunction"]
        """
        from random_query_generator.rand_query_gen import FTSESQueryGenerator
        query_gen = FTSESQueryGenerator(num_queries, query_type=query_type,
                                            seed=seed, dataset=self.dataset,
                                            fields=index.smart_query_fields)
        for fts_query in query_gen.fts_queries:
            index.fts_queries.append(
                json.loads(json.dumps(fts_query, ensure_ascii=False)))

        if self.compare_es:
            for es_query in query_gen.es_queries:
                # unlike fts, es queries are not nested before sending to fts
                # so enclose in query dict here
                es_query = {'query': es_query}
                self.es.es_queries.append(
                    json.loads(json.dumps(es_query, ensure_ascii=False)))
            return index.fts_queries, self.es.es_queries

        return index.fts_queries

    def create_index(self, bucket, index_name, index_params=None,
                             plan_params=None):
        """
        Creates a default index given bucket, index_name and plan_params
        """
        bucket_password = ""
        if bucket.authType == "sasl":
            bucket_password = bucket.saslPassword
        if self.index_kv_store:
            index_params = {"store": {"kvStoreName": self.index_kv_store}}
        if not plan_params:
            plan_params = self.construct_plan_params()
        index = self._cb_cluster.create_fts_index(
            name=index_name,
            source_name=bucket.name,
            index_params=index_params,
            source_params={"authUser": bucket.name,
                           "authPassword": bucket_password},
            plan_params=plan_params)
        self.is_index_partitioned_balanced(index)
        return index

    def create_fts_indexes_all_buckets(self, plan_params=None):
        """
        Creates 'n' default indexes for all buckets.
        'n' is defined by 'index_per_bucket' test param.
        """
        for bucket in self._cb_cluster.get_buckets():
            for count in range(self.index_per_bucket):
                self.create_index(
                    bucket,
                    "%s_index_%s" % (bucket.name, count+1),
                    plan_params=plan_params)

    def create_alias(self, target_indexes, name=None, alias_def=None):
        """
        Creates an alias spanning one or many target indexes
        """
        if not name:
            name = 'alias_%s' % int(time.time())

        if not alias_def:
            alias_def = INDEX_DEFAULTS.ALIAS_DEFINITION
            for index in target_indexes:
                alias_def['targets'][index.name] = {}
                alias_def['targets'][index.name]['indexUUID'] = index.get_uuid()

        return self._cb_cluster.create_fts_index(name=name,
                                                 index_type='fulltext-alias',
                                                 index_params=alias_def)

    def validate_index_count(self, equal_bucket_doc_count=False,
                             zero_rows_ok=True, must_equal=None):
        """
         Handle validation and error logging for docs indexed
         returns a map containing index_names and docs indexed
        """
        index_name_count_map = {}
        for index in self._cb_cluster.get_indexes():
            docs_indexed = index.get_indexed_doc_count()
            bucket_count = self._cb_cluster.get_doc_count_in_bucket(
                                    index.source_bucket)
            self.log.info("Docs in index {0}={1}, bucket docs={2}".
                      format(index.name, docs_indexed, bucket_count))
            if must_equal and docs_indexed != int(must_equal):
                self.fail("Number of docs indexed is not %s" % must_equal)
            if docs_indexed == 0 and not zero_rows_ok:
                self.fail("No docs were indexed for index %s" % index.name)
            if equal_bucket_doc_count:

                if docs_indexed != bucket_count:
                    self.fail("Bucket doc count = %s, index doc count=%s" %
                              (bucket_count, docs_indexed))
            index_name_count_map[index.name] = docs_indexed
        return index_name_count_map

    def setup_es(self):
        """
        Setup Elastic search - create empty index node defined under
        'elastic' section in .ini
        """
        self.create_index_es()

    def teardown_es(self):
        self.es.delete_indices()

    def create_es_index_mapping(self, mapping):
        self.es.create_index_mapping(index_name="es_index",
                                     mapping=mapping)

    def load_data_es_from_generator(self, generator,
                                    index_name="es_index"):
        """
            Loads json docs into ES from a generator, does a blocking load
        """

        for key, doc in generator:
            doc = json.loads(doc)
            self.es.load_data(index_name,
                              json.dumps(doc, encoding='utf-8'),
                              doc['_type'],
                              key)


    def create_index_es(self, index_name="es_index"):
        self.es.create_empty_index(index_name)
        self.log.info("Created empty index %s on Elastic Search node"
                      % index_name)

    def get_generator(self, dataset, num_items, start=0, encoding="utf-8",
                      lang="EN"):
        """
           Returns a generator depending on the dataset
        """
        if dataset == "emp":
            return JsonDocGenerator(name="emp",
                                    encoding=encoding,
                                    start=start,
                                    end=start+num_items)
        elif dataset == "wiki":
            return WikiJSONGenerator(name="wiki",
                                     lang=lang,
                                     encoding=encoding,
                                     start=start,
                                     end=start+num_items)

    def populate_create_gen(self):
        if self.dataset == "emp":
            self.create_gen = self.get_generator(self.dataset, num_items=self._num_items)
        elif self.dataset == "wiki":
            self.create_gen = self.get_generator(self.dataset, num_items=self._num_items)
        elif self.dataset == "all":
            self.create_gen = []
            self.create_gen.append(self.get_generator("emp", num_items=self._num_items/2))
            self.create_gen.append(self.get_generator("wiki", num_items=self._num_items/2))

    def load_data(self):
        """
         Blocking call to load data to Couchbase and ES
        """
        load_tasks = self.async_load_data()
        for task in load_tasks:
            task.result()
        self.log.info("Loading phase complete!")

    def async_load_data(self):
        """
         For use to run with parallel tasks like rebalance, failover etc
        """
        load_tasks = []
        self.populate_create_gen()
        if self.compare_es:
            gen = copy.deepcopy(self.create_gen)
            if isinstance(gen, list):
                for generator in gen:
                    load_tasks.append(self.es.async_bulk_load_ES(index_name='es_index',
                                                        gen=generator,
                                                        op_type='create'))
            else:
                load_tasks.append(self.es.async_bulk_load_ES(index_name='es_index',
                                                        gen=gen,
                                                        op_type='create'))
        load_tasks += self._cb_cluster.async_load_all_buckets_from_generator(
            self.create_gen)
        return load_tasks

    def run_query_and_compare(self, index, es_index_name=None):
        """
        Runs every fts query and es_query and compares them as a single task
        Runs as many tasks as there are queries
        """
        tasks = []
        fail_count = 0
        failed_queries = []
        for count in range(0, len(index.fts_queries)):
            tasks.append(self._cb_cluster.async_run_fts_query_compare(
                fts_index=index,
                es=self.es,
                es_index_name=es_index_name,
                query_index=count))

        num_queries = len(tasks)

        for task in tasks:
            task.result()
            if not task.passed:
                fail_count += 1
                failed_queries.append(task.query_index+1)

        if fail_count:
            self.fail("%s out of %s queries failed! - %s" % (fail_count,
                                                             num_queries,
                                                             failed_queries))
        else:
            self.log.info("SUCCESS: %s out of %s queries passed"
                          %(num_queries-fail_count, num_queries))

    def grab_fts_diag(self):
        """
         Grab fts diag until it is handled by cbcollect info
        """
        from httplib import BadStatusLine
        import os
        import urllib2
        import gzip
        import base64
        path = TestInputSingleton.input.param("logs_folder", "/tmp")
        for serverInfo in self._cb_cluster.get_fts_nodes():
            if not self.__is_cluster_run():
                serverInfo.fts_port = 8094
            self.log.info("Grabbing fts diag from {0}...".format(serverInfo.ip))
            diag_url = "http://{0}:{1}/api/diag".format(serverInfo.ip,
                                                        serverInfo.fts_port)
            self.log.info(diag_url)
            try:
                req = urllib2.Request(diag_url)
                authorization = base64.encodestring('%s:%s' % (
                    self._input.membase_settings.rest_username,
                    self._input.membase_settings.rest_password))
                req.headers = {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Authorization': 'Basic %s' % authorization,
                    'Accept': '*/*'}
                filename = "{0}_fts_diag.json".format(serverInfo.ip)
                page = urllib2.urlopen(req)
                with open(path+'/'+filename, 'wb') as output:
                    os.write(1, "downloading {0} ...".format(serverInfo.ip))
                    while True:
                        buffer = page.read(65536)
                        if not buffer:
                            break
                        output.write(buffer)
                        os.write(1, ".")
                file_input = open('{0}/{1}'.format(path, filename), 'rb')
                zipped = gzip.open("{0}/{1}.gz".format(path, filename), 'wb')
                zipped.writelines(file_input)
                file_input.close()
                zipped.close()
                os.remove(path+'/'+filename)
                print "downloaded and zipped diags @ : {0}/{1}".format(path,
                                                                       filename)
            except urllib2.URLError as error:
                print "unable to obtain fts diags from {0}".format(diag_url)
            except BadStatusLine:
                print "unable to obtain fts diags from {0}".format(diag_url)
            except Exception as e:
                print "unable to obtain fts diags from {0} :{1}".format(diag_url, e)
