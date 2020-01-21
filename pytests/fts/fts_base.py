"""
Base class for FTS/CBFT/Couchbase Full Text Search
"""

import unittest
import time
import copy
import logger
import logging
import re
import json

from couchbase_helper.cluster import Cluster
from membase.api.rest_client import RestConnection, Bucket
from membase.api.exception import ServerUnavailableException
from remote.remote_util import RemoteMachineShellConnection
from remote.remote_util import RemoteUtilHelper
from testconstants import STANDARD_BUCKET_PORT, LINUX_COUCHBASE_BIN_PATH, WIN_COUCHBASE_BIN_PATH, \
    MAC_COUCHBASE_BIN_PATH
from membase.helper.cluster_helper import ClusterOperationHelper
from couchbase_helper.stats_tools import StatsCommon
from membase.helper.bucket_helper import BucketOperationHelper
from memcached.helper.data_helper import MemcachedClientHelper
from TestInput import TestInputSingleton
from scripts.collect_server_info import cbcollectRunner
from couchbase_helper.documentgenerator import *

from couchbase_helper.documentgenerator import JsonDocGenerator
from lib.membase.api.exception import FTSException
from .es_base import ElasticSearchBase
from security.rbac_base import RbacBase
from lib.couchbase_helper.tuq_helper import N1QLHelper


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
            "analysis": {}
        }
    }

    ALIAS_DEFINITION = {"targets": {}}

    PLAN_PARAMS = {}

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
            # "timeout": 60000  Optional timeout( 10000 by default).
            # it's better to get rid of hardcoding
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
    def reboot_server(server, test_case, wait_timeout=120):
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
        if shell.extract_remote_info().type.lower() == OS.WINDOWS:
            time.sleep(wait_timeout * 5)
        else:
            time.sleep(wait_timeout//6)
        while True:
            try:
                # disable firewall on these nodes
                NodeHelper.disable_firewall(server)
                break
            except BaseException:
                print("Node not reachable yet, will try after 10 secs")
                time.sleep(10)
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
    def start_couchbase(server):
        """Warmp up server
        """
        shell = RemoteMachineShellConnection(server)
        shell.start_couchbase()
        shell.disconnect()

    @staticmethod
    def stop_couchbase(server):
        """Warmp up server
        """
        shell = RemoteMachineShellConnection(server)
        shell.stop_couchbase()
        shell.disconnect()

    @staticmethod
    def set_cbft_env_fdb_options(server):
        shell = RemoteMachineShellConnection(server)
        shell.stop_couchbase()
        cmd = "sed -i 's/^export CBFT_ENV_OPTIONS.*$/" \
              "export CBFT_ENV_OPTIONS=bleveMaxResultWindow=10000000," \
              "forestdbCompactorSleepDuration={0},forestdbCompactionThreshold={1}/g'\
              /opt/couchbase/bin/couchbase-server".format(
            int(TestInputSingleton.input.param("fdb_compact_interval", None)),
            int(TestInputSingleton.input.param("fdb_compact_threshold", None)))
        shell.execute_command(cmd)
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
                                "Still warming up .. ep_warmup_key_count : %s" % (
                                    mc.stats("warmup")["ep_warmup_key_count"]))
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
        while num < wait_time // 10:
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
        shell.start_couchbase()
        shell.disconnect()
        NodeHelper.wait_warmup_completed([server])

    @staticmethod
    def kill_memcached(server):
        """Kill memcached process running on server.
        """
        shell = RemoteMachineShellConnection(server)
        shell.kill_memcached()
        shell.disconnect()

    @staticmethod
    def kill_cbft_process(server):
        NodeHelper._log.info("Killing cbft on server: {0}".format(server))
        shell = RemoteMachineShellConnection(server)
        shell.kill_cbft_process()
        shell.disconnect()

    @staticmethod
    def get_log_dir(node):
        """Gets couchbase log directory, even for cluster_run
        """
        _, dir = RestConnection(node).diag_eval(
            'filename:absname(element(2, application:get_env(ns_server,error_logger_mf_dir))).')
        return str(dir)

    @staticmethod
    def get_data_dir(node):
        """Gets couchbase data directory, even for cluster_run
        """
        _, dir = RestConnection(node).diag_eval(
            'filename:absname(element(2, application:get_env(ns_server,path_config_datadir))).')

        return str(dir).replace('\"', '')

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
        print("grabbing cbcollect from {0}".format(server.ip))
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
        self.index_storage_type = TestInputSingleton.input.param("index_type", None)
        self.num_pindexes = 0
        self.index_definition = {
            "type": "fulltext-index",
            "name": "",
            "uuid": "",
            "params": {},
            "sourceType": "couchbase",
            "sourceName": "default",
            "sourceUUID": "",
            "planParams": {}
        }
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

        # Support for custom map
        self.custom_map = TestInputSingleton.input.param("custom_map", False)
        self.num_custom_analyzers = TestInputSingleton.input.param("num_custom_analyzers", 0)
        self.multiple_filters = TestInputSingleton.input.param("multiple_filters", False)
        self.cm_id = TestInputSingleton.input.param("cm_id", 0)
        if self.custom_map:
            self.generate_new_custom_map(seed=self.cm_id)

        self.fts_queries = []

        if index_params:
            self.index_definition['params'] = \
                self.build_custom_index_params(index_params)

        if plan_params:
            self.index_definition['planParams'] = \
                self.build_custom_plan_params(plan_params)

        if source_params:
            self.index_definition['sourceParams'] = {}
            self.index_definition['sourceParams'] = source_params

        if source_uuid:
            self.index_definition['sourceUUID'] = source_uuid

        self.index_definition['params']['store'] = {
            "kvStoreName": "mossStore",
            "mossStoreOptions": {}
        }

        if self.index_storage_type :
            self.index_definition['params']['store']['indexType'] =  self.index_storage_type

        if TestInputSingleton.input.param("num_snapshots_to_keep", None):
            self.index_definition['params']['store']['numSnapshotsToKeep'] = int(
                    TestInputSingleton.input.param(
                        "num_snapshots_to_keep",
                        None)
                    )

        if TestInputSingleton.input.param("level_compaction", None):
            self.index_definition['params']['store']['mossStoreOptions']= {
                "CompactionLevelMaxSegments": 9,
                "CompactionPercentage": 0.6,
                "CompactionLevelMultiplier": 3
            }

        if TestInputSingleton.input.param("moss_compact_threshold", None):
            self.index_definition['params']['store']\
                ['mossStoreOptions']['CompactionPercentage'] = int(
                    TestInputSingleton.input.param(
                        "moss_compact_threshold",
                        None)
                    )

        if TestInputSingleton.input.param("memory_only", None):
            self.index_definition['params']['store'] = \
                {"kvStoreName": "moss",
                 "mossLowerLevelStoreName": ""}

        self.moss_enabled = TestInputSingleton.input.param("moss", True)
        if not self.moss_enabled:
            if 'store' not in list(self.index_definition['params'].keys()):
                self.index_definition['params']['store'] = {}
            self.index_definition['params']['store']['kvStoreMossAllow'] = False

    def is_scorch(self):
        return self.get_index_type() == "scorch"

    def is_upside_down(self):
        return self.get_index_type() == "upside_down"

    def is_type_unspecified(self):
        return self.get_index_type() ==  None

    def get_index_type(self):
        try:
            _, defn = self.get_index_defn()
            index_type = defn['indexDef']['params']['store']['indexType']
            self.__log.info("Index type of {0} is {1}".
                          format(self.name,
                                 defn['indexDef']['params']['store']['indexType']))
            return index_type
        except Exception:
            self.__log.error("No 'indexType' present in index definition")
            return None

    def generate_new_custom_map(self, seed):
        from .custom_map_generator.map_generator import CustomMapGenerator
        cm_gen = CustomMapGenerator(seed=seed, dataset=self.dataset,
                                num_custom_analyzers=self.num_custom_analyzers,
                                multiple_filters=self.multiple_filters)
        fts_map, self.es_custom_map = cm_gen.get_map()
        self.smart_query_fields = cm_gen.get_smart_query_fields()
        print(self.smart_query_fields)
        self.index_definition['params'] = self.build_custom_index_params(
            fts_map)
        if self.num_custom_analyzers > 0:
            custom_analyzer_def = cm_gen.build_custom_analyzer()
            self.index_definition["params"]["mapping"]["analysis"] = \
                                                    custom_analyzer_def
        self.__log.info(json.dumps(self.index_definition["params"],
                                   indent=3))

    def update_custom_analyzer(self, seed):
        """
        This method will update the custom analyzer in an index definition in 3 ways -
        1) delete custom analyzer
        2) remove a custom filter
        3) change the custom analyzer used
        """

        delete_custom_analyzer = TestInputSingleton.input.param \
            ("delete_custom_analyzer", False)
        delete_custom_filter = TestInputSingleton.input.param \
            ("delete_custom_filter", False)

        # Deleting custom analyzer in use
        if delete_custom_analyzer:
            self.index_definition["params"]["mapping"]["analysis"] = {}
        else:
            if delete_custom_filter:
                custom_filters = self.index_definition["params"]["mapping"] \
                    ["analysis"]["analyzers"]["customAnalyzer1"]["token_filters"]
                for custom_filter in custom_filters:
                    self.__log.info("custom filter = " + custom_filter)
                    del self.index_definition['params']['mapping']['analysis'] \
                        ['token_filters'][custom_filter]
            else:
                from .custom_map_generator.map_generator import CustomMapGenerator
                cm_gen = CustomMapGenerator(seed=seed, dataset=self.dataset,
                                    num_custom_analyzers=self.num_custom_analyzers,
                                    multiple_filters=self.multiple_filters)
                if self.num_custom_analyzers > 0:
                    custom_analyzer_def = cm_gen.build_custom_analyzer()
                    self.index_definition["params"]["mapping"]["analysis"] = \
                        custom_analyzer_def

    def build_custom_index_params(self, index_params):
        if self.index_type == "fulltext-index":
            mapping = INDEX_DEFAULTS.BLEVE_MAPPING
            if self.custom_map:
                if not TestInputSingleton.input.param("default_map", False):
                    mapping['mapping']['default_mapping']['enabled'] = False
            mapping['mapping'].update(index_params)
        else:
            mapping = {"targets": {}}
            mapping.update(index_params)
        return mapping

    def build_custom_plan_params(self, plan_params):
        plan = INDEX_DEFAULTS.PLAN_PARAMS
        plan.update(plan_params)
        return plan

    def add_child_field_to_default_mapping(self, field_name, field_type,
                                           field_alias=None, analyzer=None):
        """
        This method will add a field mapping to a default mapping
        """
        fields = str.split(field_name, '.')
        nesting_level = len(fields)

        child_map = {}
        child_map['dynamic'] = False
        child_map['enabled'] = True
        child_map['properties'] = {}

        child_field = {}
        child_field['dynamic'] = False
        child_field['enabled'] = True
        if not field_alias:
            field_alias = fields[len(fields) - 1]
        child_field['fields'] = [
            {
                "analyzer": analyzer,
                "display_order": "0",
                "include_in_all": True,
                "include_term_vectors": True,
                "index": True,
                "name": field_alias,
                "store": True,
                "type": field_type
            }
        ]

        field_maps = []
        field_maps.append(child_field)

        if nesting_level > 1:
            for x in range(0, nesting_level - 1):
                field = fields.pop()
                # Do a deepcopy of child_map into field_map since we dont
                # want to have child_map altered because of changes on field_map
                field_map = copy.deepcopy(child_map)
                field_map['properties'][field] = field_maps.pop()
                field_maps.append(field_map)

        map = {}
        if 'mapping' not in self.index_definition['params']:
            map['default_mapping'] = {}
            map['default_mapping']['properties'] = {}
            map['default_mapping']['dynamic'] = False
            map['default_mapping']['enabled'] = True
            map['default_mapping']['properties'][fields.pop()] = field_maps.pop()
            self.index_definition['params']['mapping'] = map
        else:
            self.index_definition['params']['mapping']['default_mapping'] \
                ['properties'][fields.pop()] = field_maps.pop()

    def add_analyzer_to_existing_field_map(self, field_name, field_type,
                                           field_alias=None, analyzer=None):
        """
        Add another field mapping with a different analyzer to an existing field map.
        Can be enhanced to update other fields as well if required.
        """
        fields = str.split(field_name, '.')

        if not field_alias:
            field_alias = fields[len(fields) - 1]

        child_field = {
            "analyzer": analyzer,
            "display_order": "0",
            "include_in_all": True,
            "include_term_vectors": True,
            "index": True,
            "name": field_alias,
            "store": True,
            "type": field_type
        }

        map = copy.deepcopy(self.index_definition['params']['mapping']
                                    ['default_mapping']['properties'])

        map = self.update_nested_field_mapping(fields[len(fields) - 1],
                                                        child_field, map)
        self.index_definition['params']['mapping']['default_mapping'] \
                                                    ['properties'] = map

    def update_nested_field_mapping(self, key, value, map):
        """
        Recurse through a given nested field mapping, and append the leaf node with the specified value.
        Can be enhanced to update the current value as well if required.
        """
        for k, v in map.items():
            if k == key:
                map[k]['fields'].append(value)
                return map
            else:
                if 'properties' in map[k]:
                    map[k]['properties'] = \
                        self.update_nested_field_mapping(key, value,
                                                         map[k]['properties'])
        return map

    def add_type_mapping_to_index_definition(self, type, analyzer):
        """
        Add Type Mapping to Index Definition (and disable default mapping)
        """
        type_map = {}
        type_map[type] = {}
        type_map[type]['default_analyzer'] = analyzer
        type_map[type]['display_order'] = 0
        type_map[type]['dynamic'] = True
        type_map[type]['enabled'] = True

        if 'mapping' not in self.index_definition['params']:
            self.index_definition['params']['mapping'] = {}
            self.index_definition['params']['mapping']['default_mapping'] = {}
            self.index_definition['params']['mapping']['default_mapping'] \
                                                        ['properties'] = {}
            self.index_definition['params']['mapping']['default_mapping'] \
                                                        ['dynamic'] = False

        self.index_definition['params']['mapping']['default_mapping'] \
                                                        ['enabled'] = False
        if 'types' not in self.index_definition['params']['mapping']:
            self.index_definition['params']['mapping']['types'] = {}
            self.index_definition['params']['mapping']['types'] = type_map
        else:
            self.index_definition['params']['mapping']['types'][type] = type_map[type]


    def add_doc_config_to_index_definition(self, mode):
        """
        Add Document Type Configuration to Index Definition
        Note: These regexps have been constructed keeping
        travel-sample dataset in mind (keys like 'airline_1023')
        """
        doc_config = {}

        if mode == 'docid_regexp1':
            doc_config['mode'] = 'docid_regexp'
            doc_config['docid_regexp'] = "([^_]*)"

        if mode == 'docid_regexp2':
            doc_config['mode'] = 'docid_regexp'
            # a seq of 6 or more letters
            doc_config['docid_regexp'] = "\\b[a-z]{6,}"

        if mode == 'docid_regexp_neg1':
            doc_config['mode'] = 'docid_regexp'
            # a seq of 8 or more letters
            doc_config['docid_regexp'] = "\\b[a-z]{8,}"

        if mode == 'docid_prefix':
            doc_config['mode'] = 'docid_prefix'
            doc_config['docid_prefix_delim'] = "_"

        if mode == 'docid_prefix_neg1':
            doc_config['mode'] = 'docid_prefix'
            doc_config['docid_prefix_delim'] = "-"

        if mode == 'type_field':
            doc_config['mode'] = 'type_field'
            doc_config['type_field'] = "type"

        if mode == 'type_field_neg1':
            doc_config['mode'] = 'type_field'
            doc_config['type_field'] = "newtype"

        self.index_definition['params']['doc_config'] = {}
        self.index_definition['params']['doc_config'] = doc_config

    def get_rank_of_doc_in_search_results(self, content, doc_id):
        """
        Fetch rank of a given document in Search Results
        """
        try:
            return content.index(doc_id) + 1
        except Exception as err:
            self.__log.info("Doc ID %s not found in search results." % doc_id)
            return -1

    def create(self, rest=None):
        self.__log.info("Checking if index already exists ...")
        if not rest:
            rest = RestConnection(self.__cluster.get_random_fts_node())
        status, _ = rest.get_fts_index_definition(self.name)
        if status != 400:
            rest.delete_fts_index(self.name)
        self.__log.info("Creating {0} {1} on {2}".format(
            self.index_type,
            self.name,
            rest.ip))
        rest.create_fts_index(self.name, self.index_definition)
        self.__cluster.get_indexes().append(self)

    def update(self, rest=None):
        if not rest:
            rest = RestConnection(self.__cluster.get_random_fts_node())
        self.__log.info("Updating {0} {1} on {2}".format(
            self.index_type,
            self.name,
            rest.ip))
        rest.update_fts_index(self.name, self.index_definition)
        self.__log.info("sleeping for 200")
        time.sleep(200)

    def update_index_to_upside_down(self):
        if self.is_upside_down():
            self.__log.info("The index {0} is already upside_down index, conversion not needed!")
        else:
            self.index_definition['params']['store']['indexType'] = "upside_down"
            self.index_definition['uuid'] = self.get_uuid()
            self.update()
            time.sleep(5)
            _, defn = self.get_index_defn()
            if defn['indexDef']['params']['store']['indexType'] == "upside_down":
                self.__log.info("SUCCESS: The index type is now upside_down!")
            else:
                self.__log.error("defn['indexDef']['params']['store']['indexType']")
                raise Exception("Unable to convert index to upside_down")

    def update_index_to_scorch(self):
        if self.is_scorch():
            self.__log.info("The index {0} is already scorch index, conversion not needed!")
        else:
            self.index_definition['params']['store']['indexType'] = "scorch"
            self.index_definition['uuid'] = self.get_uuid()
            self.update()
            time.sleep(5)
            _, defn = self.get_index_defn()
            if defn['indexDef']['params']['store']['indexType'] == "scorch":
                self.__log.info("SUCCESS: The index type is now scorch!")
            else:
                self.__log.error("defn['indexDef']['params']['store']['indexType']")
                raise Exception("Unable to convert index to scorch")

    def update_num_pindexes(self, new):
        self.index_definition['planParams']['maxPartitionsPerPIndex'] = new
        self.index_definition['uuid'] = self.get_uuid()
        self.update()

    def update_num_replicas(self, new):
        self.index_definition['planParams']['numReplicas'] = new
        self.index_definition['uuid'] = self.get_uuid()
        self.update()

    def delete(self, rest=None):
        if not rest:
            rest = RestConnection(self.__cluster.get_random_fts_node())
        self.__log.info("Deleting {0} {1} on {2}".format(
            self.index_type,
            self.name,
            rest.ip))
        status = rest.delete_fts_index(self.name)
        if status:
            self.__cluster.get_indexes().remove(self)
            if not self.__cluster.are_index_files_deleted_from_disk(self.name):
                self.__log.error("Status: {0} but index file for {1} not yet "
                                 "deleted!".format(status, self.name))
            else:
                self.__log.info("Validated: all index files for {0} deleted from "
                                "disk".format(self.name))
        else:
            raise FTSException("Index/alias {0} not deleted".format(self.name))

    def get_index_defn(self, rest=None):
        if not rest:
            rest = RestConnection(self.__cluster.get_random_fts_node())
        return rest.get_fts_index_definition(self.name)

    def get_max_partitions_pindex(self):
        _, defn = self.get_index_defn()
        return int(defn['indexDef']['planParams']['maxPartitionsPerPIndex'])

    def clone(self, clone_name):
        pass

    def get_indexed_doc_count(self, rest=None):
        if not rest:
            rest = RestConnection(self.__cluster.get_random_fts_node())
        return rest.get_fts_index_doc_count(self.name)

    def get_num_mutations_to_index(self, rest=None):
        if not rest:
            rest = RestConnection(self.__cluster.get_random_fts_node())
        status, stat_value = rest.get_fts_stats(index_name=self.name,
                                                bucket_name=self._source_name,
                                                stat_name='num_mutations_to_index')
        return stat_value

    def get_src_bucket_doc_count(self):
        return self.__cluster.get_doc_count_in_bucket(self.source_bucket)

    def get_uuid(self):
        rest = RestConnection(self.__cluster.get_random_fts_node())
        return rest.get_fts_index_uuid(self.name)

    def construct_cbft_query_json(self, query, fields=None, timeout=60000,
                                                          facets=False,
                                                          sort_fields=None,
                                                          explain=False,
                                                          show_results_from_item=0,
                                                          highlight=False,
                                                          highlight_style=None,
                                                          highlight_fields=None,
                                                          consistency_level='',
                                                          consistency_vectors={},
                                                          score=''):
        max_matches = TestInputSingleton.input.param("query_max_matches", 10000000)
        max_limit_matches = TestInputSingleton.input.param("query_limit_matches", None)
        query_json = copy.deepcopy(QUERY.JSON)
        # query is a unicode dict
        query_json['query'] = query
        query_json['indexName'] = self.name
        query_json['explain'] = explain
        if max_matches is not None and max_matches != 'None':
            query_json['size'] = int(max_matches)
        else:
            del query_json['size']
        if max_limit_matches is not None:
            query_json['limit'] = int(max_limit_matches)
        if show_results_from_item:
            query_json['from'] = int(show_results_from_item)
        if timeout is not None:
            query_json['ctl']['timeout'] = int(timeout)
        if fields:
            query_json['fields'] = fields
        if facets:
            query_json['facets'] = self.construct_facets_definition()
        if sort_fields:
            query_json['sort'] = sort_fields
        if highlight:
            query_json['highlight'] = {}
            if highlight_style:
                query_json['highlight']['style'] = highlight_style
            if highlight_fields:
                query_json['highlight']['fields'] = highlight_fields
        if consistency_level is None:
            del query_json['ctl']['consistency']['level']
        else:
            query_json['ctl']['consistency']['level'] = consistency_level
        if consistency_vectors is None:
            del query_json['ctl']['consistency']['vectors']
        elif consistency_vectors != {}:
            query_json['ctl']['consistency']['vectors'] = consistency_vectors
        if score != '':
            query_json['score'] = "none"
        return query_json

    def construct_facets_definition(self):
        """
        Constructs the facets definition of the query json
        """
        facets = TestInputSingleton.input.param("facets", None).split(",")
        size = TestInputSingleton.input.param("facets_size", 10)
        terms_field = "dept"
        terms_facet_name = "Department"
        numeric_range_field = "salary"
        numeric_range_facet_name = "Salaries"
        date_range_field = "join_date"
        date_range_facet_name = "No. of Years"
        facet_definition = {}

        date_range_buckets = [
            {"name": "1 year", "start": "2015-08-01"},
            {"name": "2-5 years", "start": "2011-08-01", "end": "2015-07-31"},
            {"name": "6-10 years", "start": "2006-08-01", "end": "2011-07-31"},
            {"name": "10+ years", "end": "2006-07-31"}
        ]

        numeric_range_buckets = [
            {"name": "high salary", "min": 150001},
            {"name": "average salary", "min": 110001, "max": 150000},
            {"name": "low salary", "max": 110000}
        ]

        for facet in facets:
            if facet == 'terms':
                facet_definition[terms_facet_name] = {}
                facet_definition[terms_facet_name]['field'] = terms_field
                facet_definition[terms_facet_name]['size'] = size

            if facet == 'numeric_ranges':
                facet_definition[numeric_range_facet_name] = {}
                facet_definition[numeric_range_facet_name]['field'] = \
                                                    numeric_range_field
                facet_definition[numeric_range_facet_name]['size'] = size
                facet_definition[numeric_range_facet_name]['numeric_ranges'] = []
                for bucket in numeric_range_buckets:
                    facet_definition[numeric_range_facet_name] \
                                  ['numeric_ranges'].append(bucket)

            if facet == 'date_ranges':
                facet_definition[date_range_facet_name] = {}
                facet_definition[date_range_facet_name]['field'] = \
                                                date_range_field
                facet_definition[date_range_facet_name]['size'] = size
                facet_definition[date_range_facet_name]['date_ranges'] = []
                for bucket in date_range_buckets:
                    facet_definition[date_range_facet_name] \
                                    ['date_ranges'].append(bucket)

        return facet_definition

    def execute_query(self, query, zero_results_ok=True, expected_hits=None,
                      return_raw_hits=False, sort_fields=None,
                      explain=False, show_results_from_item=0, highlight=False,
                      highlight_style=None, highlight_fields=None, consistency_level='',
                      consistency_vectors={}, timeout=60000, rest=None, score='', expected_no_of_results=None):
        """
        Takes a query dict, constructs a json, runs and returns results
        """
        query_dict = self.construct_cbft_query_json(query,
                                                    sort_fields=sort_fields,
                                                    explain=explain,
                                                    show_results_from_item=show_results_from_item,
                                                    highlight=highlight,
                                                    highlight_style=highlight_style,
                                                    highlight_fields=highlight_fields,
                                                    consistency_level=consistency_level,
                                                    consistency_vectors=consistency_vectors,
                                                    timeout=timeout,
                                                    score=score)

        hits = -1
        matches = []
        doc_ids = []
        time_taken = 0
        status = {}
        try:
            if timeout == 0:
                # force limit in 10 min in case timeout=0(no timeout)
                rest_timeout = 600
            else:
                rest_timeout = timeout//1000 + 10
            hits, matches, time_taken, status = \
                self.__cluster.run_fts_query(self.name, query_dict, timeout=rest_timeout)
        except ServerUnavailableException:
            # query time outs
            raise ServerUnavailableException
        except Exception as e:
            self.__log.error("Error running query: %s" % e)
        if hits:
            for doc in matches:
                doc_ids.append(doc['id'])
        if int(hits) == 0 and not zero_results_ok:
            self.__log.info("ERROR: 0 hits returned!")
            raise FTSException("No docs returned for query : %s" % query_dict)
        if expected_hits and expected_hits != hits:
            self.__log.info("ERROR: Expected hits: %s, fts returned: %s"
                           % (expected_hits, hits))
            raise FTSException("Expected hits: %s, fts returned: %s"
                               % (expected_hits, hits))
        if expected_hits and expected_hits == hits:
            self.__log.info("SUCCESS! Expected hits: %s, fts returned: %s"
                            % (expected_hits, hits))
        if expected_no_of_results is not None:
            if expected_no_of_results == doc_ids.__len__():
                self.__log.info("SUCCESS! Expected number of results: %s, fts returned: %s"
                            % (expected_no_of_results, doc_ids.__len__()))
            else:
                self.__log.info("ERROR! Expected number of results: %s, fts returned: %s"
                                % (expected_no_of_results, doc_ids.__len__()))
                print(doc_ids)
                raise FTSException("Expected number of results: %s, fts returned: %s"
                                   % (expected_no_of_results, doc_ids.__len__()))

        if not return_raw_hits:
            return hits, doc_ids, time_taken, status
        else:
            return hits, matches, time_taken, status

    def execute_query_with_facets(self, query, zero_results_ok=True,
                                  expected_hits=None):
        """
        Takes a query dict with facet definition, constructs a json,
        runs and returns results
        """
        query_dict = self.construct_cbft_query_json(query, facets=True)
        hits = -1
        matches = []
        doc_ids = []
        time_taken = 0
        status = {}
        facets = []
        try:
            hits, matches, time_taken, status, facets = \
                self.__cluster.run_fts_query_with_facets(self.name, query_dict)

        except ServerUnavailableException:
            # query time outs
            raise ServerUnavailableException
        except Exception as e:
            self.__log.error("Error running query: %s" % e)
        if hits:
            for doc in matches:
                doc_ids.append(doc['id'])
        if int(hits) == 0 and not zero_results_ok:
            raise FTSException("No docs returned for query : %s" % query_dict)
        if expected_hits and expected_hits != hits:
            raise FTSException("Expected hits: %s, fts returned: %s"
                               % (expected_hits, hits))
        if expected_hits and expected_hits == hits:
            self.__log.info("SUCCESS! Expected hits: %s, fts returned: %s"
                            % (expected_hits, hits))
        return hits, doc_ids, time_taken, status, facets

    def validate_facets_in_search_results(self, no_of_hits, facets_returned):
        """
        Validate the facet data returned in the query response JSON.
        """
        facets = TestInputSingleton.input.param("facets", None).split(",")
        size = TestInputSingleton.input.param("facets_size", 10)
        field_indexed = TestInputSingleton.input.param("field_indexed", True)
        terms_facet_name = "Department"
        numeric_range_facet_name = "Salaries"
        date_range_facet_name = "No. of Years"

        for facet in facets:
            if facet == 'terms':
                facet_name = terms_facet_name
            if facet == 'numeric_ranges':
                facet_name = numeric_range_facet_name
            if facet == 'date_ranges':
                facet_name = date_range_facet_name

            # Validate Facet name
            if facet_name not in facets_returned:
                raise FTSException(facet_name + " not present in the "
                                                "search results")

            # Validate Total No. with no. of hits. It can be unequal if
            # the field is not indexed, but not otherwise.
            total_count = facets_returned[facet_name]['total']
            missing_count = facets_returned[facet_name]['missing']
            others_count = facets_returned[facet_name]['other']
            if not total_count == no_of_hits:
                if field_indexed:
                    raise FTSException("Total count of results in " + facet_name
                                   + " Facet (" + str(total_count) +
                                   ") is not equal to total hits in search "
                                   "results (" + str(no_of_hits) + ")")
                else:
                    if not ((missing_count == no_of_hits) and (total_count == 0)):
                        raise FTSException("Field not indexed, but counts "
                                           "are not expected")

            # Validate only if there are some search results
            if not total_count == 0:
                # Validate no. of terms returned, and it should be <= size
                no_of_buckets_in_facet = len(facets_returned[facet_name] \
                                                                [facet])
                if no_of_buckets_in_facet > size:
                    raise FTSException("Total no. of buckets in facets (" +
                                       no_of_buckets_in_facet +
                                       ") exceeds the size defined ("
                                       + str(size) + ")")

                # Validate count in each facet and total it up.
                # Should be Total - missing - others
                total_count_in_buckets = 0
                for bucket in facets_returned[facet_name][facet]:
                    self.__log.info(bucket)
                    total_count_in_buckets += bucket['count']

                if not total_count_in_buckets == (total_count - missing_count -
                                                      others_count):
                    raise FTSException("Total count (%d) in buckets not correct"
                                       % total_count_in_buckets)

                if not self.validate_query_run_with_facet_data\
                            (query=TestInputSingleton.input.param("query", ""),
                             facets_returned=facets_returned, facet_type=facet):
                    raise FTSException("Requerying returns different results "
                                       "than expected")
            else:
                self.__log.info("Zero total count in facet.")

        self.__log.info("Validated Facets in search results")

    def validate_query_run_with_facet_data(self, query, facets_returned,
                                           facet_type):
        """
        Form a query based on the facet data and check the # hits.
        """
        if facet_type == 'terms':
            facet_name = 'Department'
            field_name = 'dept'
            value = facets_returned[facet_name][facet_type][0]['term']
            expected_hits = facets_returned[facet_name][facet_type][0]['count']
            new_query = "{\"conjuncts\" :[" + query + ",{\"match\":\"" + \
                        value + "\", \"field\":\"" + field_name + "\"}]}"

        if facet_type == 'numeric_ranges':
            facet_name = 'Salaries'
            field_name = 'salary'
            max_value = None
            min_value = None
            min_value_query = ""
            max_value_query = ""
            try:
                max_value = facets_returned[facet_name][facet_type][0]['max']
                max_value_query = ",{\"inclusive_max\":true, \"field\":\"" \
                                  + field_name + "\", \"max\":" + \
                                  str(max_value) + "}"
            except:
                self.__log.info("max key doesnt exist for Salary facet")

            try:
                min_value = facets_returned[facet_name][facet_type][0]['min']
                min_value_query = ",{\"inclusive_min\":true, \"field\":\"" \
                                  + field_name + "\", \"min\":" + \
                                  str(min_value) + "}"
            except:
                self.__log.info("min key doesnt exist for Salary facet")

            expected_hits = facets_returned[facet_name][facet_type][0]['count']

            new_query = "{\"conjuncts\" :[" + query + min_value_query + \
                        max_value_query + "]}"

        if facet_type == 'date_ranges':
            facet_name = 'No. of Years'
            field_name = 'join_date'
            end_value = None
            start_value = None
            start_value_query = ""
            end_value_query = ""
            try:
                end_value = facets_returned[facet_name][facet_type][0]['end']
                end_value_query = ",{\"inclusive_end\":true, \"field\":\"" + \
                                  field_name + "\", \"end\":\"" + \
                                  end_value + "\"}"
            except:
                self.__log.info("end key doesnt exist for No. of Years facet")

            try:
                start_value = facets_returned[facet_name][facet_type][0]['start']
                start_value_query = ",{\"inclusive_start\":true, \"field\":\"" \
                                    + field_name + "\", \"start\":\"" + \
                                    start_value + "\"}"
            except:
                self.__log.info("start key doesnt exist for No. of Years facet")

            expected_hits = facets_returned[facet_name][facet_type][0]['count']

            new_query = "{\"conjuncts\" :[" + query + end_value_query + \
                        start_value_query + "]}"

        self.__log.info(new_query)
        new_query = json.loads(new_query)
        hits, _, _, _ = self.execute_query(query=new_query,
                                           zero_results_ok=True,
                                           expected_hits=expected_hits)
        if not hits == expected_hits:
            return False
        else:
            return True

    def validate_sorted_results(self, raw_hits, sort_fields):
        """
        Validate if the docs returned in the search result match the expected values
        """
        result = False

        expected_docs = TestInputSingleton.input.param("expected", None)
        docs = []
        # Fetch the Doc IDs from raw_hits
        for doc in raw_hits:
            docs.append(doc['id'])

        if expected_docs:
            expected_docs = expected_docs.split(',')
            # Compare docs with the expected values.
            if docs == expected_docs:
                result = True
            else:
                # Sometimes, if there are two docs with same field value, their rank
                # may be interchanged. To handle this, if the actual doc order
                # doesn't match the expected value, swap the two such docs and then
                # try to match
                tolerance = TestInputSingleton.input.param("tolerance", None)
                if tolerance:
                    tolerance = tolerance.split(',')
                    index1, index2 = expected_docs.index(
                        tolerance[0]), expected_docs.index(tolerance[1])
                    expected_docs[index1], expected_docs[index2] = expected_docs[
                                                                       index2], \
                                                                   expected_docs[
                                                                       index1]
                    if docs == expected_docs:
                        result = True
                    else:
                        self.__log.info("Actual docs returned : %s", docs)
                        self.__log.info("Expected docs : %s", expected_docs)
                        return False
                else:
                    self.__log.info("Actual docs returned : %s", docs)
                    self.__log.info("Expected docs : %s", expected_docs)
                    return False
        else :
            self.__log.info("Expected doc order not specified. It is a negative"
                            " test, so skipping order validation")
            result = True

        # Validate the sort fields in the result
        for doc in raw_hits:
            if 'sort' in list(doc.keys()):
                if not sort_fields and len(doc['sort']) == 1:
                    result &= True
                elif len(doc['sort']) == len(sort_fields):
                    result &= True
                else:
                    self.__log.info("Sort fields do not match for the following document - ")
                    self.__log.info(doc)
                    return False

        return result

    def validate_snippet_highlighting_in_result_content(self, contents, doc_id,
                                                        field_names, terms,
                                                        highlight_style=None):
        '''
        Validate the snippets and highlighting in the result content for a given
        doc id
        :param contents: Result contents
        :param doc_id: Doc ID to check highlighting/snippet for
        :param field_names: Field name for which term is to be validated
        :param terms: search term which should be highlighted
        :param highlight_style: Expected highlight style - ansi/html
        :return: True/False
        '''
        validation = True
        for content in contents:
            if content['id'] == doc_id:
                # Check if Location section is present for the document in the search results
                if 'locations' in content:
                    validation &= True
                else:
                    self.__log.info(
                        "Locations not present in the search result")
                    validation &= False

                # Check if Fragments section is present in the document in the search results
                # If present, check if the search term is highlighted
                if 'fragments' in content:
                    snippet = content['fragments'][field_names][0]

                    # Replace the Ansi highlight tags with <mark> since the
                    # ansi ones render themselves hence cannot be compared.
                    if highlight_style == 'ansi':
                        snippet = snippet.replace('\x1b[43m', '<mark>').replace(
                            '\x1b[0m', '</mark>')
                    search_term = '<mark>' + terms + '</marks>'

                    found = snippet.find(search_term)

                    if not found:
                        self.__log.info("Search term not highlighted")
                    validation &= found
                else:
                    self.__log.info(
                        "Fragments not present in the search result")
                    validation &= False

        # If the test is a negative testcase to check if snippet, flip the result
        if TestInputSingleton.input.param("negative_test", False):
            validation = ~validation
        return validation

    def validate_snippet_highlighting_in_result_content_n1ql(self, contents, doc_id,
                                                        field_names, terms,
                                                        highlight_style=None):
        '''
        Validate the snippets and highlighting in the result content for a given
        doc id
        :param contents: Result contents
        :param doc_id: Doc ID to check highlighting/snippet for
        :param field_names: Field name for which term is to be validated
        :param terms: search term which should be highlighted
        :param highlight_style: Expected highlight style - ansi/html
        :return: True/False
        '''
        validation = True
        for content in contents:
            if content['meta']['id'] == doc_id:
                # Check if Location section is present for the document in the search results
                if 'locations' in content['meta']:
                    validation &= True
                else:
                    self.__log.info(
                        "Locations not present in the search result")
                    validation &= False

                # Check if Fragments section is present in the document in the search results
                # If present, check if the search term is highlighted
                if 'fragments' in content['meta']:
                    snippet = content['meta']['fragments'][field_names][0]
                    # Replace the Ansi highlight tags with <mark> since the
                    # ansi ones render themselves hence cannot be compared.
                    if highlight_style == 'ansi':
                        snippet = snippet.replace('\x1b[43m', '<mark>').replace(
                            '\x1b[0m', '</mark>')
                    search_term = '<mark>' + terms + '</mark>'
                    found = snippet.find(search_term)
                    if found < 0:
                        self.__log.info("Search term not highlighted")
                    validation &= (found>=0)
                else:
                    self.__log.info(
                        "Fragments not present in the search result")
                    validation &= False

        # If the test is a negative testcase to check if snippet, flip the result
        if TestInputSingleton.input.param("negative_test", False):
            validation = ~validation
        return validation


    def get_score_from_query_result_content(self, contents, doc_id):
        for content in contents:
            if content['id'] == doc_id:
                return content['score']

    def is_doc_present_in_query_result_content(self, contents, doc_id):
        for content in contents:
            if content['id'] == doc_id:
                return True
        return False

    def get_detailed_scores_for_doc(self, doc_id, search_results, weight,
                                    searchTerm):
        """
        Parses the search results content and extracts the desired score component
        :param doc_id: Doc ID for which detailed score is requested
        :param search_results: Search results contents
        :param weight: component of score - queryWeight/fieldWeight/coord
        :param searchTerm: searchTerm for which score component is required
        :return: Individual Score components
        """
        tf_score = 0
        idf_score = 0
        field_norm_score = 0
        coord_score = 0
        query_norm_score = 0
        for doc in search_results:
            if doc['id'] == doc_id:
                if 'children' in doc['explanation']:
                    tree = self.find_node_in_score_tree(
                        doc['explanation']['children'], weight, searchTerm)
                    if 'children' in tree:
                        tf_score, field_norm_score, idf_score, query_norm_score, \
                            coord_score = self.extract_detailed_score_from_node(
                            tree['children'])
                    else:
                        nodes = []
                        nodes.append(tree)
                        tf_score, field_norm_score, idf_score, query_norm_score, \
                            coord_score = self.extract_detailed_score_from_node(
                            nodes)
                else:
                    tf_score, field_norm_score, idf_score, query_norm_score, \
                        coord_score = self.extract_detailed_score_from_node(
                        doc['explanation'])

        return tf_score, field_norm_score, idf_score, query_norm_score, coord_score

    def find_node_in_score_tree(self, tree, weight, searchTerm):
        """
        Finds the node that contains the desired score component in the tree
        structure containing the score explanation
        """
        while True:
            newSubnodes = []
            for node in tree:
                if (weight in node['message']) and (
                            searchTerm in node['message']):
                    self.__log.info("Found it")
                    return node
                if 'children' in node:
                    if len(node['children']) == 0:
                        break
                    for subnode in node['children']:
                        if (weight in subnode['message']) and (
                                    searchTerm in subnode['message']):
                            self.__log.info("Found it")
                            return subnode
                        else:
                            if 'children' in subnode:
                                for subsubnode in subnode['children']:
                                    newSubnodes.append(subsubnode)
            tree = copy.deepcopy(newSubnodes)
        return None

    def extract_detailed_score_from_node(self, tree):
        """
        Extracts the score components from the node containing it.
        """
        tf_score = 0
        idf_score = 0
        field_norm_score = 0
        coord_score = 0
        query_norm_score = 0
        for item in tree:
            if 'termFreq' in item['message']:
                tf_score = item['value']
            if 'fieldNorm' in item['message']:
                field_norm_score = item['value']
            if 'idf' in item['message']:
                idf_score = item['value']
            if 'queryNorm' in item['message']:
                query_norm_score = item['value']
            if 'coord' in item['message']:
                coord_score = item['value']
        return tf_score, field_norm_score, idf_score, query_norm_score, coord_score


class CouchbaseCluster:
    def __init__(self, name, nodes, log, use_hostname=False, sdk_compression=True):
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
        # to avoid querying certain nodes that undergo crash/reboot scenarios
        self.__bypass_fts_nodes = []
        self.__bypass_n1ql_nodes = []
        self.__separate_nodes_on_services()
        self.__set_fts_ram_quota()
        self.sdk_compression = sdk_compression

    def __str__(self):
        return "Couchbase Cluster: %s, Master Ip: %s" % (
            self.__name, self.__master_node.ip)

    def __set_fts_ram_quota(self):
        fts_quota = TestInputSingleton.input.param("fts_quota", None)
        if fts_quota:
            RestConnection(self.__master_node).set_fts_ram_quota(fts_quota)

    def get_node(self, ip, port):
        for node in self.__nodes:
            if ip == node.ip and port == node.port:
                return node

    def get_logger(self):
        return self.__log

    def is_cluster_run(self):
        cluster_run = False
        for server in self.__nodes:
            if server.ip == "127.0.0.1":
                cluster_run = True
        return cluster_run

    def __separate_nodes_on_services(self):
        self.__fts_nodes = []
        self.__n1ql_nodes = []
        self.__non_fts_nodes = []
        service_map = RestConnection(self.__master_node).get_nodes_services()
        for node_ip, services in service_map.items():
            if self.is_cluster_run():
                # if cluster-run and ip not 127.0.0.1
                ip = "127.0.0.1"
            else:
                ip = node_ip.rsplit(':', 1)[0]
            node = self.get_node(ip, node_ip.rsplit(':', 1)[1])
            if node:
                if "fts" in services:
                    self.__fts_nodes.append(node)
                else:
                    self.__non_fts_nodes.append(node)

                if "n1ql" in services:
                    self.__n1ql_nodes.append(node)

    def get_fts_nodes(self):
        self.__separate_nodes_on_services()
        return self.__fts_nodes

    def get_num_fts_nodes(self):
        return len(self.get_fts_nodes())

    def get_non_fts_nodes(self):
        self.__separate_nodes_on_services()
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

    def set_bypass_fts_node(self, node):
        self.__bypass_fts_nodes.append(node)

    def get_random_node(self):
        return self.__nodes[random.randint(0, len(self.__nodes) - 1)]

    def get_random_fts_node(self):
        self.__separate_nodes_on_services()
        for node in self.__bypass_fts_nodes:
            self.__fts_nodes.remove(node)
        if not self.__fts_nodes:
            raise FTSException("No node in the cluster has 'fts' service"
                               " enabled")
        if len(self.__fts_nodes) == 1:
            return self.__fts_nodes[0]
        return self.__fts_nodes[random.randint(0, len(self.__fts_nodes) - 1)]

    def get_random_n1ql_node(self):
        self.__separate_nodes_on_services()
        for node in self.__bypass_n1ql_nodes:
            self.__n1ql_nodes.remove(node)
        if not self.__n1ql_nodes:
            raise FTSException("No node in the cluster has 'n1ql' service"
                               " enabled")
        if len(self.__n1ql_nodes) == 1:
            return self.__n1ql_nodes[0]
        return self.__n1ql_nodes[random.randint(0, len(self.__n1ql_nodes) - 1)]

    def get_random_non_fts_node(self):
        return self.__non_fts_nodes[random.randint(0, len(self.__fts_nodes) - 1)]

    def are_index_files_deleted_from_disk(self, index_name):
        nodes = self.get_fts_nodes()
        for node in nodes:
            data_dir = RestConnection(node).get_data_path()
            shell = RemoteMachineShellConnection(node)
            count = -1
            retry = 0
            while count != 0:
                count, err = shell.execute_command(
                    "ls {0}/@fts |grep ^{1} | wc -l".
                        format(data_dir, index_name))
                if isinstance(count, list):
                    count = int(count[0])
                else:
                    count = int(count)
                self.__log.info(count)
                time.sleep(2)
                retry += 1
                if retry > 5:
                    files, err = shell.execute_command(
                        "ls {0}/@fts |grep ^{1}".
                            format(data_dir, index_name))
                    self.__log.info(files)
                    return False
        return True

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

        if len(cluster_services) - 1 > len(available_nodes):
            raise FTSException("Only %s nodes present for given cluster"
                               "configuration %s"
                               % (len(available_nodes) + 1, cluster_services))
        self.__init_nodes()
        if available_nodes:
            nodes_to_add = []
            node_services = []
            node_num = 0
            for index, node_service in enumerate(cluster_services):
                if index == 0 and node_service == "kv":
                    continue
                self.__log.info("%s will be configured with services %s" % (
                    available_nodes[node_num].ip,
                    node_service))
                nodes_to_add.append(available_nodes[node_num])
                node_services.append(node_service)
                node_num = node_num + 1
            try:
                self.__clusterop.async_rebalance(
                    self.__nodes,
                    nodes_to_add,
                    [],
                    use_hostnames=self.__use_hostname,
                    services=node_services).result()
            except Exception as e:
                raise FTSException("Unable to initialize cluster with config "
                                   "%s: %s" % (cluster_services, e))

            self.__nodes += nodes_to_add
        self.__separate_nodes_on_services()
        if not self.is_cluster_run() and \
                (TestInputSingleton.input.param("fdb_compact_interval", None) or \
                         TestInputSingleton.input.param("fdb_compact_threshold", None)):
            for node in self.__fts_nodes:
                NodeHelper.set_cbft_env_fdb_options(node)

    def cleanup_cluster(
            self,
            test_case,
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
        try:
            self.__log.info("Removing user 'cbadminbucket'...")
            RbacBase().remove_user_role(['cbadminbucket'], RestConnection(
                self.__master_node))
        except Exception as e:
            self.__log.info(e)

    def _create_bucket_params(self, server, replicas=1, size=0, port=11211, password=None,
                             bucket_type='membase', enable_replica_index=1, eviction_policy='valueOnly',
                             bucket_priority=None, flush_enabled=1, lww=False, maxttl=None):
        """Create a set of bucket_parameters to be sent to all of the bucket_creation methods
        Parameters:
            server - The server to create the bucket on. (TestInputServer)
            bucket_name - The name of the bucket to be created. (String)
            port - The port to create this bucket on. (String)
            password - The password for this bucket. (String)
            size - The size of the bucket to be created. (int)
            enable_replica_index - can be 0 or 1, 1 enables indexing of replica bucket data (int)
            replicas - The number of replicas for this bucket. (int)
            eviction_policy - The eviction policy for the bucket, can be valueOnly or fullEviction. (String)
            bucket_priority - The priority of the bucket:either none, low, or high. (String)
            bucket_type - The type of bucket. (String)
            flushEnabled - Enable or Disable the flush functionality of the bucket. (int)
            lww = determine the conflict resolution type of the bucket. (Boolean)

        Returns:
            bucket_params - A dictionary containing the parameters needed to create a bucket."""

        bucket_params = {}
        bucket_params['server'] = server
        bucket_params['replicas'] = replicas
        bucket_params['size'] = size
        bucket_params['port'] = port
        bucket_params['password'] = password
        bucket_params['bucket_type'] = bucket_type
        bucket_params['enable_replica_index'] = enable_replica_index
        bucket_params['eviction_policy'] = eviction_policy
        bucket_params['bucket_priority'] = bucket_priority
        bucket_params['flush_enabled'] = flush_enabled
        bucket_params['lww'] = lww
        bucket_params['maxTTL'] = maxttl
        return bucket_params

    def create_sasl_buckets(
            self, bucket_size, num_buckets=1, num_replicas=1,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_priority=BUCKET_PRIORITY.HIGH,
            bucket_type=None, maxttl=None):
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
            sasl_params = self._create_bucket_params(
                server=self.__master_node,
                password='password',
                size=bucket_size,
                replicas=num_replicas,
                eviction_policy=eviction_policy,
                bucket_priority=bucket_priority,
                bucket_type=bucket_type,
                maxttl=maxttl)

            bucket_tasks.append(self.__clusterop.async_create_sasl_bucket(name=name, password='password',
                                                                          bucket_params=sasl_params))
            self.__buckets.append(
                Bucket(
                    name=name, authType="sasl", saslPassword="password",
                    num_replicas=num_replicas, bucket_size=bucket_size,
                    eviction_policy=eviction_policy,
                    bucket_priority=bucket_priority,
                    maxttl=maxttl
                ))

        for task in bucket_tasks:
            task.result()

    def create_standard_buckets(
            self, bucket_size, name=None, num_buckets=1,
            port=None, num_replicas=1,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_priority=BUCKET_PRIORITY.HIGH,
            bucket_type=None, maxttl=None):
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

        if not bucket_type:
            bucket_type = 'membase'

        for i in range(num_buckets):
            if not (num_buckets == 1 and name):
                name = "standard_bucket_" + str(i + 1)
            standard_params = self._create_bucket_params(
                server=self.__master_node,
                size=bucket_size,
                replicas=num_replicas,
                eviction_policy=eviction_policy,
                bucket_priority=bucket_priority,
                bucket_type=bucket_type,
                maxttl=maxttl)

            bucket_tasks.append(
                self.__clusterop.async_create_standard_bucket(
                    name=name, port=STANDARD_BUCKET_PORT+i,
                    bucket_params=standard_params))

            self.__buckets.append(
                Bucket(
                    name=name,
                    authType=None,
                    saslPassword=None,
                    num_replicas=num_replicas,
                    bucket_size=bucket_size,
                    port=start_port + i,
                    eviction_policy=eviction_policy,
                    bucket_priority=bucket_priority,
                    maxttl=maxttl
                ))

        for task in bucket_tasks:
            task.result()

    def create_default_bucket(
            self, bucket_size, num_replicas=1,
            eviction_policy=EVICTION_POLICY.VALUE_ONLY,
            bucket_priority=BUCKET_PRIORITY.HIGH,
            bucket_type=None, maxttl=None):
        """Create default bucket.
        @param bucket_size: size of the bucket.
        @param num_replicas: number of replicas (1-3).
        @param eviction_policy: valueOnly etc.
        @param bucket_priority: high/low etc.
        """
        bucket_params=self._create_bucket_params(
            server=self.__master_node,
            size=bucket_size,
            replicas=num_replicas,
            eviction_policy=eviction_policy,
            bucket_priority=bucket_priority,
            bucket_type=bucket_type,
            maxttl=maxttl
        )
        self.__clusterop.create_default_bucket(bucket_params)
        self.__buckets.append(
            Bucket(
                name=BUCKET_NAME.DEFAULT,
                authType="sasl",
                saslPassword="",
                num_replicas=num_replicas,
                bucket_size=bucket_size,
                eviction_policy=eviction_policy,
                bucket_priority=bucket_priority,
                maxttl=maxttl
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
                                dictionary overriding params in
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

    def run_fts_query(self, index_name, query_dict, node=None, timeout=70):
        """ Runs a query defined in query_json against an index/alias and
        a specific node

        @return total_hits : total hits for the query,
        @return hit_list : list of docs that match the query

        """
        if not node:
            node = self.get_random_fts_node()
        self.__log.info("Running query %s on node: %s:%s"
                        % (json.dumps(query_dict, ensure_ascii=False),
                           node.ip, node.fts_port))
        total_hits, hit_list, time_taken, status = \
            RestConnection(node).run_fts_query(index_name, query_dict, timeout=timeout)
        return total_hits, hit_list, time_taken, status

    def run_n1ql_query(self, query="", node=None, timeout=70):
        """ Runs a query defined in query_json against an index/alias and
        a specific node

        """
        if not node:
            node = self.get_random_n1ql_node()
        res = RestConnection(node).query_tool(query)
        return res

    def run_fts_query_with_facets(self, index_name, query_dict, node=None):
        """ Runs a query defined in query_json against an index/alias and
        a specific node

        @return total_hits : total hits for the query,
        @return hit_list : list of docs that match the query

        """
        if not node:
            node = self.get_random_fts_node()
        self.__log.info("Running query %s on node: %s:%s"
                        % (json.dumps(query_dict, ensure_ascii=False),
                           node.ip, node.fts_port))
        total_hits, hit_list, time_taken, status, facets = \
            RestConnection(node).run_fts_query_with_facets(index_name, query_dict)
        return total_hits, hit_list, time_taken, status, facets

    def get_buckets(self):
        if not self.__buckets:
            self.__buckets = RestConnection(self.__master_node).get_buckets()
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
        for bucket_in in self.__buckets:
            if bucket_in.name == bucket_to_remove:
                self.__buckets.remove(bucket_in)

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

    def load_from_high_ops_loader(self, bucket):
        input = TestInputSingleton.input
        batch_size = input.param("batch_size", 1000)
        instances = input.param("instances", 8)
        threads = input.param("threads", 8)
        items = input.param("items", 6000000)
        self.__clusterop.load_buckets_with_high_ops(
            server=self.__master_node,
            bucket=bucket,
            items=items,
            batch=batch_size,
            threads=threads,
            start_document=0,
            instances=instances,
            ttl=0)

    def check_dataloss_with_high_ops_loader(self, bucket):
        self.__clusterop.check_dataloss_for_high_ops_loader(
            self.__master_node,
            bucket,
            TestInputSingleton.input.param("items", 6000000),
            batch=20000,
            threads=5,
            start_document=0,
            updated=False,
            ops=0,
            ttl=0,
            deleted=False,
            deleted_items=0)

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
            timeout_secs, compression=self.sdk_compression)
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
                    pause_secs, timeout_secs, compression=self.sdk_compression)
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
        if not self.__buckets:
            self.__buckets = RestConnection(self.__master_node).get_buckets()
        for bucket in self.__buckets:
            kv_gen = copy.deepcopy(self._kv_gen[ops])
            tasks.append(
                self.__clusterop.async_load_gen_docs(
                    self.__master_node, bucket.name, kv_gen,
                    bucket.kvs[kv_store], ops, exp, flag,
                    only_store_hash, batch_size, pause_secs, timeout_secs, compression=self.sdk_compression)
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
                only_store_hash, batch_size, pause_secs, timeout_secs, compression=self.sdk_compression)
        )
        return task

    def load_all_buckets_till_dgm(self, active_resident_ratio, es=None,
                                  items=1000, exp=0, kv_store=1, flag=0,
                                  only_store_hash=True, batch_size=1000,
                                  pause_secs=1, timeout_secs=30):
        """Load data synchronously on all buckets till dgm (Data greater than memory)
        for given active_resident_ratio
        @param active_resident_ratio: Dgm threshold.
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
        seed = "%s-" % self.__name
        end = 0
        current_active_resident = StatsCommon.get_stats(
            [self.__master_node],
            self.__buckets[0],
            '',
            'vb_active_perc_mem_resident')[self.__master_node]
        start = items
        while int(current_active_resident) > active_resident_ratio:
            batch_size = 1000
            if int(current_active_resident) - active_resident_ratio > 5:
                end = start + batch_size * 100
                batch_size = batch_size * 100
            else:
                end = start + batch_size * 10
                batch_size = batch_size * 10

            self.__log.info("Generating %s keys ..." % (end - start))
            kv_gen = JsonDocGenerator(seed,
                                      encoding="utf-8",
                                      start=start,
                                      end=end)
            self.__log.info("Loading %s keys ..." % (end - start))
            tasks = []
            for bucket in self.__buckets:
                tasks.append(self.__clusterop.async_load_gen_docs(
                    self.__master_node, bucket.name, copy.deepcopy(kv_gen), bucket.kvs[kv_store],
                    OPS.CREATE, exp, flag, only_store_hash, batch_size,
                    pause_secs, timeout_secs, compression=self.sdk_compression))

            if es:
                tasks.append(es.async_bulk_load_ES(index_name='default_es_index',
                                                       gen=kv_gen,
                                                       op_type='create'))

            for task in tasks:
                task.result(timeout=2000)

            start = end
            current_active_resident = StatsCommon.get_stats(
                [self.__master_node],
                bucket,
                '',
                'vb_active_perc_mem_resident')[self.__master_node]
            self.__log.info(
                "Current resident ratio: %s, desired: %s bucket %s" % (
                    current_active_resident,
                    active_resident_ratio,
                    bucket.name))
            self._kv_gen[OPS.CREATE].gen_docs.update(kv_gen.gen_docs)
            self._kv_gen[OPS.CREATE].end = kv_gen.end
        self.__log.info("Loaded a total of %s keys into bucket %s"
                        % (end, bucket.name))

        return self._kv_gen[OPS.CREATE]

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
        self.__log.info("Updating fields %s in bucket %s" % (fields_to_update,
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
                                           * (float)(perc) / 100)
        self._kv_gen[OPS.UPDATE].update(fields_to_update=fields_to_update)

        task = self.__clusterop.async_load_gen_docs(
            self.__master_node, bucket.name, self._kv_gen[OPS.UPDATE],
            bucket.kvs[kv_store], OPS.UPDATE, exp, flag, only_store_hash,
            batch_size, pause_secs, timeout_secs, compression=self.sdk_compression)
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
                                                   * (float)(perc) / 100)
                self._kv_gen[OPS.UPDATE].update(fields_to_update=fields_to_update)
                gen = self._kv_gen[OPS.UPDATE]
            elif op_type == OPS.DELETE:
                self._kv_gen[OPS.DELETE] = JsonDocGenerator(
                    self._kv_gen[OPS.CREATE].name,
                    op_type=OPS.DELETE,
                    encoding="utf-8",
                    start=int((self._kv_gen[OPS.CREATE].end)
                              * (float)(100 - perc) / 100),
                    end=self._kv_gen[OPS.CREATE].end)
                gen = copy.deepcopy(self._kv_gen[OPS.DELETE])
            else:
                raise FTSException("Unknown op_type passed: %s" % op_type)

            self.__log.info("At bucket '{0}' @ {1}: operation: {2}, key range {3} - {4}".
                            format(bucket.name, self.__name, op_type, gen.start, gen.end - 1))
            tasks.append(
                self.__clusterop.async_load_gen_docs(
                    self.__master_node,
                    bucket.name,
                    gen,
                    bucket.kvs[kv_store],
                    op_type,
                    expiration,
                    batch_size=1000,
                    compression=self.sdk_compression)
            )
        return tasks

    def async_run_fts_query_compare(self, fts_index, es, query_index, es_index_name=None, n1ql_executor=None):
        """
        Asynchronously run query against FTS and ES and compare result
        note: every task runs a single query
        """
        task = self.__clusterop.async_run_fts_query_compare(fts_index=fts_index,
                                                            es_instance=es,
                                                            query_index=query_index,
                                                            es_index_name=es_index_name,
                                                            n1ql_executor=n1ql_executor)
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
                "Number of free nodes: {0}, test tried to add {1} new nodes!".
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

    def __async_swap_rebalance(self, master=False, num_nodes=1, services=None):
        """Swap-rebalance nodes on Cluster
        @param master: True if swap-rebalance master node else False.
        """
        if master:
            to_remove_node = [self.__master_node]
        else:
            to_remove_node = self.__nodes[len(self.__nodes) - num_nodes:]

        raise_if(
            len(FloatingServers._serverlist) < num_nodes,
            FTSException(
                "Number of free nodes: {0}, test tried to add {1} new nodes!".
                    format(len(FloatingServers._serverlist), num_nodes))
        )
        to_add_node = []
        for _ in range(num_nodes):
            node = FloatingServers._serverlist.pop()
            if node not in self.__nodes:
                to_add_node.append(node)

        self.__log.info(
            "Starting swap-rebalance [remove_node:{0}] -> [add_node:{1}] at"
            " {2} cluster {3}"
                .format(to_remove_node, to_add_node, self.__name,
                        self.__master_node.ip))
        task = self.__clusterop.async_rebalance(
            self.__nodes,
            to_add_node,
            to_remove_node,
            services=services)

        for remove_node in to_remove_node:
            self.__nodes.remove(remove_node)

        self.__nodes.extend(to_add_node)

        if master:
            self.__master_node = self.__nodes[0]

        return task

    def async_swap_rebalance_master(self, services=None):
        """
           Returns without waiting for swap rebalance to complete
        """
        return self.__async_swap_rebalance(master=True, services=services)

    def async_swap_rebalance(self, num_nodes=1, services=None):
        return self.__async_swap_rebalance(num_nodes=num_nodes,
                                           services=services)

    def swap_rebalance_master(self, services=None):
        """Swap rebalance master node and wait
        """
        task = self.__async_swap_rebalance(master=True, services=services)
        task.result()

    def swap_rebalance(self, services=None, num_nodes=1):
        """Swap rebalance non-master node
        """
        task = self.__async_swap_rebalance(services=services,
                                           num_nodes=num_nodes)
        task.result()

    def async_failover_and_rebalance(self, master=False, num_nodes=1,
                                     graceful=False):
        """Asynchronously failover nodes from Cluster
        @param master: True if failover master node only.
        @param num_nodes: number of nodes to rebalance-out from cluster.
        @param graceful: True if graceful failover else False.
        """
        task = self.__async_failover(master=master,
                                     num_nodes=num_nodes,
                                     graceful=graceful)
        task.result()
        tasks = self.__clusterop.async_rebalance(self.__nodes, [], [],
                                                 services=None)
        return tasks

    def failover(self, master=False, num_nodes=1,
                                     graceful=False):
        """synchronously failover nodes from Cluster
        @param master: True if failover master node only.
        @param num_nodes: number of nodes to rebalance-out from cluster.
        @param graceful: True if graceful failover else False.
        """
        task = self.__async_failover(master=master,
                                     num_nodes=num_nodes,
                                     graceful=graceful)
        task.result()

    def __async_failover(self, master=False, num_nodes=1, graceful=False, node=None):
        """Failover nodes from Cluster
        @param master: True if failover master node only.
        @param num_nodes: number of nodes to rebalance-out from cluster.
        @param graceful: True if graceful failover else False.
        @param node: Specific node to be failed over
        """
        raise_if(
            len(self.__nodes) <= 1,
            FTSException(
                "More than 1 node required in cluster to perform failover")
        )
        if node:
            self.__fail_over_nodes = [node]
        elif master:
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

    def async_failover(self, master=False, num_nodes=1, graceful=False,node=None):
        return self.__async_failover(master=master, num_nodes=num_nodes, graceful=graceful, node=node)

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

    def async_failover_add_back_node(self, num_nodes=1, graceful=False,
                                     recovery_type=None, services=None):
        """add-back failed-over node to the cluster.
            @param recovery_type: delta/full
        """
        task = self.__async_failover(
            master=False,
            num_nodes=num_nodes,
            graceful=graceful)
        task.result()
        time.sleep(60)
        if graceful:
            # use rebalance stats to monitor failover
            RestConnection(self.__master_node).monitorRebalance()

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
        self.__fail_over_nodes = []
        tasks = self.__clusterop.async_rebalance(self.__nodes, [], [], services=services)
        return tasks

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
        self.num_custom_analyzers = self._input.param("num_custom_analyzers", 0)
        self.field_name = self._input.param("field_name", None)
        self.field_type = self._input.param("field_type", None)
        self.field_alias = self._input.param("field_alias", None)

        self.log.info(
            "==== FTSbasetests setup is started for test #{0} {1} ===="
                .format(self.__case_number, self._testMethodName))

        # workaround for MB-16794
        # self.sleep(30, "working around MB-16794")

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
        return len({server.ip for server in self._input.servers}) == 1

    def tearDown(self):
        """Clusters cleanup"""
        if len(self.__report_error_list) > 0:
            error_logger = self.check_error_count_in_fts_log()
            if error_logger:
                self.fail("Errors found in logs : {0}".format(error_logger))

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

        if self._input.param("get-fts-diags", False) and self.__is_test_failed():
            self.grab_fts_diag()

        # collect logs before tearing down clusters
        if self._input.param("get-cbcollect-info", False) and \
                self.__is_test_failed():
            for server in self._input.servers:
                self.log.info("Collecting logs @ {0}".format(server.ip))
                NodeHelper.collect_logs(server)

        # ---backup pindex_data if the test has failed
        # if self._input.param('backup_pindex_data', False) and \
        #        self.__is_test_failed():
        # To reproduce MB-20494, temporarily remove condition to
        # backup_pindex_data only if test has failed.
        if self._input.param('backup_pindex_data', False) :
            for server in self._input.servers:
                self.log.info("Backing up pindex data @ {0}".format(server.ip))
                self.backup_pindex_data(server)

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
        sdk_compression = self._input.param("sdk_compression", True)
        master = self._input.servers[0]
        first_node = copy.deepcopy(master)
        self._cb_cluster = CouchbaseCluster("C1",
                                            [first_node],
                                            self.log,
                                            use_hostanames,
                                            sdk_compression=sdk_compression)
        self.__cleanup_previous()
        if self.compare_es:
            self.setup_es()
        self._cb_cluster.init_cluster(self._cluster_services,
                                      self._input.servers[1:])

        self._enable_diag_eval_on_non_local_hosts()
        # Add built-in user
        testuser = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'password': 'password'}]
        RbacBase().create_user_source(testuser, 'builtin', master)
        
        # Assign user to role
        role_list = [{'id': 'cbadminbucket', 'name': 'cbadminbucket', 'roles': 'admin'}]
        RbacBase().add_user_role(role_list, RestConnection(master), 'builtin')
        
        self.__set_free_servers()
        if not no_buckets:
            self.__create_buckets()
        self._master = self._cb_cluster.get_master_node()

        # simply append to this list, any error from log we want to fail test on
        self.__report_error_list = []
        if self.__fail_on_errors:
            self.__report_error_list = ["panic:"]

        # for format {ip1: {"panic": 2}}
        self.__error_count_dict = {}
        if len(self.__report_error_list) > 0:
            self.__initialize_error_count_dict()

    def _enable_diag_eval_on_non_local_hosts(self):
        """
        Enable diag/eval to be run on non-local hosts.
        :return: Nothing
        """
        master = self._cb_cluster.get_master_node()
        remote = RemoteMachineShellConnection(master)
        output, error = remote.enable_diag_eval_on_non_local_hosts()
        if output is not None:
            if "ok" not in output:
                self.log.error("Error in enabling diag/eval on non-local hosts on {}".format(master.ip))
                raise Exception("Error in enabling diag/eval on non-local hosts on {}".format(master.ip))
            else:
                self.log.info(
                    "Enabled diag/eval for non-local hosts from {}".format(
                        master.ip))
        else:
            self.log.info("Running in compatibility mode, not enabled diag/eval for non-local hosts")
    
    def construct_serv_list(self, serv_str):
        """
            Constructs a list of node services
            to rebalance into cluster
            @param serv_str: like "D,D+F,I+Q,F" where the letters
                             stand for services defined in serv_dict
            @return services_list: like ['kv', 'kv,fts', 'index,n1ql','index']
        """
        serv_dict = {'D': 'kv', 'F': 'fts', 'I': 'index', 'Q': 'n1ql'}
        for letter, serv in serv_dict.items():
            serv_str = serv_str.replace(letter, serv)
        services_list = re.split('[-,:]', serv_str)
        for index, serv in enumerate(services_list):
            services_list[index] = serv.replace('+', ',')
        return services_list

    def __init_parameters(self):
        self.__case_number = self._input.param("case_number", 0)
        self.__num_sasl_buckets = self._input.param("sasl_buckets", 0)
        self.__num_stand_buckets = self._input.param("standard_buckets", 0)
        self.__eviction_policy = self._input.param("eviction_policy", 'valueOnly')
        self.__mixed_priority = self._input.param("mixed_priority", None)
        self.expected_no_of_results = self._input.param("expected_no_of_results", None)

        # Public init parameters - Used in other tests too.
        # Move above private to this section if needed in future, but
        # Ensure to change other tests too.

        self._cluster_services = \
            self.construct_serv_list(self._input.param("cluster", "D,D+F,F"))
        self._num_replicas = self._input.param("replicas", 1)
        self._create_default_bucket = self._input.param("default_bucket", True)
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
        self._disable_compaction = self._input.param("disable_compaction", "")
        self._item_count_timeout = self._input.param("item_count_timeout", 300)
        self._dgm_run = self._input.param("dgm_run", False)
        self._active_resident_ratio = \
            self._input.param("active_resident_ratio", 100)
        CHECK_AUDIT_EVENT.CHECK = self._input.param("verify_audit", 0)
        self._max_verify = self._input.param("max_verify", 100000)
        self._num_vbuckets = self._input.param("vbuckets", 1024)
        self.lang = self._input.param("lang", "EN")
        self.encoding = self._input.param("encoding", "utf-8")
        self.analyzer = self._input.param("analyzer", None)
        self.index_replicas = self._input.param("index_replicas", None)
        self.index_kv_store = self._input.param("kvstore", None)
        self.partitions_per_pindex = \
            self._input.param("max_partitions_pindex", 171)
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
            self.es.restart_es()
        else:
            self.es = None
        self.run_via_n1ql = self._input.param("run_via_n1ql", False)
        if self.run_via_n1ql:
            self.n1ql = N1QLHelper(version="sherlock", shell=None,
                                    item_flag=None, n1ql_port=8903,
                                      full_docs_list=[], log=self.log)
        else:
            self.n1ql = None
        self.create_gen = None
        self.update_gen = None
        self.delete_gen = None
        self.sort_fields = self._input.param("sort_fields", None)
        self.sort_fields_list = None
        if self.sort_fields:
            self.sort_fields_list = self.sort_fields.split(',')
        self.advanced_sort = self._input.param("advanced_sort", False)
        self.sort_by = self._input.param("sort_by", "score")
        self.sort_missing = self._input.param("sort_missing", "last")
        self.sort_desc = self._input.param("sort_desc", False)
        self.sort_mode = self._input.param("sort_mode", "min")
        self.__fail_on_errors = self._input.param("fail-on-errors", True)
        self.cli_command_location = LINUX_COUCHBASE_BIN_PATH
        self.expected_docs = str(self._input.param("expected", None))
        self.expected_docs_list = []
        if (self.expected_docs) and (',' in self.expected_docs):
            self.expected_docs_list = self.expected_docs.split(',')
        else:
            self.expected_docs_list.append(self.expected_docs)
        self.expected_results = self._input.param("expected_results", None)
        self.highlight_style = self._input.param("highlight_style", None)
        self.highlight_fields = self._input.param("highlight_fields", None)
        self.highlight_fields_list = []
        if (self.highlight_fields):
            if (',' in self.highlight_fields):
                self.highlight_fields_list = self.highlight_fields.split(',')
            else:
                self.highlight_fields_list.append(self.highlight_fields)
        self.consistency_level = self._input.param("consistency_level", '')
        if self.consistency_level.lower() == 'none':
            self.consistency_level = None
        self.consistency_vectors = self._input.param("consistency_vectors", {})
        if self.consistency_vectors != {}:
            self.consistency_vectors = eval(self.consistency_vectors)
            if self.consistency_vectors is not None and self.consistency_vectors != '':
                if not isinstance(self.consistency_vectors, dict):
                    self.consistency_vectors = json.loads(self.consistency_vectors)

    def __initialize_error_count_dict(self):
        """
            initializes self.__error_count_dict with ip, error and err count
            like {ip1: {"panic": 2}}
        """
        for node in self._input.servers:
            self.__error_count_dict[node.ip] = {}
        self.check_error_count_in_fts_log(initial=True)
        self.log.info(self.__error_count_dict)

    def __cleanup_previous(self):
        self._cb_cluster.cleanup_cluster(self, cluster_shutdown=False)

    def __set_free_servers(self):
        total_servers = self._input.servers
        cluster_nodes = self._cb_cluster.get_nodes()
        for server in total_servers:
            for cluster_node in cluster_nodes:
                if server.ip == cluster_node.ip and \
                                server.port == cluster_node.port:
                    break
                else:
                    continue
            else:
                FloatingServers._serverlist.append(server)

    def __calculate_bucket_size(self, cluster_quota, num_buckets):

        if 'quota_percent' in self._input.test_params:
            quota_percent = int(self._input.test_params['quota_percent'])
        else:
            quota_percent = None

        dgm_run = self._input.param("dgm_run", 0)
        if dgm_run:
            # buckets cannot be created if size<100MB
            bucket_size = 256
        elif quota_percent is not None:
             bucket_size = int( float(cluster_quota - 500) * float(quota_percent/100.0 ) /float(num_buckets) )
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

        bucket_type = TestInputSingleton.input.param("bucket_type", "membase")
        maxttl = TestInputSingleton.input.param("maxttl", None)

        if self._create_default_bucket:
            self._cb_cluster.create_default_bucket(
                bucket_size,
                self._num_replicas,
                eviction_policy=self.__eviction_policy,
                bucket_priority=bucket_priority,
                bucket_type=bucket_type,
                maxttl=maxttl)

        self._cb_cluster.create_sasl_buckets(
            bucket_size, num_buckets=self.__num_sasl_buckets,
            num_replicas=self._num_replicas,
            eviction_policy=self.__eviction_policy,
            bucket_priority=bucket_priority,
            bucket_type=bucket_type,
            maxttl=maxttl)

        self._cb_cluster.create_standard_buckets(
            bucket_size, num_buckets=self.__num_stand_buckets,
            num_replicas=self._num_replicas,
            eviction_policy=self.__eviction_policy,
            bucket_priority=bucket_priority,
            bucket_type=bucket_type,
            maxttl=maxttl)

    def create_buckets_on_cluster(self):
        # if mixed priority is set by user, set high priority for sasl and
        # standard buckets
        self.__create_buckets()

    def load_sample_buckets(self, server, bucketName):
        from lib.remote.remote_util import RemoteMachineShellConnection
        shell = RemoteMachineShellConnection(server)
        shell.execute_command("""curl -v -u Administrator:password \
                             -X POST http://{0}:8091/sampleBuckets/install \
                          -d '["{1}"]'""".format(server.ip, bucketName))
        shell.disconnect()
        self.sleep(20)

    def load_employee_dataset(self, num_items=None):
        """
            Loads the default JSON dataset
            see JsonDocGenerator in documentgenerator.py
        """
        self.log.info("Beginning data load ...")
        if not num_items:
            num_items = self._num_items
        if not self._dgm_run:
            self._cb_cluster.load_all_buckets(num_items, self._value_size)

        else:
            self._cb_cluster.load_all_buckets_till_dgm(
                active_resident_ratio=self._active_resident_ratio,
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

    def load_earthquakes(self, num_keys=None):
        """
        Loads geo-spatial jsons from earthquakes.json .
        """
        if not num_keys:
            num_keys = self._num_items

        gen = GeoSpatialDataLoader("earthquake",
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
        load_tasks = []
        # UPDATES
        if self._update:
            self.log.info("Updating keys @ {0} with expiry={1}".
                          format(self._cb_cluster.get_name(), self._expires))
            self.populate_update_gen(fields_to_update)
            if self.compare_es:
                gen = copy.deepcopy(self.update_gen)
                if not self._expires:
                    if isinstance(gen, list):
                        for generator in gen:
                            load_tasks.append(self.es.async_bulk_load_ES(
                                index_name='es_index',
                                gen=generator,
                                op_type=OPS.UPDATE))
                    else:
                        load_tasks.append(self.es.async_bulk_load_ES(
                            index_name='es_index',
                            gen=gen,
                            op_type=OPS.UPDATE))
                else:
                    # an expire on CB translates to delete on ES
                    if isinstance(gen, list):
                        for generator in gen:
                            load_tasks.append(self.es.async_bulk_load_ES(
                                index_name='es_index',
                                gen=generator,
                                op_type=OPS.DELETE))
                    else:
                        load_tasks.append(self.es.async_bulk_load_ES(
                            index_name='es_index',
                            gen=gen,
                            op_type=OPS.DELETE))

            load_tasks += self._cb_cluster.async_load_all_buckets_from_generator(
                kv_gen=self.update_gen,
                ops=OPS.UPDATE,
                exp=self._expires)

        [task.result() for task in load_tasks]
        if load_tasks:
            self.log.info("Batched updates loaded to cluster(s)")

        load_tasks = []
        # DELETES
        if self._delete:
            self.log.info("Deleting keys @ {0}".format(self._cb_cluster.get_name()))
            self.populate_delete_gen()
            if self.compare_es:
                del_gen = copy.deepcopy(self.delete_gen)
                if isinstance(del_gen, list):
                    for generator in del_gen:
                        load_tasks.append(self.es.async_bulk_load_ES(
                            index_name='es_index',
                            gen=generator,
                            op_type=OPS.DELETE))
                else:
                    load_tasks.append(self.es.async_bulk_load_ES(
                        index_name='es_index',
                        gen=del_gen,
                        op_type=OPS.DELETE))
            load_tasks += self._cb_cluster.async_load_all_buckets_from_generator(
                self.delete_gen, OPS.DELETE)

        [task.result() for task in load_tasks]
        if load_tasks:
            self.log.info("Batched deletes sent to cluster(s)")

        if self._wait_for_expiration and self._expires:
            self.sleep(
                self._expires,
                "Waiting for expiration of updated items")
            self._cb_cluster.run_expiry_pager()

    def print_crash_stacktrace(self, node, error):
        """ Prints panic stacktrace from goxdcr.log*
        """
        shell = RemoteMachineShellConnection(node)
        result, err = shell.execute_command("zgrep -A 40 -B 4 '{0}' {1}/fts.log*".
                                            format(error, NodeHelper.get_log_dir(node)))
        for line in result:
            self.log.info(line)
        shell.disconnect()

    def check_error_count_in_fts_log(self, initial=False):
        """
        checks if new errors from self.__report_error_list
        were found on any of the goxdcr.logs
        """
        error_found_logger = []
        fts_log = NodeHelper.get_log_dir(self._input.servers[0]) + '/fts.log*'
        for node in self._input.servers:
            shell = RemoteMachineShellConnection(node)
            for error in self.__report_error_list:
                count, err = shell.execute_command(
                    "zgrep \"{0}\" {1} | wc -l".format(error, fts_log))
                if isinstance(count, list):
                    count = int(count[0])
                else:
                    count = int(count)
                NodeHelper._log.info(count)
                if initial:
                    self.__error_count_dict[node.ip][error] = count
                else:
                    self.log.info("Initial '{0}' count on {1} :{2}, now :{3}".
                                  format(error,
                                         node.ip,
                                         self.__error_count_dict[node.ip][error],
                                         count))
                    if node.ip in list(self.__error_count_dict.keys()):
                        if (count > self.__error_count_dict[node.ip][error]):
                            error_found_logger.append("{0} found on {1}".format(error,
                                                                                node.ip))
                            self.print_crash_stacktrace(node, error)
            shell.disconnect()
        if not initial:
            if error_found_logger:
                self.log.error(error_found_logger)
            return error_found_logger

    def sleep(self, timeout=1, message=""):
        self.log.info("sleep for {0} secs. {1} ...".format(timeout, message))
        time.sleep(timeout)

    def wait_for_indexing_complete(self, item_count=None):
        """
        Wait for index_count for any index to stabilize or reach the
        index count specified by item_count
        """
        retry = self._input.param("index_retry", 20)
        for index in self._cb_cluster.get_indexes():
            if index.index_type == "fulltext-alias":
                continue
            retry_count = retry
            prev_count = 0
            es_index_count = 0
            while retry_count > 0:
                try:
                    index_doc_count = index.get_indexed_doc_count()
                    bucket_doc_count = index.get_src_bucket_doc_count()
                    if not self.compare_es:
                        self.log.info("Docs in bucket = %s, docs in FTS index '%s': %s"
                                      % (bucket_doc_count,
                                         index.name,
                                         index_doc_count))
                    else:
                        self.es.update_index('es_index')
                        es_index_count = self.es.get_index_count('es_index')
                        self.log.info("Docs in bucket = %s, docs in FTS index '%s':"
                                      " %s, docs in ES index: %s "
                                      % (bucket_doc_count,
                                         index.name,
                                         index_doc_count,
                                         es_index_count))
                    if bucket_doc_count == 0:
                        if item_count and item_count != 0:
                            self.sleep(5,
                                "looks like docs haven't been loaded yet...")
                            retry_count -= 1
                            continue

                    if item_count and index_doc_count > item_count:
                        break

                    if bucket_doc_count == index_doc_count:
                        if self.compare_es:
                            if bucket_doc_count == es_index_count:
                                break
                        else:
                            break

                    if prev_count < index_doc_count or prev_count > index_doc_count:
                        prev_count = index_doc_count
                        retry_count = retry
                    else:
                        retry_count -= 1
                except Exception as e:
                    self.log.info(e)
                    retry_count -= 1
                time.sleep(6)
            # now wait for num_mutations_to_index to become zero to handle the pure
            # updates scenario - where doc count remains unchanged
            retry_mut_count = 20
            if item_count == None:
                while True and retry_count:
                    num_mutations_to_index = index.get_num_mutations_to_index()
                    if num_mutations_to_index > 0:
                        self.sleep(5, "num_mutations_to_index: {0} > 0".format(num_mutations_to_index))
                        retry_count -= 1
                    else:
                        break

    def construct_plan_params(self):
        plan_params = {}
        plan_params['numReplicas'] = 0
        if self.index_replicas:
            plan_params['numReplicas'] = self.index_replicas
        if self.partitions_per_pindex:
            plan_params['maxPartitionsPerPIndex'] = self.partitions_per_pindex
        return plan_params

    def populate_node_partition_map(self, index):
        """
        populates the node-pindex-partition map
        """
        nodes_partitions = {}
        start_time = time.time()
        _, defn = index.get_index_defn()
        while 'planPIndexes' not in defn or not defn['planPIndexes']:
            if time.time() - start_time > 60:
                self.fail("planPIndexes unavailable for index {0} even after 60s"
                          .format(index.name))
            self.sleep(5, "No pindexes found, waiting for index to get created")
            _, defn = index.get_index_defn()

        for pindex in defn['planPIndexes']:
            for node, attr in pindex['nodes'].items():
                if attr['priority'] == 0:
                    break
            if node not in list(nodes_partitions.keys()):
                nodes_partitions[node] = {'pindex_count': 0, 'pindexes': {}}
            nodes_partitions[node]['pindex_count'] += 1
            nodes_partitions[node]['pindexes'][pindex['uuid']] = []
            for partition in pindex['sourcePartitions'].split(','):
                nodes_partitions[node]['pindexes'][pindex['uuid']].append(partition)
        return nodes_partitions

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
        self.log.info("Validating index distribution for %s ..." % index.name)
        nodes_partitions = self.populate_node_partition_map(index)

        # check 1 - test number of pindexes
        partitions_per_pindex = index.get_max_partitions_pindex()
        exp_num_pindexes = self._num_vbuckets // partitions_per_pindex
        if self._num_vbuckets % partitions_per_pindex:
            import math
            exp_num_pindexes = math.ceil(
                self._num_vbuckets // partitions_per_pindex + 0.5)
        total_pindexes = 0
        for node in list(nodes_partitions.keys()):
            total_pindexes += nodes_partitions[node]['pindex_count']
        if total_pindexes != exp_num_pindexes:
            self.fail("Number of pindexes for %s is %s while"
                      " expected value is %s" % (index.name,
                                                 total_pindexes,
                                                 exp_num_pindexes))
        self.log.info("Validated: Number of PIndexes = %s" % total_pindexes)
        index.num_pindexes = total_pindexes

        # check 2 - each pindex servicing "partitions_per_pindex" vbs
        num_fts_nodes = len(self._cb_cluster.get_fts_nodes())
        for node in list(nodes_partitions.keys()):
            for uuid, partitions in nodes_partitions[node]['pindexes'].items():
                if len(partitions) > partitions_per_pindex:
                    self.fail("sourcePartitions for pindex %s more than "
                              "max_partitions_per_pindex %s" %
                              (uuid, partitions_per_pindex))
        self.log.info("Validated: Every pIndex serves %s partitions or lesser"
                      % partitions_per_pindex)

        # check 3 - distributed - pindex present on all fts nodes?
        count = 0
        nodes_with_pindexes = len(list(nodes_partitions.keys()))
        if nodes_with_pindexes > 1:
            while nodes_with_pindexes != num_fts_nodes:
                count += 10
                if count == 60:
                    self.fail("Even after 60s of waiting, index is not properly"
                              " distributed,pindexes spread across %s while "
                              "fts nodes are %s" % (list(nodes_partitions.keys()),
                                                    self._cb_cluster.get_fts_nodes()))
                self.sleep(10, "pIndexes not distributed across %s nodes yet"
                           % num_fts_nodes)
                nodes_partitions = self.populate_node_partition_map(index)
                nodes_with_pindexes = len(list(nodes_partitions.keys()))
            else:
                self.log.info("Validated: pIndexes are distributed across %s "
                              % list(nodes_partitions.keys()))

        # check 4 - balance check(almost equal no of pindexes on all fts nodes)
        exp_partitions_per_node = self._num_vbuckets // num_fts_nodes
        self.log.info("Expecting num of partitions in each node in range %s-%s"
                      % (exp_partitions_per_node - partitions_per_pindex,
                         min(1024, exp_partitions_per_node + partitions_per_pindex)))

        for node in list(nodes_partitions.keys()):
            num_node_partitions = 0
            for uuid, partitions in nodes_partitions[node]['pindexes'].items():
                num_node_partitions += len(partitions)
            if abs(num_node_partitions - exp_partitions_per_node) > \
                    partitions_per_pindex:
                self.fail("The source partitions are not evenly distributed "
                          "among nodes, seeing %s on %s"
                          % (num_node_partitions, node))
            self.log.info("Validated: Node %s houses %s pindexes which serve"
                          " %s partitions" %
                          (node,
                           nodes_partitions[node]['pindex_count'],
                           num_node_partitions))
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
        from .random_query_generator.rand_query_gen import FTSESQueryGenerator
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

    def generate_random_geo_queries(self, index, num_queries=1, sort=False):
        """
        Generates a bunch of geo location and bounding box queries for
        fts and es.
        :param index: fts index object
        :param num_queries: no of queries to be generated
        :return: fts or fts and es queries
        """
        import random
        from .random_query_generator.rand_query_gen import FTSESQueryGenerator
        gen_queries = 0

        while gen_queries < num_queries:
            if bool(random.getrandbits(1)):
                fts_query, es_query = FTSESQueryGenerator.\
                    construct_geo_location_query()
            else:
                fts_query, es_query = FTSESQueryGenerator. \
                    construct_geo_bounding_box_query()

            index.fts_queries.append(
                json.loads(json.dumps(fts_query, ensure_ascii=False)))

            if self.compare_es:
                self.es.es_queries.append(
                        json.loads(json.dumps(es_query, ensure_ascii=False)))
            gen_queries += 1

        if self.es:
            return index.fts_queries, self.es.es_queries
        else:
            return index.fts_queries


    def create_index(self, bucket, index_name, index_params=None,
                     plan_params=None):
        """
        Creates a default index given bucket, index_name and plan_params
        """
        bucket_password = ""
        if bucket.authType == "sasl":
            bucket_password = bucket.saslPassword
        if not plan_params:
            plan_params = self.construct_plan_params()
        index = self._cb_cluster.create_fts_index(
            name=index_name,
            source_name=bucket.name,
            index_params=index_params,
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
                    "%s_index_%s" % (bucket.name, count + 1),
                    plan_params=plan_params)

    def create_alias(self, target_indexes, name=None, alias_def=None):
        """
        Creates an alias spanning one or many target indexes
        """
        if not name:
            name = 'alias_%s' % int(time.time())

        if not alias_def:
            alias_def = {"targets": {}}
            for index in target_indexes:
                alias_def['targets'][index.name] = {}

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

    def is_index_complete(self, name):
        """
         Handle validation and error logging for docs indexed
         returns a map containing index_names and docs indexed
        """
        for index in self._cb_cluster.get_indexes():
            if index.name == name:
                docs_indexed = index.get_indexed_doc_count()
                bucket_count = self._cb_cluster.get_doc_count_in_bucket(
                    index.source_bucket)

                self.log.info("Docs in index {0}={1}, bucket docs={2}".
                          format(index.name, docs_indexed, bucket_count))
                if docs_indexed != bucket_count:
                    return False
                else:
                    return True

    def setup_es(self):
        """
        Setup Elastic search - create empty index node defined under
        'elastic' section in .ini
        """
        self.create_index_es()

    def teardown_es(self):
        self.es.delete_indices()

    def create_es_index_mapping(self, es_mapping, fts_mapping=None):
        if not (self.num_custom_analyzers > 0):
            self.es.create_index_mapping(index_name="es_index",
                                         es_mapping=es_mapping, fts_mapping=None)
        else:
            self.es.create_index_mapping(index_name="es_index",
                                         es_mapping=es_mapping, fts_mapping=fts_mapping)

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

    def create_geo_index_and_load(self):
        """
        Indexes geo spatial data
        Normally when we have a nested object, we first "insert child mapping"
        and then refer to the fields inside it. But, for geopoint, the
        structure "geo" is the data being indexed. Refer: CBQE-4030
        :return: the index object
        """
        if self.compare_es:
            self.log.info("Creating a geo-index on Elasticsearch...")
            self.es.delete_indices()
            es_mapping = {
                 "earthquake": {
                     "properties": {
                         "geo": {
                             "type": "geo_point"
                             }
                         }
                     }
                 }
            self.create_es_index_mapping(es_mapping=es_mapping)

        self.log.info("Creating geo-index ...")
        from .fts_base import FTSIndex
        geo_index = FTSIndex(
            cluster=self._cb_cluster,
            name="geo-index",
            source_name="default",
        )
        geo_index.index_definition["params"] = {
            "mapping": {
                "types": {
                    "earthquake": {
                        "enabled": True,
                        "properties": {
                            "geo": {
                                "dynamic": False,
                                "enabled": True,
                                "fields": [{
                                    "docvalues": True,
                                    "include_in_all": True,
                                    "name": "geo",
                                    "type": "geopoint",
                                    "store": False,
                                    "index": True
                                }
                                ]
                            }
                        }
                    }
                }
            }
        }
        geo_index.create()
        self.is_index_partitioned_balanced(geo_index)

        self.dataset = "earthquakes"
        self.log.info("Loading earthquakes.json ...")
        self.async_load_data()
        self.sleep(10, "Waiting to load earthquakes.json ...")
        self.wait_for_indexing_complete()
        return geo_index

    def create_index_es(self, index_name="es_index"):
        self.es.create_empty_index_with_bleve_equivalent_std_analyzer(index_name)
        self.log.info("Created empty index %s on Elastic Search node with "
                      "custom standard analyzer(default)"
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
                                    end=start + num_items)
        elif dataset == "wiki":
            return WikiJSONGenerator(name="wiki",
                                     lang=lang,
                                     encoding=encoding,
                                     start=start,
                                     end=start + num_items)
        elif dataset == "earthquakes":
            return GeoSpatialDataLoader(name="earthquake",
                                     start=start,
                                     end=start + num_items)

    def populate_create_gen(self):
        if self.dataset == "all":
            # only emp and wiki
            self.create_gen = []
            self.create_gen.append(self.get_generator(
                "emp", num_items=self._num_items // 2))
            self.create_gen.append(self.get_generator(
                "wiki", num_items=self._num_items // 2))
        else:
            self.create_gen = self.get_generator(
                self.dataset, num_items=self._num_items)


    def populate_update_gen(self, fields_to_update=None):
        if self.dataset == "emp":
            self.update_gen = copy.deepcopy(self.create_gen)
            self.update_gen.start = 0
            self.update_gen.end = int(self.create_gen.end *
                                      (float)(self._perc_upd) / 100)
            self.update_gen.update(fields_to_update=fields_to_update)
        elif self.dataset == "wiki":
            self.update_gen = copy.deepcopy(self.create_gen)
            self.update_gen.start = 0
            self.update_gen.end = int(self.create_gen.end *
                                      (float)(self._perc_upd) / 100)
        elif self.dataset == "all":
            self.update_gen = []
            self.update_gen = copy.deepcopy(self.create_gen)
            for itr, _ in enumerate(self.update_gen):
                self.update_gen[itr].start = 0
                self.update_gen[itr].end = int(self.create_gen[itr].end *
                                               (float)(self._perc_upd) / 100)
                if self.update_gen[itr].name == "emp":
                    self.update_gen[itr].update(fields_to_update=fields_to_update)

    def populate_delete_gen(self):
        if self.dataset == "emp":
            self.delete_gen = JsonDocGenerator(
                self.create_gen.name,
                op_type=OPS.DELETE,
                encoding="utf-8",
                start=int((self.create_gen.end)
                          * (float)(100 - self._perc_del) / 100),
                end=self.create_gen.end)
        elif self.dataset == "wiki":
            self.delete_gen = WikiJSONGenerator(name="wiki",
                                                encoding="utf-8",
                                                start=int((self.create_gen.end)
                                                          * (float)(100 - self._perc_del) / 100),
                                                end=self.create_gen.end,
                                                op_type=OPS.DELETE)

        elif self.dataset == "all":
            self.delete_gen = []
            self.delete_gen.append(JsonDocGenerator(
                "emp",
                op_type=OPS.DELETE,
                encoding="utf-8",
                start=int((self.create_gen[0].end)
                          * (float)(100 - self._perc_del) / 100),
                end=self.create_gen[0].end))
            self.delete_gen.append(WikiJSONGenerator(name="wiki",
                                                     encoding="utf-8",
                                                     start=int((self.create_gen[1].end)
                                                               * (float)(100 - self._perc_del) / 100),
                                                     end=self.create_gen[1].end,
                                                     op_type=OPS.DELETE))

    def load_data(self):
        """
         Blocking call to load data to Couchbase and ES
        """
        if self._dgm_run:
            self.create_gen = self._cb_cluster.load_all_buckets_till_dgm(
                self._active_resident_ratio,
                self.compare_es)
            return
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

    def run_query_and_compare(self, index=None, es_index_name=None, n1ql_executor=None):
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
                query_index=count,
                n1ql_executor=n1ql_executor))

        num_queries = len(tasks)

        for task in tasks:
            task.result()
            if not task.passed:
                fail_count += 1
                failed_queries.append(task.query_index + 1)

        if fail_count:
            self.fail("%s out of %s queries failed! - %s" % (fail_count,
                                                             num_queries,
                                                             failed_queries))
        else:
            self.log.info("SUCCESS: %s out of %s queries passed"
                          % (num_queries - fail_count, num_queries))

    def grab_fts_diag(self):
        """
         Grab fts diag until it is handled by cbcollect info
        """
        from http.client import BadStatusLine
        import os
        import urllib.request, urllib.error, urllib.parse
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
                req = urllib.request.Request(diag_url)
                authorization = base64.encodestring('%s:%s' % (
                    self._input.membase_settings.rest_username,
                    self._input.membase_settings.rest_password))
                req.headers = {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Authorization': 'Basic %s' % authorization,
                    'Accept': '*/*'}
                filename = "{0}_fts_diag.json".format(serverInfo.ip)
                page = urllib.request.urlopen(req)
                with open(path + '/' + filename, 'wb') as output:
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
                os.remove(path + '/' + filename)
                print("downloaded and zipped diags @ : {0}/{1}".format(path,
                                                                       filename))
            except urllib.error.URLError as error:
                print("unable to obtain fts diags from {0}".format(diag_url))
            except BadStatusLine:
                print("unable to obtain fts diags from {0}".format(diag_url))
            except Exception as e:
                print("unable to obtain fts diags from {0} :{1}".format(diag_url, e))

    def backup_pindex_data(self, server):
        remote = RemoteMachineShellConnection(server)
        stamp = time.strftime("%d_%m_%Y_%H_%M")
        data_dir = NodeHelper.get_data_dir(server)

        try:
            info = remote.extract_remote_info()
            if info.type.lower() != 'windows':
                self.log.info("Backing up pindex data files from {0}".format(server.ip))
                command = "mkdir -p /tmp/backup_pindex_data/{0};" \
                          "zip -r /tmp/backup_pindex_data/{0}/fts_pindex_data.zip " \
                          "{1}/data/@fts/*".format(stamp, data_dir)

                remote.execute_command(command)
                output, error = remote.execute_command("ls -la /tmp/backup_pindex_data/{0}".format(stamp))
                for o in output:
                    print(o)
                self.log.info("***pindex files for {0} are copied to /tmp/backup_pindex_data/{1} on {0}".format(server.ip, stamp))
                remote.disconnect()
                return True
        except Exception as ex:
            print(ex)
            return False

    def build_sort_params(self):
        """
        This method builds the value for the sort param that is passed to the
        query request. It handles simple or advanced sorting based on the
        inputs passed in the conf file
        :return: Value for the sort param
        """
        # TBD :
        # Cases where there are multiple sort fields - one advanced, one simple
        # Cases where there are multiple sort fields - one advanced using by 'field', and another using by 'id' or 'score'
        sort_params = []
        if self.advanced_sort or self.sort_fields_list:
            if self.advanced_sort:
                for sort_field in self.sort_fields_list:
                    params = {}
                    params["by"] = self.sort_by
                    if self.sort_by == "field":
                        params["field"] = sort_field
                    params["mode"] = self.sort_mode
                    params["desc"] = self.sort_desc
                    params["missing"] = self.sort_missing
                    sort_params.append(params)
            else:
                sort_params = self.sort_fields_list
        else:
            return None
        return sort_params

    def create_test_dataset(self, server, docs):
        """
        Creates documents using MemcachedClient in the default bucket
        from a given list of json data
        :param server: Server on which docs are to be loaded
        :param docs: List of json data
        :return: None
        """
        from memcached.helper.data_helper import KVStoreAwareSmartClient
        memc_client  = KVStoreAwareSmartClient(RestConnection(server),
                                               'default')
        count = 1
        for i, doc in enumerate(docs):
            while True:
                try:
                    memc_client.set(key=str(i+1),
                                    value=json.dumps(doc))
                    break
                except Exception as e:
                    self.log.error(e)
                    self.sleep(5)
                    count += 1
                    if count > 5:
                        raise e

    def wait_till_items_in_bucket_equal(self, items=None):
        """
        Waits till items in bucket is equal to the docs loaded
        :param items: the item count that the test should wait to reach
                      after loading
        :return: Nothing
        """
        if not self._dgm_run:
            counter = 0
            if not items:
                items = self._num_items//2
            while True:
                try:
                    doc_count = self._cb_cluster.get_doc_count_in_bucket(
                        self._cb_cluster.get_buckets()[0])
                    break
                except KeyError:
                    self.log.info("bucket stats not ready yet...")
                    self.sleep(2)
            for bucket in self._cb_cluster.get_buckets():
                while items > self._cb_cluster.get_doc_count_in_bucket(
                        bucket):
                    self.log.info("Docs in bucket {0} = {1}".
                        format(
                            bucket.name,
                            self._cb_cluster.get_doc_count_in_bucket(
                                bucket)))
                    self.sleep(1, "sleeping 1s to allow for item loading")
                    counter += 1
                    if counter > 20:
                        self.log.info("Exiting load sleep loop after 21s")
                        return
