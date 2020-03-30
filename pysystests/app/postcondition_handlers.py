

import re
import sys
import types

sys.path=["../lib"] + sys.path
from mc_bin_client import MemcachedClient, MemcachedError
import app
from app.rest_client_tasks import create_rest
import testcfg as cfg
from app.celery import celery
from cache import ObjCacher, CacheHelper

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


EQUAL = '='
NOT_EQUAL = '!='
LESS_THAN = '<'
LESS_THAN_EQ = '<='
GREATER_THAN = '>'
GREATER_THAN_EQ = '>='


""" default_condition_params method helps interpret postcondition string.
    The following syntax only works for "python cbsystest.py run workload"
    accepted syntax:
        "<ip:port> , <condition>, <condition_method>"

    Where <condition> is of the form "<stat> <equality> <value>"

    example:
        "10.2.32.55, ep_warmup_thread = complete, epengine_stat_checker"

    means to use ep-engine stat collector method against host 10.2.32.55 to
    watch for warmup.

    Note by default <condition_method> is determined at prerun time and is
    really only needed for custom postcondition methods

    Also master ip:port in testcfg will be used if not specified
"""
def parse_condition(postconditions):
    condition_params = postconditions.split(',')
    condition_map = {}

    for condition_str in condition_params:

        # attempt to match against ip address
        if match_ip(condition_str, condition_map):
            continue

        # attempt to match against equality
        if match_equality(condition_str, condition_map):
            continue

        # attempt to match against active tasks type, like indexer, bucket compaction
        if match_active_task_type(condition_str, condition_map):
            continue

        # method match
        if match_method(condition_str, condition_map):
            pass

    if len(condition_map) == 0:
        logger.error("Invalid condition syntax: %s" % condition_map)
        raise AttributeError(condition_map)

    return condition_map


""" This parser works for systest_manager"""

def parse_condition_dict(postconditions):
    condition_map = {
                     "cluster_check": False
                    }

    if "conditions" in postconditions:
        match_equality(postconditions['conditions'], condition_map)

    if "method" in postconditions:
        condition_map['method'] = postconditions['method']

    if "port" in postconditions:
        condition_map['port'] = postconditions['port']

    if "ip" in postconditions:
        condition_map['ip'] = postconditions['ip']

    if "type" in postconditions:
        condition_map['active_task_type'] = postconditions['type']

    if "target" in postconditions:
        condition_map['target_value'] = postconditions['target']

    if "cluster_check" in postconditions:
        if postconditions['cluster_check'] == 'True':
            condition_map['cluster_check'] = True

    return condition_map


def match_ip(condition_str, condition_map):
    m = re.match(r"\s*?(\d+.\d+.\d+.\d+):?(\d+)?\s*?", condition_str)
    if not isinstance(m, type(None)):
        condition_map["ip"], condition_map["port"] = m.groups()

def match_active_task_type(condition_str, condition_map):
    m = re.match(r"\s*?(?P<active_task_type>\w+):(?P<target_value>\w+)\s*?", condition_str)
    if not isinstance(m, type(None)):
        condition_map["active_task_type"] = m.group('type').strip()
        condition_map["target_value"] = m.group('target_value').strip()

def match_equality(condition_str, condition_map):
    m = re.match(r"\s*?(?P<stat>[\w]+)(?P<cmp>[\s=<>]+)(?P<value>\w+)\s*?",
                 condition_str)
    if not isinstance(m, type(None)):
        condition_map["stat"] = m.group('stat').strip()
        condition_map["cmp_type"] = m.group('cmp').strip()
        condition_map["value"] = m.group('value').strip()

def match_method(condition_str, condition_map):
    m = re.match(r"\s*?(?P<method>\w+)\s*?$", condition_str)
    if not isinstance(m, type(None)):
        condition_map["method"] = m.group('method').strip()


def default_condition_params(postconditions):
    if isinstance(postconditions, dict):
        _map = parse_condition_dict(postconditions)
    else:
        _map = parse_condition(postconditions)

    return _map['stat'], _map['cmp_type'], _map['value']


""" unless postcondition manually specified
    determine if postcondition status can be checked using
    bucket-engine, ep-engine, view-engine, ssh...etc """
def getPostConditionMethod(workload):
    bucket = str(workload.bucket)
    postcondition = workload.postconditions

    if isinstance(postcondition, dict):
        params = parse_condition_dict(postcondition)
    else:
        params = parse_condition(postcondition)

    host, port = get_ep_hostip_from_params(params)

    method = None
    if 'method' in params:
        method = params['method']

    if method is None:
        stat = params['stat']
        if stat in BucketStatChecker(bucket).stat_keys:
            method = "bucket_stat_checker"
        elif stat in EPStatChecker(host, port).stat_keys:
            method = "epengine_stat_checker"

        if "active_task_type" in params and "target_value" in params:
            type = params['active_task_type']
            target_value = params['target_value']
            keys = ActiveTaskChecker(type, target_value).type_keys()
            if type in keys:
                method = "active_tasks_stat_checker"

    return method

""" handles checks against bucket post conditions exposed via ns_server rest call.
    should be able to handle any stats avaiable in UI"""
def bucket_stat_checker(workload):
    bucket = str(workload.bucket)
    postcondition = workload.postconditions

    statChecker = BucketStatChecker(bucket)
    return statChecker.check(postcondition)

""" handles checks against ep-engine post conditions typically exposed via cbstats all."""
def epengine_stat_checker(workload):

    postcondition = workload.postconditions

    if isinstance(postcondition, dict):
        params = parse_condition_dict(postcondition)
    else:
        params = parse_condition(postcondition)

    random_host, port = get_ep_hostip_from_params(params)

    status = True
    all_hosts = [random_host]
    if params['cluster_check'] == True:
        clusterStatus = CacheHelper.clusterstatus(cfg.CB_CLUSTER_TAG+"_status")
        all_hosts = clusterStatus.get_all_hosts()

    for host in all_hosts:
        statChecker = EPStatChecker(host.split(":")[0], port)
        status &= statChecker.check(postcondition)

    return status

def active_tasks_stat_checker(workload):

    status = False
    postcondition = workload.postconditions
    equality = None

    if isinstance(postcondition, dict):
        params = parse_condition_dict(postcondition)
        postcondition = postcondition['conditions']
    else:
        params = parse_condition(postcondition)

    active_task_type = "indexer"
    target_value = "_design/ddoc1"

    if 'active_task_type' in params:
        active_task_type = params['active_task_type']
    if 'target_value' in params:
        target_value = params['target_value']

    statChecker = ActiveTaskChecker.from_cache()
    if statChecker is None:
        status = False
    else:
        status = statChecker.check(postcondition)

    return status

def get_ep_hostip_from_params(params):
    app.workload_manager.updateClusterStatus()
    clusterStatus = CacheHelper.clusterstatus(cfg.CB_CLUSTER_TAG+"_status")
    random_host = None
    try:
        random_host = clusterStatus.get_random_host().split(":")[0]
    except AttributeError:
        logger.error("Can not fetch cluster status information")
        pass

    host = params.get('ip') or random_host or cfg.COUCHBASE_IP
    port = params.get('port') or 11210

    return host, int(port)

class StatChecker(object):

    def check(self, condition):

        valid = False
        try:
            stat, cmp_type, value = default_condition_params(condition)
        except AttributeError as ex:
            logger.error(ex)
            return valid

        try:
            value = int(value)
        except ValueError:
            pass # leave as string

        stats = self.get_stats()
        if stats is None:
            return False

        if len(stats) > 0:
            try:
                curr_value = stats[stat]
            except:
                logger.error('Invalid Stat Key: %s' % stat)

                # invalid stat key
                return valid

            if (cmp_type  == EQUAL and curr_value == value) or\
                (cmp_type == NOT_EQUAL and curr_value != value) or\
                (cmp_type == LESS_THAN_EQ and curr_value <= value) or\
                (cmp_type == GREATER_THAN_EQ and curr_value >= value) or\
                (cmp_type == LESS_THAN and curr_value < value) or\
                (cmp_type == GREATER_THAN and curr_value > value):
                valid = True

        return valid


class BucketStatChecker(StatChecker):

    def __init__(self, bucket = "default",
                 addr = cfg.COUCHBASE_IP +":"+cfg.COUCHBASE_PORT,
                 username = cfg.COUCHBASE_USER,
                 password = cfg.COUCHBASE_PWD):

        self.ip, self.port = addr.split(":")
        self.username = username
        self.password = password
        self.bucket = bucket
        self.rest = create_rest(self.ip, self.port, self.username, self.password)

    @property
    def stat_keys(self):
        keys = []

        stats = self.get_stats()
        if stats:
            keys = list(stats.keys())

        return keys

    def get_stats(self):
        stats = None
        try:
            stats = self.rest.get_bucket_stats(self.bucket)
        except ValueError:
            logger.error("unable to get stats for bucket: %s" % self.bucket)

        return stats

    def get_curr_items(self):
        curr_items = -1
        stats = self.get_stats()
        if stats is not None and 'curr_items' in stats:
            curr_items = stats['curr_items']

        return curr_items


class EPStatChecker(StatChecker):

    def __init__(self,
                 host = cfg.COUCHBASE_IP,
                 port = 11210):
        self.host = host
        self.port = port
        self.client = None

        try:
            self.client = MemcachedClient(self.host, self.port)
        except MemcachedError as ex:
            msg = "error connecting to host %s for gathering postconditions"\
                   %  self.host
            logger.error(msg)
            pass

    @property
    def stat_keys(self):
        keys = []

        stats = self.get_stats()
        if stats:
            keys = list(stats.keys())
        return keys

    def get_stats(self):
        stats = None

        if self.client:
            try:
                stats = self.client.stats()
            except Exception:
                logger.info("unable to get stats from host: %s:%s" %\
                    (self.host, self.port))

        return stats

class ActiveTaskChecker(StatChecker):

    def __init__(self, active_task_type = "indexer", target_value = "_design/ddoc1",
                 addr = cfg.COUCHBASE_IP +":"+cfg.COUCHBASE_PORT,
                 username = cfg.COUCHBASE_USER,
                 password = cfg.COUCHBASE_PWD):

        self.initialized = False
        self.id = cfg.CB_CLUSTER_TAG+"active_task_status"
        self.ip, self.port = addr.split(":")
        self.username = username
        self.password = password
        self.type = active_task_type
        self.target_value = target_value
        self.rest = create_rest(self.ip, self.port, self.username, self.password)
        self.task_started = False
        self.empty_stat_count = 0
        self.known_types = ["design_documents",
                            "view_compaction",
                            "bucket_compaction",
                            "indexer",
                            "initial_build",
                            "original_target"]

        self.initialized = True

    def type_keys(self):
        types = self.known_types

        try:
            stats = self.rest.active_tasks()

        except ValueError:
            logger.error("unable to get stats for active tasks")

        for stat in stats:
            # append any new keys returned by the server
            # not part of known list of supported keys
            type_ = stat["type"]
            if type_ not in types:
                types.append(type_)

        return types

    def get_stats(self):
        stats = None
        target_key = ""

        if self.type == "indexer":
            target_key = "design_documents"
            if self.target_value.lower() in ['true', 'false']:
                #track initial_build indexer task
                target_key = "initial_build"
        elif self.type == "bucket_compaction":
            target_key = "original_target"
        elif self.type == "view_compaction":
            target_key = "design_documents"
        else:
            raise Exception("type %s is not defined!" % self.type)

        try:
            tasks = self.rest.active_tasks()
            for task in tasks:
                if task["type"] == self.type and ((
                    target_key == "design_documents" and\
                        self.target_value in [ddoc for ddoc in task[target_key]]) or (
                    target_key == "original_target" and task[target_key] == self.target_value) or (
                    target_key == "initial_build" and str(task[target_key]) == self.target_value)):
                    stats = task
                    break

        except ValueError:
            logger.error("unable to get stats for active tasks")

        if stats is None:
            if (self.empty_stat_count == 5 and self.task_started)\
                or self.empty_stat_count == 20:
                    # assume progress is complete if task started
                    # but is no longer reporting stats
                    # or if we've retrieved 20 empty stat results
                    stats = {'progress' : 100}
            else:
                self.empty_stat_count = self.empty_stat_count + 1
        else:
            # retrieved stats from task, therefore it has started
            self.task_started = True

        return stats


    def __setattr__(self, name, value):
        super(ActiveTaskChecker, self).__setattr__(name, value)

        # auto cache object when certain keys change
        if self.initialized and name in ("initialized", "task_started", "empty_stat_count"):
            ObjCacher().store(CacheHelper.ACTIVETASKCACHEKEY, self)

    @staticmethod
    def from_cache():
        id_ = cfg.CB_CLUSTER_TAG+"active_task_status"
        return ObjCacher().instance(CacheHelper.ACTIVETASKCACHEKEY, id_)
