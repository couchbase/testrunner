from __future__ import absolute_import

import re
import sys
import types

sys.path=["../lib"] + sys.path
from mc_bin_client import MemcachedClient, MemcachedError
from app.rest_client_tasks import create_rest
import testcfg as cfg
from app.celery import celery
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)


EQUAL = '='
NOT_EQUAL = '!='
LESS_THAN = '<'
LESS_THAN_EQ = '<='
GREATER_THAN = '>'
GREATER_THAN_EQ = '>='


""" default_condition_params method helps interpret postcondition string.
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

        # method match
        if match_method(condition_str, condition_map):
            pass

    if len(condition_map) == 0:
        logger.error("Invalid condition syntax: %s" % condition_map)
        raise AttributeError(condition_map)

    return condition_map

def match_ip(condition_str, condition_map):
    m = re.match(r"\s*?(\d+.\d+.\d+.\d+):?(\d+)?\s*?", condition_str)
    if not isinstance(m, types.NoneType):
        condition_map["ip"], condition_map["port"] = m.groups()


def match_equality(condition_str, condition_map):
    m = re.match(r"\s*?(?P<stat>[\w]+)(?P<cmp>[\s=<>]+)(?P<value>\w+)\s*?",
                 condition_str)
    if not isinstance(m, types.NoneType):
        condition_map["stat"] = m.group('stat').strip()
        condition_map["cmp_type"] = m.group('cmp').strip()
        condition_map["value"] = m.group('value').strip()

def match_method(condition_str, condition_map):
    m = re.match(r"\s*?(?P<method>\w+)\s*?$", condition_str)
    if not isinstance(m, types.NoneType):
        condition_map["method"] = m.group('method').strip()


def default_condition_params(postconditions):
    _map = parse_condition(postconditions)
    return _map['stat'],_map['cmp_type'],_map['value']


""" unless postcondition manually specified
    determine if postcondition status can be checked using
    bucket-engine, ep-engine, view-engine, ssh...etc """
def getPostConditionMethod(workload):
    bucket = str(workload.bucket)
    postcondition = workload.postconditions
    params = parse_condition(postcondition)

    method = None
    if 'method' in params:
        method = params['method']

    if method is None:
        stat = params['stat']
        if stat in BucketStatChecker(bucket).stat_keys:
            method = "bucket_stat_checker"
        elif stat in EPStatChecker().stat_keys:
            method = "epengine_stat_checker"

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

    params = default_condition_params(postcondition)

    ip = cfg.COUCHBASE_IP
    port = 11210
    if 'ip' in params:
        ip = params['ip']
    if 'port' in params:
        port = params['port']

    statChecker = EPStatChecker(ip, port)
    return statChecker.check(postcondition)

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
            keys = stats.keys()

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
            keys = stats.keys()
        return keys

    def get_stats(self):
        stats = None

        if self.client:
            try:
                stats = self.client.stats()
            except ValueError:
                logger.error("unable to get stats from host: %s" % self.host)

        return stats

