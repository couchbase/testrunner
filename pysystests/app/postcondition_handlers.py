from __future__ import absolute_import

import re
import sys
sys.path=["../lib"] + sys.path
from app.rest_client_tasks import create_rest
import testcfg as cfg

from app.celery import celery
from celery.utils.log import get_task_logger


EQUAL = '=='
NOT_EQUAL = '!='
LESS_THAN = '<'
LESS_THAN_EQ = '<='
GREATER_THAN = '>'
GREATER_THAN_EQ = '>='

""" handles checks against bucket post conditions exposed via ns_server rest call.
    should be able to handle any stats avaiable in UI"""
def bucket_stat_checker(postcondition, bucket = "default"):
    statChecker = BucketStatChecker(bucket)
    return statChecker.check(postcondition)

class BucketStatChecker(object):

    def __init__(self, bucket = "default",
                 addr = cfg.COUCHBASE_IP +":"+cfg.COUCHBASE_PORT,
                 username = cfg.COUCHBASE_USER,
                 password = cfg.COUCHBASE_PWD):

        self.ip, self.port = addr.split(":")
        self.username = username
        self.password = password
        self.bucket = bucket
        self.rest = create_rest(self.ip, self.port, self.username, self.password)

    def get_curr_items(self):
        curr_items = -1
        stats = self.get_bucket_stats()
        if stats is not None and 'curr_items' in stats:
            curr_items = stats['curr_items']

        return curr_items

    def get_bucket_stats(self):
        stats = None
        try:
            stats = self.rest.get_bucket_stats(self.bucket)
        except ValueError:
            logger.error("unable to get stats for bucket: %s" % self.bucket)

        return stats


    def check(self, condition, datatype = int):

        valid = False
        try:
            stat, cmp_type, value = self.parse_condition(condition)
        except AttributeError as ex:
            logger.error(ex)
            return valid

        value = datatype(value)
        stats = self.get_bucket_stats()

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

    def nodes(self):
        return self.rest.node_statuses()

    def parse_condition(self, condition):
        m = re.match(r"(\w+)(\W+)(\w+)", condition)
        try:
           return [str(str_.strip()) for str_ in m.groups()]
        except AttributeError:
            logger.error("Invalid condition syntax: %s" % condition)
            raise AttributeError(condition)


