from __future__ import absolute_import

import re
import sys

sys.path=["../lib"] + sys.path
from membase.api.rest_client import RestConnection

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

class StatChecker(object):
    EQUAL = '=='
    NOT_EQUAL = '!='
    LESS_THAN = '<'
    LESS_THAN_EQ = '<='
    GREATER_THAN = '>'
    GREATER_THAN_EQ = '>='

    def __init__(self, addr, bucket = "default", username = "Administrator", password = "password"):
        self.ip, self.port = addr.split(":")
        self.username = username
        self.password = password
        self.bucket = bucket

        serverInfo = { "ip" : self.ip,
                       "port" : self.port,
                       "rest_username" : self.username,
                       "rest_password" : self.password }
        self.node = self._dict_to_obj(serverInfo)
        self.rest = RestConnection(self.node)

    def check(self, condition, datatype = int):

        valid = False
        try:
            stat, cmp_type, value = self.parse_condition(condition)
        except AttributeError:
            return valid

        value = datatype(value) 

        stats = self.rest.get_bucket_stats_for_node(self.bucket, self.node)
       
        if len(stats) > 0:
            try:
                curr_value = stats[stat]
            except:
                logger.error('Invalid Stat Key: %s' % stat)
            
                # invalid stat key
                return valid 

            if (cmp_type  == StatChecker.EQUAL and curr_value == value) or\
                (cmp_type == StatChecker.NOT_EQUAL and curr_value != value) or\
                (cmp_type == StatChecker.LESS_THAN_EQ and curr_value <= value) or\
                (cmp_type == StatChecker.GREATER_THAN_EQ and curr_value >= value) or\
                (cmp_type == StatChecker.LESS_THAN and curr_value < value) or\
                (cmp_type == StatChecker.GREATER_THAN and curr_value > value):
                valid = True
   
        return valid

    def parse_condition(self, condition):
        m = re.match(r"(\w+)(\W+)(\w+)", condition)
        try: 
           return [str(str_.strip()) for str_ in m.groups()]
        except AttributeError:
            logger.error("Invalid condition syntax: %s" % condition)
            raise AttributeError(condition)

    def _dict_to_obj(self, dict_):
        return type('OBJ', (object,), dict_)

