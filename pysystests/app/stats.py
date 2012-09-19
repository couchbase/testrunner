from __future__ import absolute_import

import re
import sys
from app.celery import celery
import testcfg as cfg
from cache import WorkloadCacher

sys.path=["../lib"] + sys.path
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

@celery.task
def resource_monitor():

    cache = WorkloadCacher()
    for workload in cache.workloads:

        # filter on active workload
        if workload.active:
            bucket = str(workload.bucket)
            stat_checker = StatChecker(cfg.COUCHBASE_IP +":"+cfg.COUCHBASE_PORT,
                                       bucket = bucket,
                                       username = cfg.COUCHBASE_USER,
                                       password = cfg.COUCHBASE_PWD)


            nodes = stat_checker.nodes()
            for node in nodes:

                # check if atop running (could be new node)
                check_atop_proc(node.ip)

                # cache sample of latest stats on all nodes
                sample = get_atop_sample(node.ip)
                if node.ip not in workload.stats:
                    workload.stats.update({node.ip : []})
                workload.stats[node.ip].append(sample)

                #TODO: log to file, putting to stdout via error flag
                logger.error(workload.stats[node.ip][-1])

        WorkloadCacher().store(workload)

    return True

def check_atop_proc(ip):
    res = exec_cmd(ip, "ps aux | grep [a]top")
    if len(res[0]) == 0:
        # atop not running, start it
        cmd = "nohup atop -a -w %s 3 > /dev/null 2>&1&" % cfg.ATOP_LOG_FILE
        exec_cmd(ip, cmd)

def get_atop_sample(ip):

    cpu = atop_cpu(ip)
    mem = atop_mem(ip)
    swap = sys_swap(ip)
    wrdsk = atop_dsk(ip)

    return {"sys_cpu" : cpu[0],
            "usr_cpu" : cpu[1],
            "vsize" : mem[0],
            "rsize" : mem[1],
            "swap" : swap,
            "wrdsk" : wrdsk}

def atop_cpu(ip):
    cmd = "grep ^CPU | awk '{print $4,$7}' | tail -1"
    return _atop_exec(ip, cmd)

def atop_mem(ip):
    flags = "-M -m"
    cmd = "grep beam.smp | head -1 |  awk '{print $5,$6}'"
    return _atop_exec(ip, cmd, flags)

def sys_swap(ip):
    cmd = "free | grep Swap | awk '{print $3}'"
    return exec_cmd(ip, cmd)

def atop_dsk(ip):
    flags = "-d"
    cmd = "grep beam.smp | awk '{print $3}'"
    return _atop_exec(ip, cmd, flags)

def _atop_exec(ip, cmd, flags = ""):
    """ runs atop program where -b <begin_time> and -e <end_time>
    are the current times.  then filters the last sample of this collection
    via tail -1"""

    res = None

    #prefix standard atop prefix
    prefix = "date=`date +%H:%M` && atop -r "+cfg.ATOP_LOG_FILE+" -b $date -e $date " + flags
    cmd = prefix + "|" + cmd + " | tail -1"

    rc  = exec_cmd(ip, cmd)

    # parse result based on what is expected from atop commands
    if len(rc[0]) > 0:
        res = rc[0][0].split()
    return res

def exec_cmd(ip, cmd, os = "linux"):
    serverInfo = {"ip" : ip,
                  "port" : 22,
                  "ssh_username" : cfg.SSH_USER,
                  "ssh_password" : cfg.SSH_PASSWORD,
                  "ssh_key" : "",
                  "type" : os }

    node = _dict_to_obj(serverInfo)
    shell = RemoteMachineShellConnection(node)
    shell.use_sudo  = False
    return shell.execute_command(cmd, node)

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
        self.node = _dict_to_obj(serverInfo)
        self.rest = RestConnection(self.node)

    def check(self, condition, datatype = int):

        valid = False
        try:
            stat, cmp_type, value = self.parse_condition(condition)
        except AttributeError as ex:
            logger.error(ex)
            return valid

        value = datatype(value) 
        stats = self.rest.get_bucket_stats(self.bucket)
       
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

    def nodes(self):
        return self.rest.node_statuses()

    def parse_condition(self, condition):
        m = re.match(r"(\w+)(\W+)(\w+)", condition)
        try: 
           return [str(str_.strip()) for str_ in m.groups()]
        except AttributeError:
            logger.error("Invalid condition syntax: %s" % condition)
            raise AttributeError(condition)

def _dict_to_obj(dict_):
    return type('OBJ', (object,), dict_)

