from __future__ import absolute_import

import re
import sys
import math
import time
import datetime
from app.celery import celery
import testcfg as cfg
from cache import ObjCacher, CacheHelper

sys.path=["../lib"] + sys.path
from membase.api.rest_client import RestConnection
from remote.remote_util import RemoteMachineShellConnection
from app.rest_client_tasks import create_rest, create_ssh_conn

from celery.utils.log import get_task_logger
logger = get_task_logger("app.stats")

@celery.task
def resource_monitor():

    rest = create_rest()
    nodes = rest.node_statuses()

    # cache sample of latest stats on all nodes
    for node in nodes:

        # check if atop running (could be new node)
        if isinstance(node.ip, unicode):
            node.ip = str(node.ip)
        if check_atop_proc(node.ip):
            restart_atop(node.ip)

        # retrieve stats from cache
        node_stats = NodeStats.from_cache(node.ip)

        if node_stats is None:
            node_stats = NodeStats(node.ip)

        # get stats from node
        sample = get_atop_sample(node.ip)

        # update collection with cbstats
        sample.update(get_cbstat_sample(node.ip))

        # update collection with cbstats
        sample.update(get_du_sample(node.ip))

        # update node state object
        update_node_stats(node_stats, sample)

    return True

@celery.task
def atop_log_rollover():
    """ task to run every 3 hours to roll over atop logs
        if atop is currently running it  will be
        manually stopped and logs backed up before
        starting new instance"""
    logger.error("Rolling over logs")
    rest = create_rest()
    nodes = rest.node_statuses()

    for node in nodes:
        if check_atop_proc(node.ip):
            stop_atop(node.ip)

        backup_log(node.ip)
        start_atop(node.ip)

def backup_log(ip):
    atop_dir = "/".join(cfg.ATOP_LOG_FILE.split("/")[:-1])
    atop_file = cfg.ATOP_LOG_FILE.split("/")[-1]
    last_index_cmd = "ls  %s | grep %s | sort | tail -1 |  sed 's/.*%s\.\?//'" %\
        (atop_dir,  atop_file, atop_file)
    res = exec_cmd(ip, last_index_cmd)

    last_index = 0
    if len(res[0]) > 0 :
        last_index = res[0][0]

    new_index = int(last_index) + 1
    new_atop_file = "%s.%s" % (cfg.ATOP_LOG_FILE, new_index)
    backup_cmd = "mv %s %s" % (cfg.ATOP_LOG_FILE, new_atop_file)
    exec_cmd(ip, backup_cmd)

def update_node_stats(node_stats, sample):

    for key in sample.keys():
        if key != 'ip':

            if key not in node_stats.samples:
                node_stats.samples[key] = []

            val = float(re.sub(r'[^\d.]+', '', sample[key]))
            node_stats.samples[key].append(val)

    ObjCacher().store(CacheHelper.NODESTATSCACHEKEY, node_stats)

def check_atop_proc(ip):
    running = False
    proc_signature = atop_proc_sig()
    res = exec_cmd(ip, "ps aux |grep '%s' | wc -l " % proc_signature)

    # one for grep, one for atop and one for bash
    # Making sure, we always have one such atop
    if int(res[0][0]) != 3:
        running = True

    return running

def restart_atop(ip):
    stop_atop(ip)
    start_atop(ip)

def stop_atop(ip):
    cmd = "killall atop"
    exec_cmd(ip, cmd)
    # Clean up load log
    cmd = "rm -rf %s" % cfg.ATOP_LOG_FILE
    exec_cmd(ip, cmd)

def start_atop(ip):
    # Start atop again
    cmd = "nohup %s > /dev/null 2>&1&" % atop_proc_sig()
    exec_cmd(ip, cmd)

def atop_proc_sig():
    return "atop -a -w %s 3" % cfg.ATOP_LOG_FILE

def get_du_sample(ip):
    sample = {}
    cmd = "df -h | grep data | awk '{ print $2 }'"
    rc = exec_cmd(ip,cmd)
    if rc:
        sample.update({"disk_used" : rc[0][0]})

    return sample

def get_cbstat_sample(ip):
    sample = {}
    curr_items = cbstat_curr_items(ip)
    dwq = cbstat_disk_write_queue(ip)
    ep_bg = cbstat_ep_bg_fetched(ip)
    ep_bg_wait = cbstat_ep_bg_wait(ip)

    if curr_items:
        sample.update({"curr_items" : curr_items[1]})

    if dwq >= 0:
        sample.update({"disk_wq" : dwq})

    if ep_bg:
        sample.update({"bg_fetched" : ep_bg[1]})

    if ep_bg_wait:
        sample.update({"bg_fetch_wait" : ep_bg_wait[1]})

    return sample

def get_atop_sample(ip):

    sample = {"ip" : ip}
    cpu = atop_cpu(ip)
    mem = atop_mem(ip)
    swap = sys_swap(ip)
    disk = atop_dsk(ip)
    if cpu:
        sample.update({"sys_cpu" : cpu[0],
                       "usr_cpu" : cpu[1]})
    if mem:
        sample.update({"vsize" : mem[0],
                       "rsize" : mem[1]})

    if swap:
        sample.update({"swap" : swap[0][0]})

    if disk:
        sample.update({"rddsk" : disk[0],
                       "wrdsk" : disk[1],
                       "disk_util" : disk[2]})

    sample.update({"ts" : str(time.time())})

    return sample

def atop_cpu(ip):
    cmd = "grep ^CPU | grep sys | awk '{print $4,$7}' "
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
    cmd = "grep beam.smp | awk '{print $2, $3, $5}'"
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

def cbstat_curr_items(ip):

    cmd = "grep curr_items: | head -1"
    return _cbtop_exec(ip, cmd)

def cbstat_disk_write_queue(ip):

    cmd = "grep ep_queue_size: | head -1"
    qsize = _cbtop_exec(ip, cmd)
    if qsize:
        cmd = "grep ep_flusher_todo: | head -1"
        fl_todo = _cbtop_exec(ip, cmd)
        if fl_todo:
            return int(qsize[1]) + int(fl_todo[1])

    return -1

def cbstat_ep_bg_fetched(ip):
    cmd = "grep ep_bg_fetched: | head -1"
    return  _cbtop_exec(ip, cmd)

def cbstat_ep_bg_wait(ip):
    cmd = "grep ep_bg_wait: | head -1"
    return  _cbtop_exec(ip, cmd)

# by default get stats from direct mc port
def _cbtop_exec(ip, cmd, port = 11210):

    res = None
    prefix = "/opt/couchbase/bin/cbstats localhost:%d all" % port
    cmd = prefix + "|" + cmd

    rc  = exec_cmd(ip, cmd)

    # parse result based on what is expected from atop commands
    if len(rc[0]) > 0:
        res = rc[0][0].split()
    return res

def exec_cmd(ip, cmd, os = "linux"):
    shell, node = create_ssh_conn(server_ip=ip, os=os)
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
        self.rest = create_rest(self.ip, self.port, self.username, self.password)

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

class NodeStats(object):
    def __init__(self, ip):
        self.id = ip
        self.samples = {}
        self.results = {}
        self.start_time = time.time()

    def get_run_interval(self):
        curr_time = time.time()
        runtime_s = curr_time - self.start_time
        return str(datetime.timedelta(seconds=runtime_s))[:-4]

    def __repr__(self):
        interval = self.get_run_interval()

        str_ = "\n"
        str_ = str_ + "Runtime: %20s \n" % interval
        for key in self.results:
            str_ = str_ + "%10s: " % (key)
            str_ = str_ + "current: %10s" % (self.samples[key][-1])
            for k, v  in self.results[key].items():
                str_ = str_ + "%10s: %10s"  % (k,v)
            str_ = str_ + "\n"
        return str_

    def __setattr__(self, name, value):
        super(NodeStats, self).__setattr__(name, value)
        ObjCacher().store(CacheHelper.NODESTATSCACHEKEY, self)

    @staticmethod
    def from_cache(id_):
        return ObjCacher().instance(CacheHelper.NODESTATSCACHEKEY, id_)

@celery.task
def generate_node_stats_report():

    allnodestats = CacheHelper.allnodestats()

    if len(allnodestats) > 0:
        # print current time at top of each report
        # TODO: add active tasks at time of report generation
        ts = time.localtime()
        ts_string = "%s/%s/%s %s:%s:%s" %\
            (ts.tm_year, ts.tm_mon, ts.tm_mday, ts.tm_hour, ts.tm_min, ts.tm_sec)
        print_separator()
        logger.error("\tSTAT REPORT: (%s)" % ts_string)

        for node_stats in allnodestats:
            calculate_node_stat_results(node_stats)

            if len(node_stats.results) > 0:
                print_node_results(node_stats)
        logger.error("\tEND OF REPORT: (%s)" % ts_string)
        print_separator()
        new_line()


def print_node_results(node_stats):
    print_separator()
    logger.error("\tNODE: (%s)" % node_stats.id)
    print_separator()
    logger.error(node_stats)
    new_line()

def print_separator():
    logger.error("---------------------------------------")

def new_line():
    logger.error("\n")

def calculate_node_stat_results(node_stats):


    # calculate results
    for k,data in node_stats.samples.items():
        if k == 'ts' : continue

        if k not in node_stats.results:
            node_stats.results[k] = {}

        # for each stat key, calculate
        # mean, max, and 99th %value
        data.sort()
        if len(data) > 0:
            idx = int(math.ceil((len(data)) * 0.99))
            if idx %2==0:
                nn_perctile = (data[idx - 1] + data[idx-2])/2.0
            else:
                nn_perctile = data[idx - 1]
        mean = sum(data) / float(len(data))

        node_stats.results[k] = {"mean" : "%.2f" % mean,
                                 "max"  : max(data),
                                 "99th" : "%.2f" % nn_perctile,
                                 "samples" : len(data)}
