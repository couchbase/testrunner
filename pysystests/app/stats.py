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

# Assumption is that celery workers and cluster nodes
# are always in time sync.

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
            logger.error("Creating Stats Cache for node %s" % node.ip)
            node_stats = NodeStats(node.ip)

        last_updated_time = node_stats.get_last_updated_time()
        sample = get_atop_sample(node.ip, ts=last_updated_time)
        node_stats.set_last_updated_time(ts=time.time())
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

@celery.task
def sync_time():
    rest = create_rest()
    nodes = rest.node_statuses()

    for node in nodes:
        cmd = "ntpdate pool.ntp.org"
        exec_cmd(node.ip, cmd)

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
        if key != 'ip' and key != 'worker_timestamp':
            if key not in node_stats.samples:
                node_stats.samples[key] = []
            # sample[key] would be a list of samples
            node_stats.samples[key].extend(sample[key])

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

def get_atop_sample(ip, ts=None):
    sample = {"ip" : ip}
    if ts:
        cpu_samples = atop_cpu(ip, "beam.smp", ts)
        mem_samples = atop_mem(ip, "beam.smp", ts)
        disk_samples = atop_dsk(ip,"memcached", ts)
        swap = sys_swap(ip)

        if cpu_samples:
            sample.update({"PRC_beam" : cpu_samples})
        if mem_samples:
            sample.update({"PRM_beam" : mem_samples})
        if disk_samples:
            sample.update({"PRD_memcached" : disk_samples})
        if swap:
            sample.update({"swap" : swap[0][0]})

        sample.update({"worker_timestamp" : str(time.time())})
    return sample

def atop_cpu(ip, process=None, ts=None):
    if process and ts:
        # utc, cpu_usr, cpu_sys, cpu_curr
        cmd = "grep %s" % process + "|" + "grep PRC | grep -v bash |" + "awk '{print $3,$11,$12,$17}'"
        return _atop_exec(ip, cmd, ts)

def atop_mem(ip, process=None, ts=None):
    if process and ts:
        # utc, virt_mem_size, rsize, virt_mem_growth, rsize_growth, minor_page_fauls, major_page_faults
        cmd = "grep %s" % process + "|" + "grep PRM | grep -v bash |" + "awk '{print $3,$11,$12,$14,$15,$16,$17}'"
        return _atop_exec(ip, cmd, ts)

def sys_swap(ip):
    cmd = "free | grep Swap | awk '{print $3}'"
    return exec_cmd(ip, cmd)

def atop_dsk(ip, process=None, ts=None):
    if process and ts:
        # utc, num_disk_reads, num_disk_writes
        cmd = "grep %s" % process + "|" + "grep PRD | grep -v bash |" + "awk '{print $3,$12,$14}'"
        return _atop_exec(ip, cmd, ts)

def _atop_exec(ip, cmd, ts=None):

    res = None
    hh_mm = convert_epoch(int(ts))
    #Get the parseable atop output
    prefix = "atop -r "+cfg.ATOP_LOG_FILE+" -b %s " % hh_mm + " -P ALL"
    cmd = prefix + "|" + cmd
    rc  = exec_cmd(ip, cmd)
    # parse result based on what is expected from atop commands
    if len(rc[0]) > 0:
        res = rc[0]
    return res

def convert_epoch(ts=None):
    l = time.strftime('%H:%M', time.localtime(ts))
    return l

def exec_cmd(ip, cmd, os = "linux"):
    #TODO: Cache ssh connections
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

    def get_curr_items(self):
        stats = self.rest.get_bucket_stats(self.bucket)
        return stats['curr_items']

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
        self.last_updated_time = self.start_time

    def get_run_interval(self):
        curr_time = time.time()
        runtime_s = curr_time - self.start_time
        return str(datetime.timedelta(seconds=runtime_s))[:-4]

    def get_last_updated_time(self):
        return self.last_updated_time

    def get_start_time(self):
        return self.start_time

    def set_last_updated_time(self, ts=None):
        self.last_updated_time = ts

#    def __repr__(self):
#        interval = self.get_run_interval()
#        print self.results
#        str_ = "\n"
#        str_ = str_ + "Runtime: %20s \n" % interval
#        for key in self.results:
#            str_ = str_ + "%10s: " % (key)
#            for k, v  in self.results[key].items():
#                str_ = str_ + "%10s: %10s"  % (k,v)
#            str_ = str_ + "\n"
#
#        return str_

    def __setattr__(self, name, value):
        super(NodeStats, self).__setattr__(name, value)
        ObjCacher().store(CacheHelper.NODESTATSCACHEKEY, self)

    # Takes data as list of value
    def compute_stats(self, key, data):
        data = map(int, data)
        if len(data) > 0:
            # Sort data to compute
            data.sort()
            idx = int(math.ceil((len(data)) * 0.99))
            if idx %2==0:
                nn_perctile = (data[idx - 1] + data[idx-2])/2.0
            else:
                nn_perctile = data[idx - 1]
            mean = sum(data) / float(len(data))

            self.results[key] = {'mean': mean,
                                 'max': max(data),
                                 '99th': "%.2f" % nn_perctile,
                                 "samples" : len(data)
                                }

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
    logger.error(node_stats.results)
    new_line()

def print_separator():
    logger.error("---------------------------------------")

def new_line():
    logger.error("\n")

def calculate_node_stat_results(node_stats):

    # calculate results
    for k, data in node_stats.samples.items():
        if k == 'worker_timestamp' : continue

        if k not in node_stats.results:
            node_stats.results[k] = {}

        # for each stat key, calculate
        # mean, max, and 99th %value
        if k == 'PRC_beam':
            key = 'curr_cpu'
            temp = [sample.split()[2] for sample in data]
            node_stats.compute_stats(key, temp)
        elif k == 'PRD_memcached':
            # num_disk_write
            key = 'disk_write_memcached'
            temp = [sample.split()[1] for sample in data]
            node_stats.compute_stats(key, temp)
            # num_disk_reads
            key = 'disk_read_memcached'
            temp = [sample.split()[2] for sample in data]
            node_stats.compute_stats(key, temp)
        elif k =='PRM_beam':
            key = 'rsize'
            temp = [sample.split()[2] for sample in data]
            node_stats.compute_stats(key, temp)
        elif k == 'swap':
            key = 'swap'
            temp = data
            node_stats.compute_stats(key, temp)
