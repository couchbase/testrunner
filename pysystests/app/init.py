import os
import sys
from rabbit_helper import RabbitHelper
from cache import CacheHelper
import testcfg as cfg

# make sure logdir exists
os.system("mkdir -p "+cfg.LOGDIR)

#make sure celeybeat-schedule.db file is deleted
os.system("rm -rf celerybeat-schedule.db")


# kill old background processes
kill_procs=["sdkserver"]
for proc in kill_procs:
    os.system("ps aux | grep %s | awk '{print $2}' | xargs kill" % proc)

# cluster setup
try:
    cfg.CLUSTER
except NameError:
    cfg.CLUSTER = {}

if cfg.CLUSTER:
    if 'setitup' in cfg.CLUSTER and cfg.CLUSTER['setitup']:
        function = "setitup"
        if 'default_bucket' in cfg.CLUSTER and cfg.CLUSTER['default_bucket'] == False:
            function = function + ",default_bucket=" + str(cfg.CLUSTER['default_bucket'])
        if 'default_mem_quota' in cfg.CLUSTER:
            function = function + ",default_mem_quota=" + str(cfg.CLUSTER['default_mem_quota'])
        if 'sasl_buckets' in cfg.CLUSTER and cfg.CLUSTER['sasl_buckets'] > 0:
            function = function + ",sasl_buckets=" + str(cfg.CLUSTER['sasl_buckets'])
        if 'sasl_mem_quota' in cfg.CLUSTER:
            function = function + ",sasl_mem_quota=" + str(cfg.CLUSTER['sasl_mem_quota'])
        if 'standard_buckets' in cfg.CLUSTER and cfg.CLUSTER['standard_buckets'] > 0:
            function = function + ",standard_buckets=" + str(cfg.CLUSTER['standard_buckets'])
        if 'standard_mem_quota' in cfg.CLUSTER:
            function = function + ",standard_mem_quota=" + str(cfg.CLUSTER['standard_mem_quota'])

        if 'xdcr' in cfg.CLUSTER and cfg.CLUSTER['xdcr']:
            function = function + ",xdcr=True"
            if 'bidirection' in cfg.CLUSTER and cfg.CLUSTER['bidirection']:
                function = function + ",rdirection=bidirection"

        os.system("cd .. && ./testrunner -i " + cfg.CLUSTER['ini'] + " -t cluster_setup.SETUP." + function + " && cd pysystests")

# delete queues (note using --purge will remove cc_queues)
queues = CacheHelper.task_queues() + CacheHelper.miss_queues()

# when --purge set delete cc_queue's as well
# as seriesly db
if "--purge" in sys.argv:

    queues = set(CacheHelper.queues())

    # cleaning up seriesly database (fast and slow created by cbtop)
    if cfg.SERIESLY_IP != '':
        from seriesly import Seriesly
        os.system("curl -X DELETE http://{0}:3133/fast".format(cfg.SERIESLY_IP))
        os.system("curl -X DELETE http://{0}:3133/slow".format(cfg.SERIESLY_IP))
        os.system("curl -X DELETE http://{0}:3133/event".format(cfg.SERIESLY_IP))
        os.system("curl -X DELETE http://{0}:3133/atop".format(cfg.SERIESLY_IP))

        seriesly = Seriesly(cfg.SERIESLY_IP, 3133)
        seriesly.create_db('event')



for q_ in queues:
    try:
        RabbitHelper().delete(q_)
        print "Cleanup Queue: %s" % q_
    except Exception as ex:
        pass

# clean up cache
CacheHelper.cacheClean()

# start sdk server
os.system("python sdkserver.py  &")
