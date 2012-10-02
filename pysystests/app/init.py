import os
from rabbit_helper import RabbitHelper
from cache import WorkloadCacher, TemplateCacher, BucketStatusCacher, cacheClean
import testcfg as cfg

def worker_init():
    # cleanup queues
    rabbitHelper = RabbitHelper()

    cached_queues = WorkloadCacher().queues +  TemplateCacher().cc_queues
    test_queues = ["workload","workload_template", "admin_tasks", "xdcr_tasks"] + cached_queues

    for queue in test_queues:
        try:
            if rabbitHelper.qsize(queue) > 0:
                print "Purge Queue: "+queue +" "+ str(rabbitHelper.qsize(queue))
                rabbitHelper.purge(queue)
        except Exception as ex:
            print ex

    cacheClean()

    # kill old background processes
    kill_procs=["sdkserver"]
    for proc in kill_procs:
        os.system("ps aux | grep %s | awk '{print $2}' | xargs kill" % proc)

    # start sdk servers
    os.system("ruby sdkserver.rb &")
    os.system("python sdkserver.py  &")

    # make sure logdir exists
    os.system("mkdir -p "+cfg.LOGDIR)
