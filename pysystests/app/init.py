import os
import time
from rabbit_helper import RabbitHelper
from cache import WorkloadCacher, TemplateCacher, BucketStatusCacher, cacheClean

# cleanup queues
rabbitHelper = RabbitHelper()

cached_queues = WorkloadCacher().queues +  TemplateCacher().cc_queues

test_queues = ["workload","workload_template"] + cached_queues

for queue in test_queues:
    try:
        if rabbitHelper.qsize(queue) > 0:
            print "Purge Queue: "+queue +" "+ str(rabbitHelper.qsize(queue))
            rabbitHelper.purge(queue)
    except Exception as ex:
        pass

cacheClean()

# kill+start sdk's
os.system("ps aux | grep sdkserver | awk '{print $2'} | xargs kill")
os.system("ruby sdkserver.rb &")
os.system("python sdkserver.py  &")
