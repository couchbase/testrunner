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
