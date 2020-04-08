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
kill_procs=["consumer"]
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
        seriesly = Seriesly(cfg.SERIESLY_IP, 3133)
        dbs = seriesly.list_dbs()
        for db in dbs:
            seriesly.drop_db(db)

        seriesly.create_db('event')



for q_ in queues:
    try:
        RabbitHelper().delete(q_)
        print("Cleanup Queue: %s" % q_)
    except Exception as ex:
        pass

# clean up cache
CacheHelper.cacheClean()

# start local consumer
exchange = cfg.CB_CLUSTER_TAG+"consumers"
RabbitHelper().exchange_declare(exchange, "fanout")
os.system("python consumer.py  &")
os.system("ulimit -n 10240")


