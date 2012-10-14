import os
from rabbit_helper import RabbitHelper
from cache import CacheHelper
import testcfg as cfg

# make sure logdir exists
os.system("mkdir -p "+cfg.LOGDIR)

#make sure celeybeat-schedule.db file is deleted
os.system("rm -rf celerybeat-schedule.db")

CacheHelper.cacheClean()

# kill old background processes
kill_procs=["sdkserver"]
for proc in kill_procs:
    os.system("ps aux | grep %s | awk '{print $2}' | xargs kill" % proc)

# start sdk server
os.system("python sdkserver.py  &")
