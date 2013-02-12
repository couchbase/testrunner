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

# cluster setup
if cfg.CLUSTER:
    if 'setitup' in cfg.CLUSTER and cfg.CLUSTER['setitup']:
        function = "setitup"
        if 'default_bucket' in cfg.CLUSTER and cfg.CLUSTER['default_bucket'] == False:
            function = function + ",default_bucket=" + cfg.CLUSTER['default_bucket']
        if 'sasl_buckets' in cfg.CLUSTER and cfg.CLUSTER['sasl_buckets'] > 0:
            function = function + ",sasl_buckets=" + cfg.CLUSTER['sasl_buckets']
        if 'standard_buckets' in cfg.CLUSTER and cfg.CLUSTER['standard_buckets'] > 0:
            function = function + ",standard_buckets=" + cfg.CLUSTER['standard_buckets']

        if 'xdcr' in cfg.CLUSTER and cfg.CLUSTER['xdcr']:
            function = function + ",xdcr=True"
            if 'bidirection' in cfg.CLUSTER and cfg.CLUSTER['bidirection']:
                function = function + ",rdirection=bidirection"

        os.system("cd .. && ./testrunner -i " + cfg.CLUSTER['ini'] + " -t cluster_setup.SETUP." + function + " && cd pysystests")

# cleaning up seriesly database (fast and slow created by cbtop)

if cfg.SERIESLY_IP != '':
    from seriesly import Seriesly
    os.system("curl -X DELETE http://{0}:3133/fast".format(cfg.SERIESLY_IP))
    os.system("curl -X DELETE http://{0}:3133/slow".format(cfg.SERIESLY_IP))
    os.system("curl -X DELETE http://{0}:3133/event".format(cfg.SERIESLY_IP))
    os.system("curl -X DELETE http://{0}:3133/atop".format(cfg.SERIESLY_IP))

    seriesly = Seriesly(cfg.SERIESLY_IP, 3133)
    seriesly.create_db('event')

# start sdk server
os.system("python sdkserver.py  &")
