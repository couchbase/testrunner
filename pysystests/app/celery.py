
import sys
from celery import Celery
from app.config import BaseConfig
from celery.app.log import Logging
from celery.signals import worker_process_init
import logging
import testcfg as cfg


celery = Celery(include=['app.sdk_client_tasks', 'app.rest_client_tasks', 'app.workload_manager', 'app.admin_manager', 'app.query', 'app.systest_manager'])
config = None


"""
    Worker behavior can be set in testcfg
    and also at start of worker using the -n option as 'name'

    examaple of starting 4 isolated workers on one vm with 2 process dedicated to each:

        celeryd-multi start kv query admin -A app --purge -l ERROR  -B -I:kv app.init \
        -n:kv kv -n:query query -n:admin admin  -c 2

    Where syntax (-n:kv  kv) means for worker named kv, start the kv scheduler , create it's queues and routes
    Note also init is only started once along with the kv worker, although it can be started as standalone.

    To start all types in a single worker do:

        celeryd-multi start all -A app --purge -l ERROR  -B  -n:all all -c 8

    One worker with two schedulers:

        celeryd-multi start kv query -A app --purge -l ERROR  -B -I:kv app.init \
        -n:kv kv -n:query query  -c 4

"""

if "-n" in sys.argv:
    opt_pos = sys.argv.index("-n")

    # retrieve option value
    workerTypes = sys.argv[opt_pos + 1].split(',')

    # create custom config
    config = BaseConfig(workerTypes)

    # remove from worker opts
    # so we don't crash when it's
    # started in main since
    # comma separated list isn't acceptable here
    del sys.argv[opt_pos:opt_pos+2]

if config is None:
    config = BaseConfig(cfg.WORKER_CONFIGS)

celery.config_from_object(config)

def setup_logging(**kw):
    setup_query_logger()

def setup_query_logger():
    logger = logging.getLogger('app.rest_client_tasks')
    handler = logging.FileHandler(cfg.LOGDIR+'/celery-query.log')
    formatter = logging.Formatter(logging.BASIC_FORMAT) # you may want to customize this.
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False


worker_process_init.connect(setup_logging)
