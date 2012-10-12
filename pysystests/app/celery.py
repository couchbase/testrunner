from __future__ import absolute_import

from celery import Celery
from app.config import BaseConfig
from celery.app.log import Logging
from celery.signals import worker_process_init
import logging
import testcfg as cfg


celery = Celery(include=['app.sdk_client_tasks','app.rest_client_tasks','app.workload_manager','app.stats','app.admin_manager','app.query'])
config = BaseConfig(cfg.WORKER_CONFIGS)
celery.config_from_object(config)

# setup celery process logger


def stats_tasks_setup_logging(**kw):
    logger = logging.getLogger('app.stats')
    handler = logging.FileHandler(cfg.LOGDIR+'/celery-stats.log')
    formatter = logging.Formatter(logging.BASIC_FORMAT) # you may want to customize this.
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False

worker_process_init.connect(stats_tasks_setup_logging)
