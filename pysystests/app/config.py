from __future__ import absolute_import
from kombu import Exchange, Queue
from datetime import timedelta
from celery.task.schedules import crontab
import testcfg as cfg

BROKER_URL = 'librabbitmq://'+cfg.RABBITMQ_IP

CELERY_ACKS_LATE = True
CELERYD_PREFETCH_MULTIPLIER = 1 
CELERY_DISABLE_RATE_LIMITS = True
CELERY_TASK_RESULT_EXPIRES = 5

#CELERYD_LOG_FILE='celeryd.log'

CELERYBEAT_SCHEDULE = { ## TODO schedule start of sdk imediately, and do not allow any ops until started

    'task_scheduler': {
    'task': 'app.workload_manager.taskScheduler',
    'schedule': timedelta(seconds=1),
    },
    'query_scheduler': {
    'task': 'app.query.queryScheduler',
    'schedule': timedelta(seconds=1),
    },

    'workload_consumer': {
    'task': 'app.workload_manager.workloadConsumer',
    'schedule': timedelta(seconds=2),
    },

    'admin_consumer': {
    'task': 'app.admin_manager.adminConsumer',
    'schedule': timedelta(seconds=2),
   },
    'xdcr_consumer': {
    'task': 'app.admin_manager.xdcrConsumer',
    'schedule': timedelta(seconds=2),
    },

    'postcondition_handler': {
    'task': 'app.workload_manager.postcondition_handler',
    'schedule': timedelta(seconds=2),
    },
    'cluster_resource_monitor': {
    'task': 'app.stats.resource_monitor',
    'schedule': timedelta(seconds=120), # every 2 minutes
    },

    'sync_time': {
    'task': 'app.stats.sync_time',
    #'schedule': crontab(minute=0, hour=0), #Execute daily at midnight.
    'schedule': timedelta(seconds=10800),
    },
    'atop_log_rollover': { # Execute every three hours
    'task': 'app.stats.atop_log_rollover',
    'schedule': timedelta(seconds=10800),
    },
    'generate_node_stats_report': { # every 2 minutes print out report from collected stats
    'task': 'app.stats.generate_node_stats_report',
    'schedule': timedelta(seconds=300),
    },
    'do_backup': { # every once per day
    'task': 'app.admin_manager.backup_task',
    'schedule': crontab(minute=0, hour=0), #Execute daily at midnight.
    'args': [cfg.ENABLE_BACKUPS]
    },
}

CELERY_QUEUES = (
    Queue('default',Exchange('default'), routing_key='default'),
    Queue('delete',   Exchange('memcached'), routing_key='memcached.delete'),
    Queue('set',    Exchange('memcached'), routing_key='memcached.set'),
    Queue('get',    Exchange('memcached'),   routing_key='memcached.get'),
    Queue('query',  Exchange('rest'),   routing_key='rest.query'),
    Queue('manager', Exchange('manager', type='topic'),   routing_key='workload.#'),
)   

CELERY_DEFAULT_QUEUE = 'default'
CELERY_DEFAULT_EXCHANGE_TYPE = 'direct'
CELERY_DEFAULT_ROUTING_KEY = 'default'

CELERY_ROUTES = ({'app.sdk_client_tasks.mset': {
                  'queue': 'set',
                  'routing_key': 'memcached.set'
                }},
                {'app.sdk_client_tasks.delete': {
                  'queue': 'delete',
                  'routing_key': 'memcached.delete'
                }},
                {'app.sdk_client_tasks.mget': {
                  'queue': 'get',
                  'routing_key': 'memcached.get'
                }},
                {'app.rest_client_tasks.query_view': {
                  'queue': 'query',
                  'routing_key': 'rest.query'
                }},
                {'app.workload_manager.workloadConsumer': {
                  'queue': 'manager',
                  'routing_key': 'workload.consumer'
                }},
                {'app.workload_manager.taskScheduler': {
                  'queue': 'manager',
                  'routing_key': 'workload.scheduler'
                }},
                {'app.query.queryScheduler': {
                  'queue': 'query',
                  'routing_key': 'rest.query'
                }},
                {'app.workload_manager.postcondition_handler': {
                  'queue': 'manager',
                  'routing_key': 'workload.postcondition'
                }},
                {'app.workload_manager.sysTestRunner': {
                  'queue': 'manager',
                  'routing_key': 'workload.systestrunner'
                }},
)
