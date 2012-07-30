from kombu import Exchange, Queue
from datetime import timedelta
import testcfg as cfg
 
BROKER_URL = 'librabbitmq://'+cfg.RABBITMQ_IP
CELERY_RESULT_BACKEND = 'cache'
CELERY_TASK_SERIALIZER = 'pickle'
CELERY_RESULT_SERIALIZER = 'pickle'
CELERY_TIMEZONE = 'Europe/Oslo'
CELERY_ENABLE_UTC = True
CELERY_CACHE_BACKEND = 'memcached://%s:%s' % (cfg.RESULT_CACHE_IP, cfg.RESULT_CACHE_PORT)
CELERY_CACHE_BACKEND_OPTIONS = {"binary": True,
                               "behaviors": {"tcp_nodelay": True}}
CELERY_ACKS_LATE = True
CELERYD_PREFETCH_MULTIPLIER = 1 
CELERY_DISABLE_RATE_LIMITS = True

#CELERYD_LOG_FILE='celeryd.log'

CELERYBEAT_SCHEDULE = { ## TODO schedule start of sdk imediately, and do not allow any ops until started

    'task_scheduler': {
    'task': 'app.workload_manager.taskScheduler',
    'schedule': timedelta(seconds=1),
    },
    'workload_consumer': {
    'task': 'app.workload_manager.workloadConsumer',
    'schedule': timedelta(seconds=2),
    },
    'postcondition_handler': {
    'task': 'app.workload_manager.postcondition_handler',
    'schedule': timedelta(seconds=2),
    },

#   'cluster_health_checker': {
#   'task': 'app.stats.health_checker',
#   'schedule': timedelta(seconds=10),
#   },
  
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
                {'app.rest_client_worker.query_view': {
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
                {'app.workload_manager.postcondition_handler': {
                  'queue': 'manager',
                  'routing_key': 'workload.postcondition'
                }},
                {'app.workload_manager.sysTestRunner': {
                  'queue': 'manager',
                  'routing_key': 'workload.systestrunner'
                }},
)
