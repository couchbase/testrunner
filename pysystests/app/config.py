from __future__ import absolute_import
from kombu import Exchange, Queue
from datetime import timedelta
from celery.task.schedules import crontab
import uuid
import testcfg as cfg


class BaseConfig(object):
    def __init__(self, types):

        self.BROKER_URL = 'librabbitmq://'+cfg.RABBITMQ_IP+'/'+cfg.CB_CLUSTER_TAG
        self.CELERY_ACKS_LATE = True
        self.CELERYD_PREFETCH_MULTIPLIER = 1
        self.CELERY_TASK_SERIALIZER = 'pickle'
        self.CELERY_DISABLE_RATE_LIMITS = True
        self.CELERY_TASK_RESULT_EXPIRES = 5
        self.CELERY_DEFAULT_EXCHANGE = 'default'
        self.CELERY_DEFAULT_EXCHANGE_TYPE = 'direct'
        self.CELERY_DEFAULT_ROUTING_KEY = 'default'
        self.CB_CLUSTER_TAG = cfg.CB_CLUSTER_TAG
        self.CELERY_DEFAULT_QUEUE =  cfg.CB_CLUSTER_TAG
        self.CELERYBEAT_SCHEDULE = {
            'systest_manager': {
                'task': 'app.systest_manager.systestManager',
                'schedule': timedelta(seconds=3),
                'args' : ('systest_manager_'+cfg.CB_CLUSTER_TAG,)
            },
        }


        default_ex = Exchange(self.CELERY_DEFAULT_EXCHANGE,
                              routing_key = self.CB_CLUSTER_TAG,
                              auto_delete = True,
                              durable = True)

        self.CELERY_QUEUES = (

            # queue for default routing
            Queue(self.CB_CLUSTER_TAG, default_ex, auto_delete = True),

            # queue for system test-case execution
            self.make_queue('systest_mgr_consumer', 'test.mgr', default_ex),
        )

        self.CELERY_ROUTES = (
            {'app.systest_manager.systestManager'  : self.route_args('systest_mgr_consumer','test.mgr') },
        )

        for type_ in types:
            if type_ == "kv" or type_ == "all":
                self.add_kvconfig()
            if type_ == "query" or type_ == "all":
                self.add_queryconfig()
            if type_ == "admin" or type_ == "all":
                self.add_adminconfig()
            if type_ == "stats" or type_ == "all":
                self.add_statsconfig()


    def make_queue(self, queue, routing_key = None, exchange = None):

        if exchange is None:
            exchange = Exchange(self.CELERY_DEFAULT_EXCHANGE,
                                type="direct",
                                auto_delete = True,
                                durable = True)

        queue, routing_key = self._queue_key_bindings(queue, routing_key)

        return Queue(queue,
                     exchange,
                     routing_key = routing_key,
                     auto_delete = True,
                     durable = True)


    def route_args(self, queue, routing_key):

        queue, routing_key = self._queue_key_bindings(queue, routing_key)
        return  {
                   'queue':  queue,
                   'routing_key': routing_key,
                }

    def _queue_key_bindings(self, queue, key):
        # every queue has a routing key for message delivery
        # in the case that key is None, set it equal to queue
        # as this is default behavior

        queue = "%s_%s" % (queue , self.CB_CLUSTER_TAG)
        routing_key = None
        if key is None:
            routing_key = queue
        else:
            routing_key = "%s.%s" % (self.CB_CLUSTER_TAG , key)
        return queue, routing_key

    def add_kvconfig(self):

        direct_ex = Exchange("kv_direct", type="direct", auto_delete = True, durable = True)
        topic_ex  = Exchange("kv_topic", type="topic", auto_delete = True, durable = True)

        self.CELERYBEAT_SCHEDULE.update(
        {
            'task_scheduler': {
                'task': 'app.workload_manager.taskScheduler',
                'schedule': timedelta(seconds=1),
            },
            'workload_consumer': {
                'task': 'app.workload_manager.workloadConsumer',
                'schedule': timedelta(seconds=2),
                'args' : ("workload_" + self.CB_CLUSTER_TAG, "workload_template_" + self.CB_CLUSTER_TAG)

            },
            'postcondition_handler': {
                'task': 'app.workload_manager.postcondition_handler',
                'schedule': timedelta(seconds=2),
            }
        })

        self.CELERY_QUEUES = self.CELERY_QUEUES +\
        (
            self.make_queue('delete',   'kv.delete', direct_ex),
            self.make_queue('set',      'kv.set', direct_ex),
            self.make_queue('get',      'kv.get', direct_ex),
            self.make_queue('kv_tasks',     'kv_tasks.#', topic_ex),
        )


        self.CELERY_ROUTES = self.CELERY_ROUTES +\
        (
            {'app.sdk_client_tasks.mdelete': self.route_args('delete','kv.delete') },
            {'app.sdk_client_tasks.mset'   : self.route_args('set','kv.set') },
            {'app.sdk_client_tasks.mget'  : self.route_args('get','kv.get') },
            {'app.workload_manager.workloadConsumer' : self.route_args('kv_tasks','kv_tasks.consumer') },
            {'app.workload_manager.taskScheduler' : self.route_args('kv_tasks','kv_tasks.scheduler') },
            {'app.workload_manager.postcondition_handler' : self.route_args('kv_tasks','kv_tasks.postcondition') },
            {'app.workload_manager.sysTestRunner' : self.route_args('kv_tasks','kv_tasks.systestrunner') },
        )


    def add_queryconfig(self):

        topic_ex  = Exchange("query_topic", type="topic", auto_delete = True, durable = True)
        direct_ex = Exchange("query_direct", type="direct", auto_delete = True, durable = True)

        self.CELERYBEAT_SCHEDULE.update(
        {
            'query_consumer': {
                'task': 'app.query.queryConsumer',
                'schedule': timedelta(seconds=2),
                'args' : ('query_'+self.CB_CLUSTER_TAG,)
            },
            'query_runner': {
                'task': 'app.query.queryRunner',
                'schedule': timedelta(seconds=1),
            },
        })

        self.CELERY_QUEUES = self.CELERY_QUEUES +\
            (
                # schedulable queue for multiple tasks
                self.make_queue('query_tasks',  'query_tasks.#', topic_ex),

                # high performance direct exhcnage for multi_query tasks
                self.make_queue('query_multi',  'query.multi', direct_ex),
            )

        self.CELERY_ROUTES = self.CELERY_ROUTES +\
        (   # route schedulable tasks both to same interal task queue
            {'app.query.queryConsumer': self.route_args('query_tasks','query_tasks.consumer')},
            {'app.query.activeRunner': self.route_args('query_tasks','query_tasks.runner')},
            {'app.rest_client_tasks.multi_query': self.route_args('query_multi','query.multi')},
        )

    def add_adminconfig(self):
        topic_ex  = Exchange("admin_topic", type="topic", auto_delete = True, durable = True)

        self.CELERYBEAT_SCHEDULE.update(
        {
            'admin_consumer': {
                'task': 'app.admin_manager.adminConsumer',
                'schedule': timedelta(seconds=2),
                'args' : ('admin_'+self.CB_CLUSTER_TAG,)
            },
             'xdcr_consumer': {
                 'task': 'app.admin_manager.xdcrConsumer',
                 'schedule': timedelta(seconds=2),
                'args' : ('xdcr_'+self.CB_CLUSTER_TAG,)
             },
            'do_backup': { # every once per day
                'task': 'app.admin_manager.backup_task',
                'schedule': crontab(minute=0, hour=0), #Execute daily at midnight.
                'args': [cfg.ENABLE_BACKUPS]
            },
        })

        self.CELERY_QUEUES = self.CELERY_QUEUES +\
            (
                # schedulable queue for multiple tasks
                self.make_queue('admin_tasks',  'admin_tasks.#', topic_ex),
            )

        self.CELERY_ROUTES = self.CELERY_ROUTES +\
        (   # route schedulable tasks both to same interal task queue
            {'app.admin_manager.adminConsumer':
                self.route_args('admin_tasks','admin_tasks.adminconsumer')},
            {'app.admin_manager.xdcrConsumer':
                self.route_args('admin_tasks','admin_tasks.xdcrconsumer')},
            {'app.admin_manager.backup_task':
                self.route_args('admin_tasks','admin_tasks.backuptasks')},
            {'app.rest_client_tasks.perform_admin_tasks':
                self.route_args('admin_tasks','admin_tasks.performadmin')},
            {'app.rest_client_tasks.perform_xdcr_tasks':
                self.route_args('admin_tasks','admin_tasks.performxdcr')},
        )


    def add_statsconfig(self):
        topic_ex  = Exchange("admin_topic", type="topic", auto_delete = True, durable = True)

        self.CELERYBEAT_SCHEDULE.update(
        {
            'cluster_resource_monitor': {
                'task': 'app.stats.resource_monitor',
                'schedule': timedelta(seconds=120), # every 2 minutes
            },
          # 'sync_time': {
          #     'task': 'app.stats.sync_time',
          #     'schedule': timedelta(seconds=10800),
          # },
          # 'atop_log_rollover': { # Execute every three hours
          #     'task': 'app.stats.atop_log_rollover',
          #     'schedule': timedelta(seconds=10800),
          # },
            'generate_node_stats_report': { # every 2 minutes print out report from collected stats
                'task': 'app.stats.generate_node_stats_report',
                'schedule': timedelta(seconds=360),
            },
        })

        self.CELERY_QUEUES = self.CELERY_QUEUES +\
            (
                # schedulable queue for multiple tasks
                self.make_queue('stats_tasks',  'stats_tasks.#', topic_ex),
            )

        self.CELERY_ROUTES = self.CELERY_ROUTES +\
        (
            # route schedulable tasks both to same interal task queue
            {'app.stats.resource_monitor':
                self.route_args('stats_tasks','stats_tasks.resourcemonitor')},
            {'app.stats.sync_time':
                self.route_args('stats_tasks','stats_tasks.synctime')},
            {'app.stats.atop_log_rollover':
                self.route_args('stats_tasks','stats_tasks.atop')},
            {'app.stats.generate_node_stats_report':
                self.route_args('stats_tasks','stats_tasks.genreport')},
        )

