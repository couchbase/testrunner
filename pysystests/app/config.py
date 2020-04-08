
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

            # queue for cluster status tasks
            self.make_queue('cluster_status', 'cluster.status', default_ex),

            self.make_queue('phase_status', 'phase.status', default_ex),
            self.make_queue('run_phase', 'run.phase', default_ex),
        )

        self.CELERY_ROUTES = (
            {'app.systest_manager.systestManager'  : self.route_args('systest_mgr_consumer', 'test.mgr') },
            {'app.systest_manager.get_phase_status'  : self.route_args('phase_status', 'phase.status') },
            {'app.systest_manager.runPhase'  : self.route_args('run_phase', 'run.phase') },
            {'app.workload_manager.updateClusterStatus'  : self.route_args('cluster_status', 'cluster.status') },
        )

        for type_ in types:
            if type_ == "kv" or type_ == "all":
                self.add_kvconfig()
                self.add_kv_ops_manager()
                self.add_report_kv_latency()
            if type_ == "query" or type_ == "all":
                self.add_queryconfig()
            if type_ == "admin" or type_ == "all":
                self.add_adminconfig()

            self.CELERYBEAT_SCHEDULE.update(
            {
                'update_cluster_status': {
                    'task': 'app.workload_manager.updateClusterStatus',
                    'schedule': timedelta(seconds=10),
                },
            })

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

        queue = "%s_%s" % (queue, self.CB_CLUSTER_TAG)
        routing_key = None
        if key is None:
            routing_key = queue
        else:
            routing_key = "%s.%s" % (self.CB_CLUSTER_TAG, key)
        return queue, routing_key

    def add_kvconfig(self):

        direct_ex = Exchange("kv_direct", type="direct", auto_delete = True, durable = True)

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
            self.make_queue('kv_consumer',     'kv.consumer', direct_ex),
            self.make_queue('kv_scheduler',     'kv.scheduler', direct_ex),
            self.make_queue('kv_postcondition',     'kv.postcondition', direct_ex),
            self.make_queue('kv_prerun',     'kv.prerun', direct_ex),
            self.make_queue('kv_postrun',     'kv.postrun', direct_ex),
            self.make_queue('kv_queueops',     'kv.queueops', direct_ex),
            self.make_queue('kv_task_gen',     'kv.taskgen', direct_ex),
            self.make_queue('kv_systestrunner',     'kv.systestrunner', direct_ex),
        )


        self.CELERY_ROUTES = self.CELERY_ROUTES +\
        (
            {'app.sdk_client_tasks.mdelete': self.route_args('delete', 'kv.delete') },
            {'app.sdk_client_tasks.mset'   : self.route_args('set', 'kv.set') },
            {'app.sdk_client_tasks.mget'  : self.route_args('get', 'kv.get') },
            {'app.workload_manager.workloadConsumer' : self.route_args('kv_consumer', 'kv.consumer') },
            {'app.workload_manager.taskScheduler' : self.route_args('kv_scheduler', 'kv.scheduler') },
            {'app.workload_manager.postcondition_handler' : self.route_args('kv_postcondition', 'kv.postcondition') },
            {'app.workload_manager.task_prerun_handler' : self.route_args('kv_prerun', 'kv.prerun') },
            {'app.workload_manager.postrun' : self.route_args('kv_postrun', 'kv.postrun') },
            {'app.workload_manager.queue_op_cycles' : self.route_args('kv_queueops', 'kv.queueops') },
            {'app.workload_manager.generate_pending_tasks' : self.route_args('kv_task_gen', 'kv.taskgen') },
            {'app.workload_manager.sysTestRunner' : self.route_args('kv_systestrunner', 'kv.systestrunner') },
        )


    def add_queryconfig(self):

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
                'args' : (10,) # no. of msgs to trigger throttling
            },
            'query_ops_manager': {
                'task': 'app.query.query_ops_manager',
                'schedule': timedelta(seconds=10), # every 10s
                'args' : (10,) # no. of msgs to trigger throttling
            },
        })

        self.CELERY_QUEUES = self.CELERY_QUEUES +\
            (
                # schedulable queue for the consumer task
                self.make_queue('query_consumer',  'query.consumer', direct_ex),

                # high performance direct exhcnage for multi_query tasks
                self.make_queue('query_multi',  'query.multi', direct_ex),

                # dedicated queues
                self.make_queue('query_runner',  'query.runner', direct_ex),
                self.make_queue('query_upt_builder',  'query.updateqb', direct_ex),
                self.make_queue('query_upt_workload',  'query.updateqw', direct_ex),
                self.make_queue('query_ops_manager',  'query.opmanager', direct_ex),
            )

        self.CELERY_ROUTES = self.CELERY_ROUTES +\
        (   # route schedulable tasks both to same interal task queue
            {'app.query.queryConsumer': self.route_args('query_consumer', 'query.consumer')},
            {'app.query.queryRunner': self.route_args('query_runner', 'query.runner')},
            {'app.query.updateQueryBuilders': self.route_args('query_upt_builder', 'query.updateqb')},
            {'app.query.updateQueryWorkload': self.route_args('query_upt_workload', 'query.updateqw')},
            {'app.rest_client_tasks.multi_query': self.route_args('query_multi', 'query.multi')},
            {'app.query.query_ops_manager': self.route_args('query_ops_manager', 'query.opmanager')},
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
                self.route_args('admin_tasks', 'admin_tasks.adminconsumer')},
            {'app.admin_manager.xdcrConsumer':
                self.route_args('admin_tasks', 'admin_tasks.xdcrconsumer')},
            {'app.admin_manager.backup_task':
                self.route_args('admin_tasks', 'admin_tasks.backuptasks')},
            {'app.rest_client_tasks.perform_admin_tasks':
                self.route_args('admin_tasks', 'admin_tasks.performadmin')},
            {'app.rest_client_tasks.perform_xdcr_tasks':
                self.route_args('admin_tasks', 'admin_tasks.performxdcr')},
        )


    def add_kv_ops_manager(self):
        direct_ex = Exchange("kv_ops_direct", type="direct", auto_delete = True, durable = True)

        self.CELERYBEAT_SCHEDULE.update(
        {
            'kv_ops_manager': {
                'task': 'app.workload_manager.kv_ops_manager',
                'schedule': timedelta(seconds=10), # every 10s
                'args' : (1000,) # no. of msgs to trigger throttling
            },
        })

        self.CELERY_QUEUES = self.CELERY_QUEUES +\
            (
                # schedulable queue for multiple tasks
                self.make_queue('kv_ops_mgr',  'kv.ops_mgr', direct_ex),
            )

        self.CELERY_ROUTES = self.CELERY_ROUTES +\
        (
            # route schedulable tasks both to same interal task queue
            {'app.workload_manager.kv_ops_manager':
                self.route_args('kv_ops_mgr', 'kv.ops_mgr')},
        )

    def add_report_kv_latency(self):
        direct_ex = Exchange("kv_ops_direct", type="direct", auto_delete = True, durable = True)

        self.CELERYBEAT_SCHEDULE.update(
        {
            'report_kv_latency': {
                'task': 'app.workload_manager.report_kv_latency',
                'schedule': timedelta(seconds=10), # every 10s
            },
        })

        self.CELERY_QUEUES = self.CELERY_QUEUES +\
            (
                self.make_queue('kv_mng_latency',  'kv.mnglatency', direct_ex),
                self.make_queue('kv_sdk_latency',  'kv.sdklatency', direct_ex),
                self.make_queue('kv_mc_latency',  'kv.mclatency', direct_ex),
            )

        self.CELERY_ROUTES = self.CELERY_ROUTES +\
        (
            {'app.workload_manager.report_kv_latency':
                self.route_args('kv_mng_latency', 'kv.mnglatency')},
            {'app.sdk_client_tasks.mc_op_latency':
                self.route_args('kv_mc_latency', 'kv.mclatency')},
            {'app.sdk_client_tasks.sdk_op_latency':
                self.route_args('kv_sdk_latency', 'kv.sdklatency')},
        )

