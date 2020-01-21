from librabbitmq import Connection, Message
from pyrabbit.api import Client
import json
import pickle
from celery import Task
import testcfg as cfg
from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

class PersistedMQ(Task):
    _conn = None

    @property
    def rabbitHelper(self):
        if self._conn is None:
            self._conn = RabbitHelper()
        return self._conn

    def close(self):
        del self._conn
        self._conn = None

class RabbitHelper(object):
    def __init__(self, mq_server = None, virtual_host = cfg.CB_CLUSTER_TAG):

        if mq_server == None:
            mq_server = cfg.RABBITMQ_IP

        self.connection = Connection(host= mq_server, userid="guest", password="guest", virtual_host = virtual_host)
        self.manager = Client(mq_server+":55672", "guest", "guest")


    def declare(self, queue = None, durable = True):
        res = None
        channel = self.connection.channel()
        if queue:
            if not isinstance(queue, str): queue = str(queue)
            res = channel.queue_declare(queue = queue, durable = durable, auto_delete = True)
        else:
            # tmp queue
            res = channel.queue_declare(exclusive = True)

        channel.close()
        return res


    def exchange_declare(self, exchange, type_='direct'):
        channel = self.connection.channel()
        channel.exchange_declare(exchange = exchange,
                                type=type_)
        channel.close()

    def bind(self, exchange, queue):
        channel = self.connection.channel()
        channel.queue_bind(exchange = exchange, queue = queue)
        channel.close()

    def delete(self, queue):
        channel = self.connection.channel()
        if not isinstance(queue, str): queue = str(queue)
        channel.queue_delete(queue=queue)
        channel.close()

    def purge(self, queue):
        channel = self.connection.channel()
        if not isinstance(queue, str): queue = str(queue)
        channel.queue_purge(queue=queue)
        channel.close()

    def channel(self):
        return  self.connection.channel(), self.connection


    def qsize(self, queue):
        size = 0
        if queue != None:

            if not isinstance(queue, str): queue = str(queue)

            response = self.declare(queue = queue)
            size = response[1]

        return size

    def broadcastMsg(self, routing_key, body):
        channel = self.connection.channel()
        rc = channel.basic_publish(exchange = '', routing_key = routing_key,  body = body)
        channel.close()

    def getExchange(self, vhost, exchange):
        return self.manager.get_exchange(vhost, exchange)

    def numExchangeQueues(self, vhost, exchange):

        try:
          ex = self.getExchange(vhost, exchange)
          return len(ex['outgoing'])
        except Exception:
          return 1 # todo: sometimes the broker doesn't return expected response


    def putMsg(self, routing_key, body, exchange = ''):

        channel = self.connection.channel()
        if not isinstance(routing_key, str): routing_key= str(routing_key)

        rc = channel.basic_publish(exchange = exchange,
                                   routing_key = routing_key,
                                   body = body)
        channel.close()


    def getMsg(self, queue, no_ack = False, requeue = False):

        channel = self.connection.channel()
        message = channel.basic_get(queue = queue)
        body = None

        if message is not None:
            body = message.body
            # Handle data receipt acknowldegement
            if no_ack == False:
               message.ack()

            if requeue:
                self.putMsg(queue, body)

        channel.close()
        return body

    def getJsonMsg(self, queue, no_ack = False, requeue = False):

        msg = self.getMsg(queue, no_ack, requeue)
        body = {}
        if msg is not None:
            try:
                body = json.loads(msg)
            except ValueError:
                pass

        return body

    def close(self):
        self.connection.close()

    def __del__(self):
        self.close()


"""
" rawTaskPublisher
"
" This method assembles a celery tasks and sends it to the requested broker.
" It can be useful for special cases (i.e cli or xdcr) where sender wants to
" trigger a task that is listening for messages from outside to perform work
"
" task: full task string method. this must be a string with the name of a
"       defined method with @celery.task annotations. i.e (app.systest_manager.getWorkloadStatus)
" args: tuple of task args  ('abc',)
" server: broker to run task against
" vhost: vhost in broker where task should be routed to
" userid: userid to access broker
" password: password to access broker
" exchange: name of exchange in broker to send message
" routing_key: routing key that will put message in appropriate queue.  see app.config for a map
"              of queue's to routing keys
"
" example:
"    task = "app.systest_manager.getWorkloadStatus"
"    args = ('abc',)
"    rabbit_helper.rawTaskPublisher(task, args, 'kv_workload_status_default',
"                                   exchange="kv_direct",
"                                   routing_key="default.kv.workloadstatus")
"
"""
def rawTaskPublisher(task, args, queue,
                    broker = cfg.RABBITMQ_IP,
                    vhost = cfg.CB_CLUSTER_TAG,
                    exchange = "",
                    userid="guest",
                    password="guest",
                    routing_key = None):

    # setup broker connection
    connection = Connection(host = broker, userid=userid, password=password, virtual_host = vhost)
    channel = connection.channel()

    # construct task body
    body={'retries': 0, 'task': task, 'errbacks': None, 'callbacks': None, 'kwargs': {},
         'eta': None, 'args': args, 'id': 'e7cb7ff5-acd3-4060-8f7e-2ef85f810fe5',
         'expires': None, 'utc': True}
    header = {'exclusive': False, 'name': queue, 'headers': {}, 'durable': True, 'delivery_mode': 2,
              'no_ack': False, 'priority': None, 'alias': None, 'queue_arguments': None,
              'content_encoding': 'binary', 'content_type': 'application/x-python-serialize',
              'binding_arguments': None, 'auto_delete': True}

    # prepare message
    body = pickle.dumps(body)
    message = (body, header)

    if routing_key is None:
        routing_key = queue

    # publish!
    rc = channel.basic_publish(message, exchange = exchange,
                               routing_key = routing_key)
