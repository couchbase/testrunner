from librabbitmq import Connection, Message
import json
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


    def declare(self, queue, durable = True):
        channel = self.connection.channel()
        if not isinstance(queue,str): queue = str(queue)
        res = channel.queue_declare(queue = queue, durable = durable, auto_delete = True)
        channel.close()
        return res

    def delete(self, queue):
        channel = self.connection.channel()
        if not isinstance(queue,str): queue = str(queue)
        channel.queue_delete(queue=queue)
        channel.close()

    def purge(self, queue):
        channel = self.connection.channel()
        if not isinstance(queue,str): queue = str(queue)
        channel.queue_purge(queue=queue)
        channel.close()

    def consume(self, callback, queue, no_ack = True):
        channel = self.connection.channel()
        channel.basic_consume(callback, queue = queue, no_ack = no_ack)
        channel.start_consuming()
        channel.close()


    def qsize(self, queue):
        size = 0
        if queue != None:

            if not isinstance(queue,str): queue = str(queue)

            response = self.declare(queue = queue)
            size = response[1]

        return size

    def putMsg(self, queue, body):
        channel = self.connection.channel()
        if not isinstance(queue, str): queue = str(queue)
        rc = channel.basic_publish(exchange = '', routing_key=queue,  body = body)
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

