from librabbitmq import Connection, Message
import json
import re
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


class RabbitHelper(object):
    def __init__(self, mq_server = cfg.RABBITMQ_IP):
	self.connection = Connection(host= mq_server, userid="guest", password="guest", virtual_host="/")
        self.channel = self.connection.channel()
        self.channel_writer = self.connection.channel()
        self.declare("workload")
        self.declare("workload_template")

    def declare(self, queue, durable = False):
        if not isinstance(queue,str): queue = str(queue)
        return self.channel.queue_declare(queue = queue, durable = durable)
       
    def purge(self, queue):
        if not isinstance(queue,str): queue = str(queue)
        self.channel.queue_delete(queue=queue)

    def consume(self, callback, queue, no_ack = True):
        self.channel.basic_consume(callback, queue = queue, no_ack = no_ack)
        self.channel.start_consuming()

    def qsize(self, queue):
        size = 0
        if queue != None:

            if not isinstance(queue,str): queue = str(queue)

            response = self.declare(queue = queue)
            size = response[1]

        return size

    def putMsg(self, queue, body):
        if not isinstance(queue,str): queue = str(queue)

        rc = self.channel_writer.basic_publish(exchange = '', routing_key=queue,  body = body)

    def getMsg(self, queue, no_ack = False, requeue = False):

        message = self.channel.basic_get(queue = queue)
        body = message.body

        # Handle data receipt acknowldegement 
        if no_ack == False:
           message.ack

        if requeue:
            self.putMsg(queue, body)


        #TODO: try json loads here	
        return body

    def close(self):
        self.connection.close()


