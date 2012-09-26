from __future__ import absolute_import
from app.celery import celery

from rabbit_helper import PersistedMQ

from app.rest_client_tasks import perform_admin_tasks

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

@celery.task(base = PersistedMQ)
def adminConsumer(adminQueue = "admin_tasks"):

    rabbitHelper = adminConsumer.rabbitHelper
    try:
        adminQueueSize = rabbitHelper.qsize(adminQueue)
        if adminQueueSize > 0:
            adminMsg = rabbitHelper.getJsonMsg(adminQueue)
            logger.error(adminMsg)
            perform_admin_tasks(adminMsg)

    except Exception as ex:
        logger.error(ex)


