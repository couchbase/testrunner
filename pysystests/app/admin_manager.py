from __future__ import absolute_import
from app.celery import celery
import testcfg as cfg
from rabbit_helper import PersistedMQ

from app.rest_client_tasks import perform_admin_tasks, create_ssh_conn

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
            perform_admin_tasks.apply_async(args=[adminMsg])

    except Exception as ex:
        logger.error(ex)

@celery.task
def backup_task(do_backups=False):
    """ Run backup once a day"""
    # Invoke backup from the staging node
    if do_backups:
        shell, node = create_ssh_conn(server_ip = cfg.BACKUP_NODE_IP, username=cfg.BACKUP_NODE_SSH_USER,
                                  password=cfg.BACKUP_NODE_SSH_PWD)
        logger.error("Running backup")
        #TODO: Support per bucket backup, by default does all
        #TODO: Play with num threads
        command_options = ['-t %s' % 4]
        shell.execute_cluster_backup(backup_location=cfg.BACKUP_DIR, command_options=command_options,
                                     cluster_ip=cfg.COUCHBASE_IP, cluster_port=cfg.COUCHBASE_PORT)
