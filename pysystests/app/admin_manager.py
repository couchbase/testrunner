
from app.celery import celery
import testcfg as cfg
from rabbit_helper import PersistedMQ
import string
from app.rest_client_tasks import perform_admin_tasks, perform_xdcr_tasks, create_ssh_conn

from celery.utils.log import get_task_logger
logger = get_task_logger(__name__)

@celery.task(base = PersistedMQ)
def adminConsumer(adminQueue = "admin_default"):

    rabbitHelper = adminConsumer.rabbitHelper

    try:
        adminQueueSize = rabbitHelper.qsize(adminQueue)
        if adminQueueSize > 0:
            adminMsg = rabbitHelper.getJsonMsg(adminQueue)
            perform_admin_tasks.apply_async(args=[adminMsg])

            if 'rcq' in adminMsg:
                # filter out unspecified ops in return value
                msg = [(k, str(adminMsg[k])) for k in list(adminMsg.keys()) if str(adminMsg[k]) is not '' and\
                    string.find(str(adminMsg[k]), 'rc_') < 0]
                rabbitHelper.putMsg(adminMsg['rcq'], "Started admin task: %s" % msg)

    except Exception as ex:
        logger.error(ex)

@celery.task(base = PersistedMQ)
def xdcrConsumer(xdcrQueue = "xdcr_default"):

    rabbitHelper = xdcrConsumer.rabbitHelper

    try:
        xdcrQueueSize = rabbitHelper.qsize(xdcrQueue)
        if xdcrQueueSize > 0:
            xdcrMsg = rabbitHelper.getJsonMsg(xdcrQueue)
            logger.error(xdcrMsg)
            perform_xdcr_tasks.apply_async(args=[xdcrMsg])
            if 'rcq' in xdcrMsg:
                rabbitHelper.putMsg(xdcrMsg['rcq'], "Started xdcr task: %s" % xdcrMsg)

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
