from __future__ import absolute_import

from celery import Celery
from app import config

celery = Celery(include=['app.sdk_client_tasks','app.rest_client_tasks','app.workload_manager','app.init'])
celery.config_from_object(config)

