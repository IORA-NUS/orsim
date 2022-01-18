
from __future__ import absolute_import
from celery import Celery
# from .config import settings
# # import logging

# app = Celery('apps',
#              broker=f'amqp://{settings["RABBITMQ_ADMIN_USER"]}:{settings["RABBITMQ_ADMIN_PASSWORD"]}@{settings["MQTT_BROKER"]}',
#              backend='rpc://',
#              include=['apps.tasks'])

from orsim.core import ORSimEnv

broker_url = f'amqp://{ORSimEnv.messenger_backend["RABBITMQ_ADMIN_USER"]}:{ORSimEnv.messenger_backend["RABBITMQ_ADMIN_PASSWORD"]}@{ORSimEnv.messenger_backend["MQTT_BROKER"]}'

## Disable result backent and also ignore results.
task_ignore_result = True

# List of modules to import when the Celery worker starts.
imports = ('orsim.tasks',)

app = Celery('OpenRoad_RideHail_Agents',
            broker=broker_url,
            task_ignore_result=True,
            include=imports)


if __name__ == "__main__":
    app.start()

