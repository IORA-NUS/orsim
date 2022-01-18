
from __future__ import absolute_import
from celery import Celery


from .celery_config import CeleryConfig

app = Celery('OpenRoad_Agents',
             config_from_object=CeleryConfig)

if __name__ == "__main__":
    app.start()

