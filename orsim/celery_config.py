
class CeleryConfig:

    # List of modules to import when the Celery worker starts.
    imports = ('apps.tasks',)

    ## Broker settings.
    broker_url = 'amqp://'
    ## Disable result backent and also ignore results.
    task_ignore_result = True

