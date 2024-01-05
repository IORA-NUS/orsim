
class CeleryConfig:

    # List of modules to import when the Celery worker starts.
    imports = ('orsim.tasks',)

    ## Broker settings.
    broker_url = 'amqp://'
    ## Disable result backend and also ignore results.
    task_ignore_result = True

    ## to suppress broker_connection_retry_on_startup warmning
    broker_connection_retry_on_startup = True
