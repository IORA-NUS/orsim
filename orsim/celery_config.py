
class CeleryConfig:
    """
    Celery configuration class for setting up worker behavior and broker connection.

    Attributes:
        imports (tuple): List of modules to import when the Celery worker starts.
        broker_url (str): URL for the message broker (default is AMQP).
        task_ignore_result (bool): If True, disables the result backend and ignores task results.
        broker_connection_retry_on_startup (bool): If True, suppresses broker connection retry warnings on startup.
    """

    # List of modules to import when the Celery worker starts.
    imports = ('orsim.tasks',)

    ## Broker settings.
    broker_url = 'amqp://'
    ## Disable result backend and also ignore results.
    task_ignore_result = True

    ## to suppress broker_connection_retry_on_startup warmning
    broker_connection_retry_on_startup = True
