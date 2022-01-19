=====
Usage
=====


ORSim is a fully distributed Agent-Based Simulation platform and uses a variety of techniques to ensure performance at scale. There are 2 core components that make up the Simulator - ORSimScheduler and ORSimAgent

ORSimScheduler
--------------
ORSimScheduler, as the name suggests is a task scheduler and ensures that the various Agents in its purview execute their processes at the appropriate Clock tick. A Simulation Controller that implements ORSim will initiate **One or More** Schedulers and call the step() method at eack clock tick. Agents can be added / removed as the need demands and at each step the ORSimScheduler ensures consistent execution of all the active Agents. The Agents are expected to be independant as the executor distributes the tasks across all available resources and each Agent executes their step in parallel. The interprocess communication between the Agents and Scheduers is handled via a Message Queues and the current implementation relies on `RabbitMQ`_ as the backend.

The task distribution is handled using `Celery`_ which works well with RabbitMQ when using the AMQP protocol.

.. _Celery: https://docs.celeryproject.org/en/stable/index.html
.. _RabbitMQ: https://www.rabbitmq.com/


ORSimAgent
----------
The individual behavior of the agents is implemented by inheriting from the ORSimAgent Class. Agents are added / removed from the Scheduler as the simulation model demands. The only expectation on an ORSimAgent subclass is that it implements the abstract methods and the response at each step takes a finite time. An MQTT based inter-agent communication is enabled by default to ensure that each agent can perform direct communication with another agent (This allows the agents to run independant of each other and the scheduler). Tha Agent can have 2 modes of communication,

* Agent-Scheduler Messaging: This is handled by the ORSim Library and involves receiving the `step` signal from the Scheduler and returning at the end of the step computation.
* Agent-Agent Messaging: The ORSim Library exposes the Messaging backend to the agent to allow for inter-agent communication. This allows flexiility in Simulation model by allowing agents to make decisions independant of the scheduler (or the Simulation controller). The task of identifying the proper channel to communicate on, is left to the agent Implementation.

To keep the tech stack lean, the Agent and scheduler Messaging use the MQTT implementation in RabbitMQ.


Initiating a RabbitMQ Server with MQTT enabled
----------------------------------------------

A compatible RabbitMQ Server can be initiated using docker, with the following Dockerfile
    .. code-block:: docker
        :linenos:

        FROM rabbitmq:3.8.3-alpine

        ENV RABBITMQ_VERSION=3.8.3

        RUN rabbitmq-plugins enable rabbitmq_management
        RUN rabbitmq-plugins enable rabbitmq_mqtt
        RUN rabbitmq-plugins enable rabbitmq_web_mqtt
        RUN rabbitmq-plugins enable rabbitmq_prometheus
        # RUN rabbitmq-plugins enable --offline rabbitmq_auth_backend_oauth2
        # Fix nodename
        RUN echo 'NODENAME=rabbit@localhost' > /etc/rabbitmq/rabbitmq-env.conf


        EXPOSE 15670
        EXPOSE 15672
        EXPOSE 15675
        EXPOSE 15692
        EXPOSE 1883
        EXPOSE 8883

and launched using this shell script
    .. code-block:: shell

        docker run -p 15670:15670 -p 15672:15672 -p 15675:15675 -p 15692:15692 -p 1883:1883 -p 8883:8883 -p 5671:5671 -p 5672:5672 rabbit-mqtt

The RabbitMQ Backend settings should be registered before initiating the `ORSimScheduler`. The messenger_backend should include the following keys (the example below assumes RabbitMQ is running in the local dev enviroment). It is recommended to define this in a config.py file and imported where needed.

    .. code-block:: python
        :linenos:

        messenger_backend = {
            'RABBITMQ_MANAGEMENT_SERVER': "http://localhost:15672/api",
            'RABBITMQ_ADMIN_USER': 'guest',
            'RABBITMQ_ADMIN_PASSWORD': 'guest',

            'MQTT_BROKER': "localhost",
            'WEB_MQTT_PORT': 15675,
        }
