
import json

# from numpy import isin
# from orsim.config import settings
import requests
# import urllib3
# from urllib.parse import quote
import logging

import paho.mqtt.client as paho


class Messenger:
    """
    Messenger class for managing MQTT and RabbitMQ-based messaging.

    This class handles connecting to an MQTT broker, subscribing to channels, and registering users with RabbitMQ management API.
    It supports both single and multiple channel subscriptions, and allows for custom message handling via a callback.

    Attributes:
        settings (dict): Configuration settings for MQTT and RabbitMQ servers.
        credentials (dict): User credentials containing 'email' and 'password'.
        channel_id (str or list, optional): Channel(s) to subscribe to.
        client: MQTT client instance.

    Args:
        settings (dict): Configuration settings for the messaging system.
        credentials (dict): User credentials for authentication.
        channel_id (str or list, optional): Channel(s) to subscribe to.
        on_message (callable, optional): Callback function for incoming messages.
        transport (optional): Custom transport for MQTT client.

    Methods:
        disconnect():
            Disconnects the MQTT client and stops its loop.

        register_user(username, password):
            Registers a user with RabbitMQ management API and sets permissions.
    """

    def __init__(self, settings, credentials, channel_id=None, on_message=None, transport=None):
        """
        Initializes the Messenger instance with the provided settings, credentials, and optional parameters.

        Args:
            settings (dict): Configuration settings, including MQTT broker information.
            credentials (dict): User credentials containing 'email' and 'password'.
            channel_id (str or list, optional): Channel(s) to subscribe to. Can be a single channel (str) or multiple channels (list of str).
            on_message (callable, optional): Callback function to handle incoming messages.
            transport (optional): Custom transport client. If None, a default MQTT client is created.

        Notes:
            - Registers the user and connects to the MQTT broker if no custom transport is provided.
            - Subscribes to the specified channel(s) and starts the MQTT client loop.
            - Designed for inter-agent communication using RabbitMQ PubSub queues.
        """

        self.settings = settings
        self.credentials = credentials
        self.channel_id = channel_id

        if transport is None:
            self.client = paho.Client(credentials['email'],  clean_session=True)
            self.client.username_pw_set(username=self.credentials['email'], password=self.credentials['password'])
            # Messenger.register_user(self.credentials['email'], self.credentials['password'])
            self.register_user(self.credentials['email'], self.credentials['password'])

            self.client.connect(self.settings['MQTT_BROKER'])

        if on_message is not None:
            self.client.on_message = on_message


        # RabbitMQ PubSub queue is used for processing requests in sequence
        # This is a deliberate design choice to enable:
        #   - Inter-Agent communication as core part of system design

        # if channel_id is not None:
        if isinstance(channel_id, str):
            # self.client.loop_start()
            self.client.subscribe(channel_id, qos=0)
            logging.debug(f"Channel: {channel_id}")
        elif isinstance(channel_id, list):
            # self.client.loop_start()
            self.client.subscribe([(cid, 0) for cid in channel_id])
            logging.debug(f"Channel: {channel_id}")

        self.client.loop_start()



    def disconnect(self):
        """
        Disconnects the client from the messaging service.

        Stops the client's event loop if a channel is subscribed, and then disconnects the client.
        Exceptions during loop stopping or disconnection are logged.

        Raises:
            Logs any exceptions encountered during loop stopping or disconnection.
        """

        # try:
        #     self.client.unsubscribe(self.channel_id)
        # except Exception as e:
        #     logging.exception(str(e))

        if self.channel_id is not None:
            try:
                self.client.loop_stop(force=True)
            except Exception as e:
                logging.exception(str(e))

        try:
            self.client.disconnect()
        except Exception as e:
            logging.exception(str(e))




    # @classmethod
    def register_user(self, username, password):
        """
        Registers a new user in RabbitMQ and sets appropriate permissions.

        This method checks if the user already exists in the RabbitMQ management server.
        If the user does not exist, it creates the user with the specified password.
        Regardless of existence, it sets permissions and topic permissions for the user on the default vhost.

        Args:
            username (str): The username to register.
            password (str): The password for the new user.

        Raises:
            Exception: If there is an error during user creation or permission assignment.

        Note:
            Requires RabbitMQ management API credentials and server URL in self.settings.
        """


        response = requests.get(f"{self.settings['RABBITMQ_MANAGEMENT_SERVER']}/users/{username}")
        if (response.status_code >= 200) and (response.status_code <= 299):
            logging.warning('User is already registered')
        else:
            try:
                response = requests.put(f"{self.settings['RABBITMQ_MANAGEMENT_SERVER']}/users/{username}",
                                        data=json.dumps({'password': password, 'tags': ''}),
                                        headers={"content-type": "application/json"},
                                        auth=(self.settings['RABBITMQ_ADMIN_USER'], self.settings['RABBITMQ_ADMIN_PASSWORD'])
                                    )
            except Exception as e:
                logging.exception(str(e))
                raise e

        # reset the user and set appropriate permissions as needed
        quoted_slash = '%2F'
        response = requests.put(f"{self.settings['RABBITMQ_MANAGEMENT_SERVER']}/permissions/{quoted_slash}/{username}",
                        data=json.dumps({"username":username, "vhost":"/", "configure":".*", "write":".*", "read":".*"}),
                        headers={"content-type": "application/json"},
                        auth=(self.settings['RABBITMQ_ADMIN_USER'], self.settings['RABBITMQ_ADMIN_PASSWORD'])
                    )

        response = requests.put(f"{self.settings['RABBITMQ_MANAGEMENT_SERVER']}/topic-permissions/{quoted_slash}/{username}",
                        data=json.dumps({username: username, "vhost": "/", "exchange": "", "write": ".*", "read": ".*"}),
                        headers={"content-type": "application/json"},
                        auth=(self.settings['RABBITMQ_ADMIN_USER'], self.settings['RABBITMQ_ADMIN_PASSWORD'])
                    )

