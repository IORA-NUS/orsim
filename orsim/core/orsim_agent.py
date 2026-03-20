from abc import ABC, abstractclassmethod, abstractmethod
import asyncio, json, logging, time, os, traceback
from collections import OrderedDict
# NOTE IMPORTANT: Do not import eventlet at top level due to an issue of monkey patching and interaction with other libraries. Import within the function as needed.

from datetime import datetime
from dateutil.relativedelta import relativedelta
from orsim.messenger import Messenger

from orsim.core.orsim_env import ORSimEnv
from orsim.utils import time_to_str, str_to_time

from cerberus import Validator

class ORSimAgent(ABC):
    """
    Abstract base class for ORSim agents in the OpenRoad Simulation framework.

    This class manages agent lifecycle, message handling, time-stepping, and communication with the simulation scheduler.
    Agents are expected to implement domain-specific logic by subclassing ORSimAgent and providing concrete implementations
    for the abstract methods: `process_payload`, `estimate_next_event_time`, and `logout`.

    Attributes:
        messenger (Messenger): Handles communication with the simulation environment.
        agent_failed (bool): Indicates if the agent initialization failed.
        payload_cache (dict): Stores the most recent message payload.
        step_log (dict): Log of step actions and messages.
        unique_id (str): Unique identifier for the agent.
        run_id (str): Identifier for the current simulation run.
        scheduler_id (str): Identifier for the scheduler managing this agent.
        reference_time (datetime): Start time reference for simulation steps.
        current_time (datetime): Current simulation time for the agent.
        next_event_time (datetime): Estimated time for the next agent event.
        prev_time_step (int): Previous simulation time step.
        current_time_step (int): Current simulation time step.
        elapsed_duration_steps (int): Steps elapsed since previous time step.
        active (bool): Indicates if the agent is active.
        _shutdown (bool): Indicates if the agent is shutting down.
        behavior (dict): Behavior configuration for the agent.
        agent_credentials (dict): Credentials used for agent authentication.
        start_time (float): Timestamp when message processing started.
        end_time (float): Timestamp when message processing ended.
        message_processing_active (bool): Indicates if the agent is processing a message.
        message_handlers (dict): Mapping of message topics to handler methods.
        orsim_settings (dict): Simulation settings validated for the agent.

    Methods:
        register_message_handler(topic, method): Registers a handler for a specific message topic.
        is_active(): Returns whether the agent is currently active.
        reset_step_log(): Clears the step log.
        add_step_log(message): Adds a message to the step log.
        take_first_step(dummy_payload): Begins agent processing with an initial payload.
        handle_orsim_agent_message(payload): Handles incoming messages for the agent.
        on_receive_message(client, userdata, message): Callback for receiving messages from the messenger.
        start_listening(): Begins listening for incoming messages and starts heartbeat monitoring.
        stop_listening(): Disconnects the agent from the messenger.
        get_current_time_str(): Returns the current simulation time as a string.
        bootstrap_step(time_step): Initializes agent state for a given simulation step.
        shutdown(): Shuts down the agent and performs logout.
        handle_heartbeat_failure(): Shuts down the agent if message processing exceeds timeout.
        get_transition_probability(condition, default): Retrieves transition probability for a given condition.

    Abstract Methods:
        process_payload(payload): Processes a received payload. Must be implemented by subclasses.
        estimate_next_event_time(): Estimates the next event time for the agent. Must be implemented by subclasses.
        logout(): Handles agent logout and cleanup. Must be implemented by subclasses.
    """

    messenger = None
    agent_failed = False
    payload_cache = None
    step_log = {}

    # def __init__(self, unique_id, run_id, reference_time, init_time_step, scheduler_id, behavior, orsim_settings):
    def __init__(self, unique_id, run_id, reference_time, init_time_step, scheduler, behavior): #, orsim_settings):
        """
        Initialize an ORSimAgent instance.

        Args:
            unique_id (str): Unique identifier for the agent.
            run_id (str): Identifier for the simulation run.
            reference_time (str): Reference time in the format '%Y%m%d%H%M%S'.
            init_time_step (int): Initial time step for the agent.
            scheduler (dict): Scheduler configuration containing 'id' and 'orsim_settings'.
            behavior (Any): Behavior configuration for the agent.

        Attributes:
            unique_id (str): Unique identifier for the agent.
            run_id (str): Identifier for the simulation run.
            scheduler_id (str): Identifier for the scheduler.
            reference_time (datetime): Reference time as a datetime object.
            current_time (datetime): Current time of the agent.
            next_event_time (datetime): Time for the next event.
            orsim_settings (dict): Validated ORSim environment settings.
            prev_time_step (int): Previous time step.
            current_time_step (int): Current time step.
            elapsed_duration_steps (int): Number of elapsed duration steps.
            active (bool): Indicates if the agent is active.
            _shutdown (bool): Indicates if the agent is shutting down.
            behavior (Any): Behavior configuration.
            agent_credentials (dict): Credentials for the agent.
            messenger (Messenger): Messenger instance for communication.
            start_time (float): Start time of the agent.
            message_processing_active (bool): Indicates if message processing is active.
            message_handlers (dict): Registered message handlers.

        Methods:
            bootstrap_step(init_time_step): Initializes the agent's state.
            register_message_handler(topic, method): Registers a message handler for a topic.
            handle_orsim_agent_message: Handles messages for the ORSimAgent.
        """
        self.unique_id = unique_id
        self.run_id = run_id
        self.scheduler_id = scheduler['id']
        self.reference_time = datetime.strptime(reference_time, '%Y%m%d%H%M%S') # datetime
        self.current_time = self.reference_time
        self.next_event_time = self.reference_time # To be set by agent at every step_response
        # self.orsim_settings = orsim_settings
        self.orsim_settings = ORSimEnv.validate_orsim_settings(scheduler['orsim_settings'])


        # Ideally behavior should be read from a datafile/db or in case of simulation, generated by the Model and passed in as attribute
        self.prev_time_step = 0
        self.current_time_step = 0
        self.elapsed_duration_steps = 0
        self._active = False

        self._shutdown = False
        self.behavior = behavior

        self.agent_credentials = {
            'email': f"{self.run_id}_{self.scheduler_id}_{unique_id}",
            'password': "secret_password",
        }

        self.messenger = Messenger(ORSimEnv.messenger_settings, self.agent_credentials)

        self.start_time = time.time()
        self.message_processing_active = False

        self.message_handlers = {}
        self.bootstrap_step(init_time_step)

        self.register_message_handler(topic=f"{self.run_id}/{self.scheduler_id}/ORSimAgent",
                                 method=self.handle_orsim_agent_message)

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, value):
        self._active = value

    def register_message_handler(self, topic, method):
        """
        Registers a message handler method for a specific topic.

        Args:
            topic (str): The topic to associate with the handler.
            method (callable): The handler method to be called when a message for the topic is received.

        """
        self.message_handlers[topic] = method

    def is_active(self):
        """
        Check if the agent is currently active.

        Returns:
            bool: True if the agent is active, False otherwise.
        """
        return self.active

    def reset_step_log(self):
        """
        Resets the step log by initializing it as an empty dictionary.

        This method is typically called at the beginning of a new simulation step
        to clear any previously stored log data.
        """
        self.step_log = {}

    def add_step_log(self, message):
        """
        Adds a log entry for the current step with a timestamp.

        Args:
            message (str): The log message to record for the current step.

        Side Effects:
            Updates the `step_log` dictionary with the current timestamp as the key and the provided message as the value.
        """
        self.step_log[datetime.now().isoformat()] = message

    def take_first_step(self, dummy_payload):
        """
        Activates message processing and handles the initial agent message.

        Args:
            dummy_payload: The initial payload to be processed by the agent.
        """
        self.message_processing_active = True
        self.handle_orsim_agent_message(dummy_payload)

    def handle_orsim_agent_message(self, payload):
        """
        Handles incoming messages for the ORSim agent, processes actions based on the payload,
        and publishes a response to the scheduler.

        Supported actions:
            - 'init': Initializes the agent for the given time step (currently unused).
            - 'step': Processes a simulation step and updates the agent's state.
            - 'shutdown': Prepares the agent for shutdown.

        In case of errors during processing, publishes an error response with traceback details.

        Args:
            payload (dict): The message payload containing at least 'action' and 'time_step' keys.

        Publishes:
            A JSON response to the scheduler topic with agent status and runtime information.
        """
        # print('Inside handle_orsim_agent_message')
        self.add_step_log('In handle_orsim_agent_message')

        try:
            self.bootstrap_step(payload['time_step'])

            if payload.get('action') == 'init':
                ''' NOTE This is unused block of code at the moment'''
                # print(f"{self.unique_id} received {payload=}")
                did_step = self.process_payload(payload)
                self.next_event_time = self.estimate_next_event_time()

                response_payload = {
                    'agent_id': self.unique_id,
                    'time_step': self.current_time_step,
                    'action': 'ready', # 'completed',
                    'did_step': did_step,
                    'run_time': time.time() - self.start_time,
                }
            elif payload.get('action') == 'step':

                did_step = self.process_payload(payload)
                self.next_event_time = self.estimate_next_event_time()

                response_payload = {
                    'agent_id': self.unique_id,
                    'time_step': self.current_time_step,
                    'action': 'completed' if self._shutdown==False else 'shutdown',
                    'did_step': did_step,
                    'run_time': time.time() - self.start_time,
                }
            elif payload.get('action') == 'shutdown':
                ''' '''
                # self.shutdown()
                response_payload = {
                    'agent_id': self.unique_id,
                    'time_step': self.current_time_step,
                    'action': 'shutdown',
                    'did_step': True,
                    'run_time': time.time() - self.start_time,
                }
        except Exception as e:
            response_payload = {
                'agent_id': self.unique_id,
                'time_step': self.current_time_step,
                'action': 'error',
                'did_step': False,
                'run_time': time.time() - self.start_time,
                'details': traceback.format_exc(), # str(e)
            }
            # logging.exception(f"{self.unique_id} raised {str(e)}")

        self.messenger.client.publish(f'{self.run_id}/{self.scheduler_id}/ORSimScheduler', json.dumps(response_payload))

        if payload.get('action') == 'shutdown':
            self.shutdown()

        self.end_time = time.time()
        self.message_processing_active = False

    def on_receive_message(self, client, userdata, message):
        """
        Callback function invoked when a message is received from the MQTT broker.

        Decodes the incoming message payload, updates internal state, and dispatches
        the payload to the appropriate handler based on the message topic.

        Args:
            client: The MQTT client instance.
            userdata: User-defined data passed to the callback.
            message: The MQTT message object containing topic and payload.

        Side Effects:
            - Updates `start_time`, `message_processing_active`, and `payload_cache`.
            - Resets the step log.
            - Logs the received action and processing runtime.
            - Invokes the corresponding handler method for the message topic if available.
        """

        payload = json.loads(message.payload.decode('utf-8'))
        # print("received message", message.payload.decode('utf-8'))
        self.start_time = time.time()
        self.message_processing_active = True
        self.payload_cache = payload
        self.reset_step_log()

        logging.debug(f"Agent {self.unique_id} received {payload.get('action')}")

        if self.message_handlers.get(message.topic) is not None:
            method = self.message_handlers[message.topic]
            method(payload)

        logging.debug(f"Runtime for {self.unique_id} at {self.current_time_step}: {self.end_time - self.start_time:0.2f} secs ")

    def start_listening(self):
        """
        Starts the agent's message listening process and heartbeat monitoring.

        Subscribes the agent to relevant message topics and sets up the message handler.
        Depending on the concurrency strategy, uses eventlet to spawn a background task
        that periodically checks for heartbeat failures and handles agent shutdown.
        After setup, sends a 'ready' or 'init_error' message based on agent initialization status,
        and triggers the agent's first step.

        Returns:
            None
        """
        start_time_for_ready = time.time() # NOTE Local start time NOT a class variable

        if not self.agent_failed:
            # self.agent_messenger = Messenger(self.agent_credentials, f"{self.run_id}/{self.scheduler_id}/ORSimAgent", self.on_receive_message)
            if self.message_handlers:
                self.messenger.client.subscribe([(topic, 0) for topic, _ in self.message_handlers.items()])
            else:
                logging.warning("No message handlers registered. Subscription skipped.")
            self.messenger.client.on_message = self.on_receive_message

            # print('subscribed to ', self.message_handlers)

            # if settings['CONCURRENCY_STRATEGY'] == 'ASYNCIO':
            #     logging.debug(f'Agent {self.unique_id} is Listening for Messages')
            #     loop = asyncio.get_event_loop()
            #     try:
            #         loop.run_forever()
            #     except KeyboardInterrupt:
            #         pass
            #         loop.close()
            # elif settings['CONCURRENCY_STRATEGY'] == 'EVENTLET':
            import eventlet

            def run_forever():
                heartbeat_interval = self.orsim_settings.get('HEARTBEAT_INTERVAL', 5)  # Default to 5 seconds
                while True:
                    eventlet.sleep(heartbeat_interval)
                    self.handle_heartbeat_failure()
                    # logging.info(f"{self.unique_id} Heartbeat")
                    if self._shutdown == True:
                        self.stop_listening()
                        break

            eventlet.spawn(run_forever)

            # Once agent is setup and listening, send the ready message
            response_payload = {
                'agent_id': self.unique_id,
                'time_step': -1,
                'action': 'ready',
                'run_time': time.time() - start_time_for_ready,
            }
        else:
            response_payload = {
                'agent_id': self.unique_id,
                'time_step': -1,
                'action': 'init_error',
                'run_time': time.time() - start_time_for_ready,
            }

        self.take_first_step({
            'action': 'init',
            'time_step': self.current_time_step
        })

        # self.messenger.client.publish(f'{self.run_id}/{self.scheduler_id}/ORSimScheduler', json.dumps(response_payload))

    def stop_listening(self):
        self.messenger.disconnect()

    def get_current_time_str(self):
        return time_to_str(self.current_time)

    # @classmethod
    # def time_to_str(cls, time_var):
    #     return datetime.strftime(time_var, "%a, %d %b %Y %H:%M:%S GMT")

    # @classmethod
    # def str_to_time(cls, time_str):
    #     return datetime.strptime(time_str, "%a, %d %b %Y %H:%M:%S GMT")

    def bootstrap_step(self, time_step):

        self.prev_time_step = self.current_time_step
        self.current_time_step = time_step
        self.elapsed_duration_steps = self.current_time_step - self.prev_time_step

        self.current_time = self.reference_time + relativedelta(seconds = time_step * self.orsim_settings['STEP_INTERVAL'])

    def shutdown(self):
        if not self._shutdown:
            logging.info(f'Shutting down {self.unique_id = }')
            # self.stop_listening()
            self.logout()
            self.active = False
            self._shutdown = True

    def handle_heartbeat_failure(self):
        if self.message_processing_active:
            now = time.time()
            threshold = self.orsim_settings['STEP_TIMEOUT']
            if (now - self.start_time) > threshold:
                logging.warning(f"Auto Shutdown Agent {self.unique_id}. Exceeded heartbeat threshold {threshold} sec while processing...")
                # logging.warning(f"{self.payload_cache = }")
                logging.warning(f"{json.dumps(self.step_log, indent=2)}")
                # self.stop_listening()
                self.active = False
                self._shutdown = True

    def get_transition_probability(self, condition, default):
        try:
            for rule in self.behavior.get('transition_prob'):
                if rule[0] == condition:
                    return rule[1]
        except: pass

        return default

    @abstractmethod
    def process_payload(self, payload):
        raise NotImplementedError

    @abstractmethod
    def estimate_next_event_time(self):
        raise NotImplementedError

    @abstractmethod
    def logout(self):
        ''' process any logout processes needed in the agent.
        '''
        raise NotImplementedError

