from abc import ABC, abstractclassmethod, abstractmethod
import asyncio, json, logging, time, os, pprint

from datetime import datetime
# from apps import orsim
from orsim.messenger import Messenger

from random import random
from .orsim_env import ORSimEnv

from orsim.tasks import start_agent


class ORSimScheduler(ABC):
    """
    Abstract base class for the ORSimScheduler, responsible for managing agent scheduling and simulation steps in the ORSim environment.

    Attributes:
        orsim_settings (dict): Validated simulation settings from ORSimEnv.
        run_id (str): Unique identifier for the simulation run.
        scheduler_id (str): Unique identifier for the scheduler instance.
        time (int): Current simulation time step.
        init_failure_handler (str): Strategy for handling agent initialization failures ('soft' or other).
        agent_collection (dict): Dictionary holding agent specifications and step responses.
        agent_stat (dict): Dictionary holding statistics for each simulation step.
        agent_credentials (dict): Credentials used for agent messenger communication.
        agent_messenger (Messenger): Messenger instance for agent communication.
        pp (PrettyPrinter): Pretty printer for logging and debugging.

    Methods:
        __init__(run_id, scheduler_id, orsim_settings, init_failure_handler='soft'):
            Initializes the scheduler, validates settings, and sets up messaging.

        add_agent(spec, project_path, agent_class):
            Adds a new agent to the scheduler, launches it asynchronously, and tracks its state.

        remove_agent(unique_id):
            Removes an agent from the scheduler and logs the event.

        on_receive_message(client, userdata, message):
            Handles incoming messages from agents, updating their step responses.

        async confirm_responses():
            Waits for all agents to respond for the current time step, updating statistics.

        async step():
            Advances the simulation by one step, sends step/shutdown messages to agents, and processes responses.

        step_timeout_handler(e):
            Handles timeout errors during agent response confirmation, applying tolerance logic.

    Raises:
        Exception: If ORSimEnv is not properly initialized or agent launch fails beyond allowed tolerance.
    """

    def __init__(self, run_id, scheduler_id, orsim_settings, init_failure_handler='soft'):
        """
        Initialize an ORSimScheduler instance.

        Args:
            run_id (str): Unique identifier for the simulation run.
            scheduler_id (str): Unique identifier for the scheduler.
            orsim_settings (dict): Configuration settings for ORSim.
            init_failure_handler (str, optional): Handler for initialization failure. Defaults to 'soft'.

        Raises:
            Exception: If ORSimEnv.messenger_settings is not initialized.

        Attributes:
            orsim_settings (dict): Validated ORSim settings.
            run_id (str): Simulation run identifier.
            scheduler_id (str): Scheduler identifier.
            time (int): Simulation time, initialized to 0.
            init_failure_handler (str): Initialization failure handler.
            agent_collection (dict): Collection of agents managed by the scheduler.
            agent_stat (dict): Statistics for agents.
            agent_credentials (dict): Credentials for the scheduler agent.
            agent_messenger (Messenger): Messenger instance for agent communication.
            pp (PrettyPrinter): Pretty printer for logging and debugging.
        """

        if ORSimEnv.messenger_settings is None:
            raise Exception("Please Initialize ORSimEnv.set_backend()")

        self.orsim_settings = ORSimEnv.validate_orsim_settings(orsim_settings)

        self.run_id = run_id
        self.scheduler_id = scheduler_id
        self.time = 0

        self.init_failure_handler = init_failure_handler

        self.agent_collection = {}
        self.agent_stat = {}

        self.agent_credentials = {
            'email': f"{self.run_id}_{self.scheduler_id}_ORSimScheduler",
            'password': "secret_password",
        }

        self.agent_messenger = Messenger(ORSimEnv.messenger_settings, self.agent_credentials, f"{self.run_id}/{self.scheduler_id}/ORSimScheduler", self.on_receive_message)

        self.pp = pprint.PrettyPrinter(indent=2)
        logging.info(f'Starting new {scheduler_id= } for {run_id= }')


    # def add_agent(self, unique_id, method, spec):
    def add_agent(self, spec, project_path, agent_class):
        """
        Adds a new agent to the scheduler and initiates its execution as a Celery task.

        Args:
            spec (dict): Specification dictionary for the agent, must include a 'unique_id' key.
            project_path (str): Path to the project directory containing the agent code.
            agent_class (str): Fully qualified class name of the agent (e.g., 'module.submodule.AgentClass').

        Side Effects:
            - Updates `self.agent_collection` with the new agent's specification and initial step response.
            - Starts the agent asynchronously using a Celery task (`start_agent.delay`).
            - Logs and prints information about the agent entering the market.

        Notes:
            - The method prepares agent initialization arguments, including scheduler settings.
            - The agent's launch status and error handling are managed in commented-out code.
        """
        self.agent_collection[spec['unique_id']] = {
            # 'method': method,
            'spec': spec,
            # 'step_response': 'waiting'
            'step_response': {
                self.time: {
                    'reaction': 'waiting',
                    'did_step': False,
                    'run_time': 0,
                }
            }
        }

        # import sys
        # if not project_path in sys.path:
        #     sys.path.append(project_path)
        # print(sys.path)

        module_comp = agent_class.split('.')
        module_name, agent_class_name = str.join('.', module_comp[:-1]), module_comp[-1]

        kwargs = spec.copy()
        # kwargs['scheduler_id'] = self.scheduler_id
        kwargs['scheduler'] = {
            'id': self.scheduler_id,
            'orsim_settings': self.orsim_settings
        }
        # method.delay(**kwargs) # NOTE This starts the Celery Task in a new worker thread
        start_agent.delay(project_path, module_name, agent_class_name, ORSimEnv.messenger_settings, **kwargs) # NOTE This starts the Celery Task in a new worker thread

        logging.info(f"agent {spec['unique_id']} entering market")
        print(f"agent {spec['unique_id']} entering market")

        # launch_start = time.time()
        # while True:
        #     # if self.agent_collection[unique_id]['step_response'] == 'ready':
        #     if self.agent_collection[unique_id]['step_response'][self.time]['reaction'] == 'ready':
        #         logging.info(f'agent {unique_id} is ready')
        #         print(f'agent {unique_id} is ready')
        #         break
        #     elif (self.agent_collection[unique_id]['step_response'][self.time]['reaction'] == 'init_error') or \
        #                         ((time.time() - launch_start) > self.orsim_settings['AGENT_LAUNCH_TIMEOUT']):
        #         logging.exception(f'Failed to Launch agent {unique_id}')
        #         print(f'Failed to Launch agent {unique_id}')
        #         self.remove_agent(unique_id)
        #         if self.init_failure_handler == 'soft':
        #             break
        #         else:
        #             raise Exception(f"Shutdown {self.scheduler_id} due to {self.init_failure_handler=}. Agent {unique_id} failed to launch.")
        #     else:
        #         time.sleep(0.1)


    def remove_agent(self, unique_id):
        """
        Removes an agent from the agent collection using the provided unique ID.

        Args:
            unique_id: The unique identifier of the agent to be removed.

        Logs:
            - An info message indicating the agent has left.
            - Exception details if removal fails.

        Raises:
            Logs and suppresses any exceptions that occur during removal.
        """
        try:
            logging.info(f"agent {unique_id} has left")
            print(f"agent {unique_id} has left")
            self.agent_collection.pop(unique_id)
        except Exception as e:
            logging.exception(str(e))
            # print(e)

    def on_receive_message(self, client, userdata, message):
        """
        Handles incoming MQTT messages for the ORSimScheduler.

        This method processes messages received on the topic specific to the current run and scheduler.
        It decodes the message payload, extracts relevant information, and updates the agent's step response.
        If the message indicates an error or the response time step is invalid, a warning is logged.

        Args:
            client: The MQTT client instance.
            userdata: User-defined data passed to the callback.
            message: The MQTT message object containing topic and payload.

        Side Effects:
            Updates the agent_collection with the step response for the specified agent and time step.
            Logs a warning if an error action is received or the response time step is None.
        """
        if message.topic == f"{self.run_id}/{self.scheduler_id}/ORSimScheduler":
            payload = json.loads(message.payload.decode('utf-8'))

            response_time_step = payload.get('time_step') if payload.get('time_step') != -1 else self.time

            try:
                self.agent_collection[payload.get('agent_id')]['step_response'][response_time_step] = {
                    'reaction': payload.get('action'),
                    'did_step': payload.get('did_step'),
                    'run_time': payload.get('run_time')
                }
            except: pass
            # except Exception as e:
            #     logging.exception(str(e))

            if (payload.get('action') == 'error') or (response_time_step is None):
                logging.warning(f'{self.__class__.__name__} received {message.payload = }')

    async def confirm_responses(self):
        """
        Monitors and confirms the responses from agents in the agent_collection for the current simulation time step.

        Iterates through all agents, categorizes their responses (completed, ready, error, shutdown, waiting), and updates
        agent statistics accordingly. Periodically logs the current status if waiting for agent responses takes longer than 5 seconds.
        Sleeps briefly between checks to avoid busy-waiting.

        Updates:
            - self.agent_stat[self.time]: Dictionary containing counts of agent response types and other statistics.

        Returns:
            None
        """

        start_time = time.time()
        base = 0
        completed = 0
        ready = 0
        shutdown = 0
        error = 0
        waiting = len(self.agent_collection)

        while waiting > 0:
            completed = 0
            ready = 0
            shutdown = 0
            error = 0
            waiting = 0
            num_did_step = 0
            for agent_id, _ in self.agent_collection.items():
                response = self.agent_collection[agent_id]['step_response'][self.time]
                if (response['reaction'] == 'completed'):
                    completed += 1
                elif (response['reaction'] == 'ready'):
                    ready += 1
                elif (response['reaction'] == 'error'):
                    error += 1
                elif (response['reaction'] == 'shutdown'):
                    shutdown += 1
                elif (response['reaction'] == 'waiting'):
                    waiting += 1

                if response['did_step']:
                    num_did_step += 1

            self.agent_stat[self.time] = {
                'completed': completed,
                'ready': ready,
                'error': error,
                'shutdown': shutdown,
                'waiting': waiting,
                'stepping_agents': num_did_step,
                'total_agents': len(self.agent_collection),
                # 'run_time_dist': []
            }
            current_time = time.time()
            if current_time - start_time >= 5:
                # logging.info(f"Waiting for Agent Response... {completed=}, {error=}, {shutdown=}, {waiting=} of {len(self.agent_collection)}: {base + (current_time - start_time):0.0f} sec")
                logging.info(f"Waiting for Agent Response... {self.agent_stat[self.time]}: {base + (current_time - start_time):0.0f} sec")
                base = base + (current_time - start_time)
                start_time = current_time

            await asyncio.sleep(0.1)

    async def step(self):
        """
        Advances the simulation by one step, coordinating agent actions and handling timeouts.

        This asynchronous method performs the following:
        - Logs the current step and scheduler information.
        - Determines whether to send a 'step' or 'shutdown' message to agents based on simulation progress.
        - Initializes agent step responses for the current time step.
        - Publishes the step/shutdown message to all agents.
        - Awaits confirmation responses from agents, handling timeouts if necessary.
        - Removes agents that have shut down or are still waiting after the step.
        - Increments the simulation time.
        - Returns a dictionary indicating the status of the simulation and whether it has ended.

        Returns:
            dict: A dictionary with keys 'status' (str) and 'end_sim' (bool) indicating the result of the step.

        Raises:
            asyncio.TimeoutError: If agent responses are not received within the configured timeout.
        """

        logging.info(f"{self.scheduler_id} Step: {self.time}")
        start_time = time.time()

        if self.time == self.orsim_settings['SIMULATION_LENGTH_IN_STEPS']-1:
            message = {'action': 'shutdown', 'time_step': self.time}
        else:
            message = {'action': 'step', 'time_step': self.time}

        for agent_id, _ in self.agent_collection.items():
            self.agent_collection[agent_id]['step_response'][self.time] = {
                'reaction': 'waiting',
                'did_step': False,
                'run_time': 0
            }

        self.agent_messenger.client.publish(f'{self.run_id}/{self.scheduler_id}/ORSimAgent', json.dumps(message))

        try:
            # start_time = time.time()
            await asyncio.wait_for(self.confirm_responses(), timeout=self.orsim_settings['STEP_TIMEOUT'])
            # end_time = time.time()
            logging.info(f'{self.agent_stat[self.time] = }')
            # logging.info(f'{self.scheduler_id} Runtime: {(time.time()-start_time):0.2f} sec')

        except asyncio.TimeoutError as e:
            logging.exception(f'Scheduler {self.scheduler_id} timeout beyond {self.orsim_settings["STEP_TIMEOUT"] = } while waiting for confirm_responses.')
            self.step_timeout_handler(e)

        # Handle shutdown agents once successfully exiting the loop
        agents_shutdown = []
        for agent_id, agent_item in self.agent_collection.items():
            if agent_item['step_response'][self.time]['reaction'] in ['shutdown', 'waiting']:
                agents_shutdown.append(agent_id)

        for agent_id in agents_shutdown:
            self.remove_agent(agent_id)


        self.time += 1

        sim_stat = {
            'status': 'success',
            'end_sim': False,
        }

        if self.time == self.orsim_settings['SIMULATION_LENGTH_IN_STEPS']-1:
            sim_stat['end_sim'] = True


        logging.info(f'{self.scheduler_id} Runtime: {(time.time()-start_time):0.2f} sec')
        return sim_stat


    def step_timeout_handler(self, e):
        """
        Handles timeout events during simulation steps by checking the percentage of agents
        experiencing network issues. If the percentage of agents waiting for a response is
        within the allowed tolerance, logs a warning and continues processing. If the percentage
        exceeds the tolerance, logs an error, outputs agent collection details, and raises the
        provided exception to abort processing.

        Args:
            e (Exception): The exception to be raised if the tolerance is exceeded.

        Raises:
            Exception: Raises the provided exception if the percentage of agents with network issues
            exceeds the configured tolerance.
        """
        ''' '''
        tolerance = self.orsim_settings['STEP_TIMEOUT_TOLERANCE'] # Max % or agents having network issues


        if (self.agent_stat[self.time]['waiting'] / len(self.agent_collection)) <= tolerance:
            logging.warning(f"agent_stat = {self.pp.pformat(self.agent_stat[self.time])}")
            logging.warning(f"Unable to receive response from {self.agent_stat[self.time]['waiting']} Agents at {self.time=}. % Error ({(self.agent_stat[self.time]['waiting'] / len(self.agent_collection)):0.3f}) is within {tolerance=}. Continue processing...")
            # logging.warning(f'{self.pp.pformat(self.agent_collection)}')
        else:
            logging.error(f"Too many missing messages. % Error ({self.agent_stat[self.time]['waiting'] *100 / len(self.agent_collection)}) exceeded {tolerance=}. Abort...")
            logging.error(f'{self.pp.pformat(self.agent_collection)}')
            raise e
