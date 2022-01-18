from abc import ABC, abstractclassmethod, abstractmethod
import asyncio, json, logging, time, os, traceback
from collections import OrderedDict

from datetime import datetime
from dateutil.relativedelta import relativedelta
from orsim.messenger import Messenger

from orsim.core.orsim_env import ORSimEnv
from orsim.utils import time_to_str, str_to_time

from cerberus import Validator

class ORSimAgent(ABC):

    messenger = None
    agent_failed = False
    payload_cache = None
    step_log = {}

    # def __init__(self, unique_id, run_id, reference_time, init_time_step, scheduler_id, behavior, orsim_settings):
    def __init__(self, unique_id, run_id, reference_time, init_time_step, scheduler, behavior): #, orsim_settings):
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
        self.active = False

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

    def register_message_handler(self, topic, method):
        self.message_handlers[topic] = method

    def is_active(self):
        return self.active

    def reset_step_log(self):
        self.step_log = {}

    def add_step_log(self, message):
        self.step_log[datetime.now().isoformat()] = message

    def take_first_step(self, dummy_payload):
        self.message_processing_active = True
        self.handle_orsim_agent_message(dummy_payload)

    def handle_orsim_agent_message(self, payload):
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
        ''' '''
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
        start_time_for_ready = time.time() # NOTE Local start time NOT a class variable

        if not self.agent_failed:
            # self.agent_messenger = Messenger(self.agent_credentials, f"{self.run_id}/{self.scheduler_id}/ORSimAgent", self.on_receive_message)
            self.messenger.client.subscribe([(topic, 0) for topic, _ in self.message_handlers.items()])
            self.messenger.client.on_message = self.on_receive_message

            # print('subscribed to ', self.message_handlers)

            # if settings['CONCURRENCY_STRATEGY'] == 'ASYNCIO':
            #     logging.debug(f'Agent {self.unique_id} is Listening for Messages')
            #     loop = asyncio.get_event_loop()
            #     try:
            #         loop.run_forever()
            #     except KeyboardInterrupt:
            #         pass
            #     finally:
            #         loop.close()
            # elif settings['CONCURRENCY_STRATEGY'] == 'EVENTLET':
            import eventlet

            def run_forever():
                while True:
                    # eventlet.sleep(0.1)
                    eventlet.sleep(5)
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

