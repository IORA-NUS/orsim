from orsim.core import ORSimAgent, ORSimEnv
from datetime import datetime

orsim_settings = {
    # 'SIMULATION_LENGTH_IN_STEPS': 960, # 960, # 600,    # 60 # Num Steps
    'SIMULATION_LENGTH_IN_STEPS': 960, # 960, # 600,    # 60 # Num Steps
    'STEP_INTERVAL': 30, # 15, # 6,     # 60   # seconds in Simulation Universe

    'AGENT_LAUNCH_TIMEOUT': 15,
    'STEP_TIMEOUT': 60, # Max Compute time for each step (seconds) in CPU time
    'STEP_TIMEOUT_TOLERANCE': 0.1,

    'REFERENCE_TIME': '2020-01-01 04:00:00',
}

messenger_backend = {
    'RABBITMQ_MANAGEMENT_SERVER': "http://localhost:15672/api", # "http://192.168.10.135:15672/api", # "http://localhost:15672/api",
    'RABBITMQ_ADMIN_USER': 'guest', # 'test', # 'guest',
    'RABBITMQ_ADMIN_PASSWORD': 'guest', #'test', # 'guest',

    'MQTT_BROKER': "localhost", # "192.168.10.115", # "localhost",
    'WEB_MQTT_PORT': 15675,
}


class DummyAgent(ORSimAgent):

    def __init__(self, unique_id, run_id, reference_time, init_time_step, scheduler, behavior): #, orsim_settings):
        ''' '''
        super().__init__(unique_id, run_id, reference_time, init_time_step, scheduler, behavior) #, orsim_settings)

    def process_payload(self, payload):
        return True

    def estimate_next_event_time(self):
        return True

    def logout(self):
        ''' process any logout processes needed in the agent.
        '''
        return True



def test_orsim_agent_():

    ORSimEnv.set_backend(messenger_backend)

    agent = DummyAgent(unique_id='unique_id',
                       run_id='run_id',
                       reference_time='20200101000000',
                       init_time_step=0,
                       scheduler={
                           'id': 'scheduler_id',
                           'orsim_settings': orsim_settings,
                       },
                       behavior={},
    )
                    #    executor_name='DummyAgent')

    assert agent.current_time == datetime(2020, 1, 1)


