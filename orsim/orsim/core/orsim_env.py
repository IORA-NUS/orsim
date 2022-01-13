from cerberus import Validator
from cerberus.errors import ValidationError
import json, logging

messenger_backend_settings_schema = {
    'RABBITMQ_MANAGEMENT_SERVER': {'type': 'string', 'required': True,},
    'RABBITMQ_ADMIN_USER': {'type': 'string', 'required': True,},
    'RABBITMQ_ADMIN_PASSWORD': {'type': 'string', 'required': True,},
    'MQTT_BROKER': {'type': 'string', 'required': True,},
}

orsim_settings_schema = {
    'SIMULATION_LENGTH_IN_STEPS': {'type': 'integer', 'required': True,},
    'STEP_INTERVAL': {'type': 'integer', 'required': True,},

    'AGENT_LAUNCH_TIMEOUT': {'type': 'integer', 'required': True,},
    'STEP_TIMEOUT': {'type': 'integer', 'required': True,},
    'STEP_TIMEOUT_TOLERANCE': {'type': 'float', 'required': True,}, # NOTE deprecated

    'REFERENCE_TIME': {'type': 'string', 'required': True,},
}

class ORSimEnv:

    messenger_settings = None

    @classmethod
    def set_backend(cls, settings):
        v = Validator(allow_unknown=True)

        if v.validate(settings, messenger_backend_settings_schema):
            cls.messenger_settings = settings
        else:
            logging.error(f'{json.dumps(v.errors, indent=2)}')
            raise ValidationError(json.dumps(v.errors))


    @classmethod
    def validate_orsim_settings(cls, settings):
        v = Validator(allow_unknown=True)

        if v.validate(settings, orsim_settings_schema):
            return settings
        else:
            logging.error(f'{json.dumps(v.errors, indent=2)}')
            raise ValidationError(json.dumps(v.errors))
