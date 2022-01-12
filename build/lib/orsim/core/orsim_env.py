from cerberus import Validator
from cerberus.errors import ValidationError
import json, logging

settings_schema = {
    'RABBITMQ_MANAGEMENT_SERVER': {'type': 'string', 'required': True,},
    'RABBITMQ_ADMIN_USER': {'type': 'string', 'required': True,},
    'RABBITMQ_ADMIN_PASSWORD': {'type': 'string', 'required': True,},
    'MQTT_BROKER': {'type': 'string', 'required': True,},
}


class ORSimEnv:

    messenger_settings = None


    @classmethod
    def set_backend(cls, settings):
        v = Validator(allow_unknown=True)

        if v.validate(settings, settings_schema):
            cls.messenger_settings = settings
        else:
            logging.error(f'{json.dumps(v.errors, indent=2)}')
            raise ValidationError(json.dumps(v.errors))
