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
    'HEARTBEAT_INTERVAL': {'type': 'integer', 'required': True,},
}

class ORSimEnv:
    """
    ORSimEnv provides environment configuration and validation utilities for the ORSim simulation.

    Class Attributes:
        messenger_settings (dict or None): Stores the current messenger backend settings.

    Class Methods:
        set_backend(settings):
            Validates and sets the messenger backend settings using the provided schema.
            Args:
                settings (dict): Messenger backend configuration to validate and set.
            Raises:
                ValidationError: If the settings do not conform to the schema.

        validate_orsim_settings(settings):
            Validates the ORSim environment settings using the provided schema.
            Args:
                settings (dict): ORSim environment configuration to validate.
            Returns:
                dict: The validated settings.
            Raises:
                ValidationError: If the settings do not conform to the schema.
    """

    messenger_settings = None

    @classmethod
    def set_backend(cls, settings):
        """
        Sets the messenger backend settings for the class after validating them against the schema.

        Args:
            settings (dict): A dictionary containing the messenger backend settings to be validated and set.

        Raises:
            ValidationError: If the provided settings do not conform to the messenger backend settings schema.
        """
        v = Validator(allow_unknown=True)

        if v.validate(settings, messenger_backend_settings_schema):
            cls.messenger_settings = settings
        else:
            logging.error(f'{json.dumps(v.errors, indent=2)}')
            raise ValidationError(json.dumps(v.errors))


    @classmethod
    def validate_orsim_settings(cls, settings):
        """
        Validates the provided ORSim settings dictionary against the predefined schema.

        Args:
            settings (dict): The ORSim settings to validate.

        Returns:
            dict: The validated settings if they conform to the schema.

        Raises:
            ValidationError: If the settings do not conform to the schema, with details about the validation errors.
        """
        v = Validator(allow_unknown=True)

        if v.validate(settings, orsim_settings_schema):
            return settings
        else:
            logging.error(f'{json.dumps(v.errors, indent=2)}')
            raise ValidationError(json.dumps(v.errors))
