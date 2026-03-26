# AGENT_BEHAVIOR_SCHEMA for ORSimAgent validation

AGENT_BEHAVIOR_SCHEMA = {
    'email': {'type': 'string', 'required': True},
    'password': {'type': 'string', 'required': True},
    'persona': {
        'type': 'dict',
        'schema': {
            'domain': {'type': 'string', 'required': True},
            'role': {'type': 'string', 'required': True},
        },
        'required': True
    },
    'profile': {'type': 'dict', 'required': True},
    'response_rate': {'type': 'number', 'required': True},
    'step_only_on_events': {'type': 'boolean', 'required': True},
    'steps_per_action': {'type': 'integer', 'required': True},
    # Add agent-specific fields in subclasses or at runtime
}
