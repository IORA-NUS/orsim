
from __future__ import absolute_import

import sys
from orsim.core import ORSimEnv
# from apps.worker import app
from orsim.worker import app

# from apps.analytics_app import AnalyticsAgentIndie
from celery.signals import after_setup_task_logger

@app.task
def start_agent(project_path, module_name, agent_class_name, messenger_settings, **kwargs):

    if not project_path in sys.path:
        sys.path.append(project_path)
    # print(sys.path)
    # from apps.config import messenger_backend

    ORSimEnv.set_backend(messenger_settings)

    import importlib
    module = importlib.import_module(module_name)
    AgentClass = getattr(module, agent_class_name)

    agent = AgentClass(**kwargs)
    agent.start_listening()
