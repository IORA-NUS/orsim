
from __future__ import absolute_import

import sys
from orsim.core import ORSimEnv
# from apps.worker import app
from orsim.worker import app

# from apps.analytics_app import AnalyticsAgentIndie
from celery.signals import after_setup_task_logger

@app.task
def start_agent(project_path, module_name, agent_class_name, messenger_settings, **kwargs):
    """
    Starts an agent by dynamically importing the specified module and class, initializing it with provided arguments, and starting its listening process.

    Args:
        project_path (str): The file system path to the project directory to be added to sys.path.
        module_name (str): The name of the module containing the agent class.
        agent_class_name (str): The name of the agent class to instantiate.
        messenger_settings (dict): Settings to configure the messaging backend for the agent environment.
        **kwargs: Additional keyword arguments to pass to the agent class constructor.

    Returns:
        None
    """

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
