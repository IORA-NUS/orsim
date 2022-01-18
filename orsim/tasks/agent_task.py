


from __future__ import absolute_import
# from apps.worker import app
from orsim.worker import app

# from apps.analytics_app import AnalyticsAgentIndie
from celery.signals import after_setup_task_logger

@app.task
def start_agent(module_path, agent_class_name, **kwargs):

    # from orsim import ORSimEnv
    # from apps.config import messenger_backend
    # ORSimEnv.set_backend(messenger_backend)

    import importlib
    module = importlib.import_module(module_path)
    AgentClass = getattr(module, agent_class_name)

    agent = AgentClass(**kwargs)
    agent.start_listening()
