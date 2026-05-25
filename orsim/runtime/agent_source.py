from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable


class AgentSource(ABC):
    """
    Supplies agent launch specs to ORSimRuntime.

    Each dict returned by agents_for_step() and bootstrap_agents() has the shape::

        {
            'scheduler_key': str,   # key into ORSimRuntime.schedulers
            'spec':          dict,  # passed as spec= to scheduler.add_agent()
            'project_path':  str,   # passed as project_path= to scheduler.add_agent()
            'agent_class':   str,   # passed as agent_class= to scheduler.add_agent()
        }
    """

    def bootstrap_agents(self) -> list[dict]:
        """Agents to launch before the simulation loop (e.g. persistent services).

        Override in subclasses that distinguish service agents from step-based agents.
        Default returns an empty list.
        """
        return []

    @abstractmethod
    def agents_for_step(self, step: int) -> list[dict]:
        """Return specs of agents whose Celery task should be launched at this step."""

    @abstractmethod
    def is_exhausted(self) -> bool:
        """True once no further agents will ever be produced for any future step."""


class PrecomputedAgentSource(AgentSource):
    """
    Wraps a scenario_manager + agent_config dict.

    Specs are grouped by init_time_step at construction time, preserving the
    behaviour of the old SimulationRuntime.agent_registry dict.  Service agents
    (scheduler_key != 'agent') are returned from bootstrap_agents() so they are
    launched before the simulation loop begins.

    Args:
        scenario_manager:   Object with a get_agent_collection(role) method.
        agent_config:       Dict mapping role name to config dict (same structure
                            as SimulationRuntime agent_config).
        run_id:             Simulation run identifier.
        reference_time:     Simulation reference datetime.
        project_path:       Absolute path passed to scheduler.add_agent().
        context:            Optional object forwarded to each extra_fields callable
                            as its third argument.  Use a SimpleNamespace or the
                            calling runtime if domain fields are needed.
    """

    def __init__(
        self,
        scenario_manager,
        agent_config: dict,
        run_id: str,
        reference_time: datetime,
        project_path: str,
        context=None,
    ):
        self._registry: dict[int, list[dict]] = {}
        self._bootstrap: list[dict] = []
        self._max_step = 0

        for role, cfg in agent_config.items():
            collection = scenario_manager.get_agent_collection(role)
            scheduler_key: str = cfg['scheduler_key']
            agent_class: str = cfg['agent_class']
            init_time_step_key: str | None = cfg.get('init_time_step_key')
            extra_fields_fn: Callable | None = cfg.get('extra_fields')

            for agent_id, behavior in collection.items():
                init_time_step = (
                    behavior.get('profile', {}).get(init_time_step_key, 0)
                    if init_time_step_key else 0
                )
                spec = {
                    'unique_id': agent_id,
                    'run_id': run_id,
                    'reference_time': datetime.strftime(reference_time, '%Y%m%d%H%M%S'),
                    'init_time_step': init_time_step,
                    'behavior': behavior,
                }
                if extra_fields_fn:
                    spec.update(extra_fields_fn(agent_id, behavior, context))

                item = {
                    'scheduler_key': scheduler_key,
                    'spec': spec,
                    'project_path': project_path,
                    'agent_class': agent_class,
                }

                if scheduler_key == 'agent':
                    self._registry.setdefault(init_time_step, []).append(item)
                    self._max_step = max(self._max_step, init_time_step)
                else:
                    self._bootstrap.append(item)

        self._current_step = -1

    def bootstrap_agents(self) -> list[dict]:
        return list(self._bootstrap)

    def agents_for_step(self, step: int) -> list[dict]:
        self._current_step = step
        return self._registry.get(step, [])

    def is_exhausted(self) -> bool:
        return self._current_step >= self._max_step


class GenerativeAgentSource(AgentSource):
    """
    Calls a user-supplied function each step to produce new agent specs.

    Suitable for long or infinite simulation horizons where the full agent
    population cannot be materialised upfront.

    Args:
        fn: Callable with signature fn(step: int) -> list[dict].
            Each dict must include 'scheduler_key', 'spec', 'project_path',
            and 'agent_class'.  Return an empty list for steps with no arrivals.

    Call stop() to signal that no further agents will be produced; is_exhausted()
    will then return True and DrainTermination can complete the simulation once
    all in-flight agents have finished.
    """

    def __init__(self, fn: Callable[[int], list[dict]]):
        self._fn = fn
        self._stopped = False

    def agents_for_step(self, step: int) -> list[dict]:
        return self._fn(step)

    def is_exhausted(self) -> bool:
        return self._stopped

    def stop(self):
        self._stopped = True
