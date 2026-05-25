from abc import ABC, abstractmethod
from datetime import datetime


class TerminationCondition(ABC):
    """
    Determines when the simulation loop should stop.

    should_terminate() is evaluated at the top of each iteration — before agents
    are launched or a step is executed.  is_final_step() is evaluated inside the
    loop to decide whether to pass is_final=True to scheduler.step(), which causes
    agents to receive action='shutdown' rather than action='step'.
    """

    @abstractmethod
    def should_terminate(self, step: int, schedulers: dict, source) -> bool:
        """Return True if the simulation should stop before executing this step."""

    def is_final_step(self, step: int, schedulers: dict, source) -> bool:
        """Return True if this step is the last one that will execute.

        Default: look one step ahead.  Override for conditions where the final
        step cannot be determined by a single forward check (e.g. DrainTermination).
        """
        return self.should_terminate(step + 1, schedulers, source)


class FixedStepTermination(TerminationCondition):
    """Terminate after exactly n steps.  Direct replacement for SIMULATION_LENGTH_IN_STEPS."""

    def __init__(self, n: int):
        self.n = n

    def should_terminate(self, step: int, schedulers: dict, source) -> bool:
        return step >= self.n

    def is_final_step(self, step: int, schedulers: dict, source) -> bool:
        return step == self.n - 1


class DrainTermination(TerminationCondition):
    """
    Terminate when the source is exhausted AND every scheduler has no registered agents.

    Use with GenerativeAgentSource (call source.stop() when the data stream ends).
    The simulation runs until the last wave of agents completes naturally — no
    forced shutdown message is sent while active agents remain.
    """

    def should_terminate(self, step: int, schedulers: dict, source) -> bool:
        if not source.is_exhausted():
            return False
        return all(len(s.agent_collection) == 0 for s in schedulers.values())


class WallClockTermination(TerminationCondition):
    """Terminate when real wall-clock time passes end_datetime."""

    def __init__(self, end_datetime: datetime):
        self.end_datetime = end_datetime

    def should_terminate(self, step: int, schedulers: dict, source) -> bool:
        return datetime.now() >= self.end_datetime


class CompositeTermination(TerminationCondition):
    """Combine multiple TerminationConditions with 'any' (OR) or 'all' (AND) logic."""

    def __init__(self, conditions: list[TerminationCondition], mode: str = 'any'):
        if mode not in ('any', 'all'):
            raise ValueError(f"mode must be 'any' or 'all', got {mode!r}")
        self.conditions = conditions
        self.mode = mode

    def should_terminate(self, step: int, schedulers: dict, source) -> bool:
        fn = any if self.mode == 'any' else all
        return fn(c.should_terminate(step, schedulers, source) for c in self.conditions)
