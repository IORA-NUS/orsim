import asyncio
import logging
import time
import cerberus

from .agent_source import AgentSource
from .termination import TerminationCondition


class ORSimRuntime:
    """
    Generic simulation orchestrator — part of the orsim library.

    Handles scheduler instantiation, service bootstrapping, the agent launch
    loop, and the step loop.  Domain-specific lifecycle events are expressed as
    hook methods that subclasses override:

        on_before_run()                   — once at the start of run_simulation()
        on_step_complete(step, elapsed)   — after each step completes
        on_simulation_complete(elapsed)   — once when the loop exits

    Args:
        run_id:                Unique identifier for the simulation run.
        scheduler_config:      Dict mapping scheduler keys to parameter dicts.
                               Same structure as before; validated internally.
        agent_source:          AgentSource instance controlling agent arrivals.
        termination_condition: TerminationCondition instance controlling the
                               loop exit and final-step signal.
        messenger_backend:     Dict passed to ORSimEnv.set_backend().
    """

    def __init__(
        self,
        run_id: str,
        scheduler_config: dict,
        agent_source: AgentSource,
        termination_condition: TerminationCondition,
        messenger_backend: dict,
    ):
        from orsim.core.orsim_env import ORSimEnv
        ORSimEnv.set_backend(messenger_backend)

        self.run_id = run_id
        self.agent_source = agent_source
        self.termination_condition = termination_condition

        self._validate_scheduler_config(scheduler_config)
        self.schedulers = self._instantiate_schedulers(scheduler_config)

        for item in self.agent_source.bootstrap_agents():
            self.schedulers[item['scheduler_key']].add_agent(
                spec=item['spec'],
                project_path=item['project_path'],
                agent_class=item['agent_class'],
            )

    # ------------------------------------------------------------------ hooks

    def on_before_run(self):
        """Called once at the start of run_simulation(), before the first step."""

    def on_step_complete(self, step: int, elapsed: float):
        """Called after each step completes.  elapsed is total seconds since run started."""

    def on_simulation_complete(self, elapsed: float):
        """Called once when the simulation loop exits."""

    # ------------------------------------------------------------------ loop

    def run_simulation(self):
        self.on_before_run()
        start = time.time()
        step = 0

        while not self.termination_condition.should_terminate(step, self.schedulers, self.agent_source):
            logging.info(f"[ORSimRuntime] Step {step}")

            for item in self.agent_source.agents_for_step(step):
                self.schedulers[item['scheduler_key']].add_agent(
                    spec=item['spec'],
                    project_path=item['project_path'],
                    agent_class=item['agent_class'],
                )

            is_final = self.termination_condition.is_final_step(step, self.schedulers, self.agent_source)
            for scheduler in self.schedulers.values():
                asyncio.run(scheduler.step(is_final=is_final))

            self.on_step_complete(step, time.time() - start)
            step += 1

        total = time.time() - start
        self.on_simulation_complete(total)
        logging.info(f"[ORSimRuntime] Simulation complete in {total:.2f}s")

    # --------------------------------------------------------------- helpers

    def _instantiate_schedulers(self, scheduler_config: dict) -> dict:
        from orsim.core.orsim_scheduler import ORSimScheduler
        return {key: ORSimScheduler(**params) for key, params in scheduler_config.items()}

    def _validate_scheduler_config(self, scheduler_config: dict):
        field_schema = {
            'run_id':               {'type': 'string', 'required': True},
            'scheduler_id':         {'type': 'string', 'required': True},
            'orsim_settings':       {'type': 'dict',   'required': True},
            'init_failure_handler': {'type': 'string', 'required': False},
        }
        schema = {
            key: {'type': 'dict', 'schema': field_schema, 'required': True}
            for key in scheduler_config
        }
        v = cerberus.Validator()
        if not v.validate(scheduler_config, schema):
            raise ValueError(f"Invalid scheduler_config: {v.errors}")
