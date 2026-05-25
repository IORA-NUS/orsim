# Design: Flexible Simulation Runtime

**Status:** Proposed — under review before implementation  
**Branch:** `feature/event-driven-confirm-responses` (event-driven scheduler already merged here)

---

## Motivation

The current `SimulationRuntime` in `openride_apps` has two structural constraints that limit the platform's scalability and generality:

1. **All agent data must be materialised upfront.** `scenario_manager.get_agent_collection()` returns a complete dict at init time. This works for precomputed, deterministic studies but cannot support long time horizons or generative agent populations.

2. **The simulation horizon is hardcoded.** `SIMULATION_LENGTH_IN_STEPS` in `orsim_settings` is the sole termination oracle, baked into both the scheduler and the runtime loop. Infinite or condition-based termination is not expressible.

A third, related issue: `SimulationRuntime` lives in the application layer (`openride_apps`) but contains generic orchestration logic that belongs in the `orsim` library. Every new ecosystem re-implements the same scaffolding.

---

## Proposed Design

Two new abstractions — `AgentSource` and `TerminationCondition` — are introduced as first-class interfaces in `orsim`. The core runtime loop moves into the library as `ORSimRuntime`. Domain-specific lifecycle events are expressed as override hooks.

### 1. `AgentSource`

Decouples *when agent data is generated* from *when an agent is launched*.

```python
# orsim/runtime/agent_source.py

class AgentSource(ABC):
    @abstractmethod
    def agents_for_step(self, step: int) -> list[dict]:
        """Return specs of agents whose Celery task should launch at this step."""

    @abstractmethod
    def is_exhausted(self) -> bool:
        """True once no further agents will ever be produced for any future step."""
```

| Implementation | Use case | Behaviour |
|---|---|---|
| `PrecomputedAgentSource(scenario_manager, agent_config)` | Current ridehail scenario | Wraps existing `scenario_manager`; groups specs by `init_time_step` at construction; yields on demand. Replaces `agent_registry` dict. |
| `GenerativeAgentSource(fn)` | Long / infinite horizon | Calls `fn(step, sim_state) -> list[specs]` each step. `is_exhausted()` returns `False` until an external stop signal is set. |
| `StreamingAgentSource(queue)` | Real-time / external feed | Drains a queue each step; exhausted when queue is closed. |

`PrecomputedAgentSource` preserves exact current behaviour for ridehail — it is a drop-in replacement for `self.agent_registry`.

### 2. `TerminationCondition`

Decouples *why the simulation stops* from *how many steps were configured*.

```python
# orsim/runtime/termination.py

class TerminationCondition(ABC):
    @abstractmethod
    def should_terminate(self, step: int, schedulers: dict, source: AgentSource) -> bool:
        ...

    def is_final_step(self, step: int, schedulers: dict, source: AgentSource) -> bool:
        """Called one step ahead to allow the scheduler to send 'shutdown' to agents."""
        return self.should_terminate(step + 1, schedulers, source)
```

| Implementation | Behaviour |
|---|---|
| `FixedStepTermination(n)` | Terminates after exactly `n` steps. Equivalent to current `SIMULATION_LENGTH_IN_STEPS`. |
| `DrainTermination()` | Terminates when `source.is_exhausted()` **and** all agent schedulers have zero registered agents. Lets the last wave of agents complete naturally. |
| `WallClockTermination(end_datetime)` | Terminates when the simulation clock passes a wall-clock timestamp. |
| `CompositeTermination([...], mode='any'/'all')` | Logical combination of conditions. |

### 3. `ORSimRuntime` — moves into the library

```python
# orsim/runtime/orsim_runtime.py

class ORSimRuntime:
    def __init__(
        self,
        run_id: str,
        scheduler_config: dict,
        agent_source: AgentSource,
        termination_condition: TerminationCondition,
        project_path: str,
    ):
        ...

    # --- Lifecycle hooks (override in domain subclass) ---
    def on_before_run(self): pass
    def on_step_complete(self, step: int): pass
    def on_simulation_complete(self): pass

    def run_simulation(self):
        self.on_before_run()
        step = 0
        while not self.termination_condition.should_terminate(step, self.schedulers, self.agent_source):
            for spec in self.agent_source.agents_for_step(step):
                self.schedulers[spec['_scheduler_key']].add_agent(
                    spec=spec, project_path=self.project_path, agent_class=spec['_agent_class']
                )
            is_final = self.termination_condition.is_final_step(step, self.schedulers, self.agent_source)
            for scheduler in self.schedulers.values():
                asyncio.run(scheduler.step(is_final=is_final))
            self.on_step_complete(step)
            step += 1
        self.on_simulation_complete()
```

### 4. `ORSimScheduler.step()` — minor signature change

The scheduler currently determines `action='shutdown'` vs `action='step'` by comparing `self.time` to `SIMULATION_LENGTH_IN_STEPS`. This knowledge should come from the runtime:

```python
async def step(self, is_final: bool = False):
    action = 'shutdown' if is_final else 'step'
    ...
```

`SIMULATION_LENGTH_IN_STEPS` remains in `orsim_settings` as a required field for `FixedStepTermination` and for pre-allocating any step-indexed data structures, but it is no longer the termination oracle inside the scheduler.

---

## Impact on the ridehail use case

The existing `run_ridehail_simulation.py` and the `agent_config` / `scheduler_config` dicts are **unchanged in structure**. The changes are entirely at the wiring layer.

### What changes

**`openride_apps/apps/simulation/simulation_runtime.py`**  
Becomes a thin subclass of `ORSimRuntime`, implementing only the three domain-specific hooks:

```python
from orsim import ORSimRuntime

class SimulationRuntime(ORSimRuntime):

    def on_before_run(self):
        self.user = self._setup_user()
        self.run_record = self._init_run_config()
        self._register_state_machines()

    def on_step_complete(self, step):
        self.run_record = self._update_status('In Progress', ...)

    def on_simulation_complete(self):
        self._update_status('success', ...)
```

All generic orchestration (scheduler instantiation, agent registration loop, step loop, config validation) is deleted from this file — it lives in `ORSimRuntime`.

**`run_ridehail_simulation.py`**  
Three additions; nothing removed:

```python
from orsim.runtime import PrecomputedAgentSource, FixedStepTermination

agent_source = PrecomputedAgentSource(scenario_manager, agent_config)
termination = FixedStepTermination(scenario_manager.orsim_settings['SIMULATION_LENGTH_IN_STEPS'])

sim = SimulationRuntime(
    run_id=run_id,
    scheduler_config=scheduler_config,
    agent_source=agent_source,
    termination_condition=termination,
    ...
)
```

### What stays the same

- `agent_config` dict — **identical**
- `scheduler_config` dict — **identical**
- `ScenarioManager` — **untouched**
- `sim.run_simulation()` call — **identical**
- Agent classes (`DriverAgentIndie`, `PassengerAgentIndie`, etc.) — **untouched**
- All `ORSimAgent` / `ORSimApp` / `ORSimManager` subclasses — **untouched**

---

## The selective wakeup opportunity (future work)

`ORSimAgent.estimate_next_event_time()` is defined on every agent but the scheduler never consults it. The broadcast model currently messages **all registered agents** every step, regardless of whether they have anything to do.

For ridehail: a driver shift spans ~480 steps but a driver may only meaningfully act on ~20 of them. The remaining 460 are no-op round trips over MQTT.

A future enhancement to `ORSimScheduler`:
- Agents report `next_event_step` in their step response payload
- Scheduler maintains a min-heap `(next_event_step, agent_id)`
- Each step: only agents with `next_event_step <= current_step` are messaged; others are pre-filled as `did_step=False, reaction='completed'`

This reduces MQTT broadcast cost from **O(all registered agents)** to **O(agents active this step)** — potentially one to two orders of magnitude for sparse populations. This is additive and does not affect the interfaces described above.

---

## New module layout in `orsim`

```
orsim/
├── core/
│   ├── orsim_env.py
│   └── orsim_scheduler.py        (add is_final param to step())
├── runtime/                       ← new package
│   ├── __init__.py
│   ├── orsim_runtime.py           (ORSimRuntime base class)
│   ├── agent_source.py            (AgentSource ABC + PrecomputedAgentSource, GenerativeAgentSource)
│   └── termination.py             (TerminationCondition ABC + FixedStepTermination, DrainTermination, ...)
├── lifecycle/
│   ...
```

---

## Open questions before implementation

- Should `ORSimRuntime` also absorb `validate_agent_config` / `validate_scheduler_config` from `SimulationRuntime`, or keep validation in the application layer where schemas may vary per domain?
- `SIMULATION_LENGTH_IN_STEPS` in `orsim_settings_schema` — make optional (nullable) once `FixedStepTermination` is the explicit owner of that value, or keep required for backward compatibility?
- For `DrainTermination`, what is the right definition of "all agent schedulers have zero agents"? Currently `remove_agent` is called post-step for shutdown/waiting agents, so the count reaches zero at the end of the final active step — this needs verification.
