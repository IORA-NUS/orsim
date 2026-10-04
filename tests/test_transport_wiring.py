"""The transport reaches the two places that actually talk to the broker.

A transport parameter that is accepted and then dropped is worse than no
parameter at all, and that is not hypothetical here: ``Messenger.__init__``
already HAD a ``transport=`` argument before this work, and it was dead --
supplying one skipped the branch that assigned ``self.client``, so the very next
line raised ``AttributeError``.  Nothing caught it because nothing exercised it.

These tests exercise it, end to end, with no broker, no eventlet greenlet and no
Celery worker: construct a scheduler and an agent with a ``FakeTransport``, then
drive one real step through each and read the message off the fake.
"""

import asyncio
import json

import pytest

from orsim.core import ORSimEnv, ORSimScheduler
from orsim.lifecycle import ORSimAgent

from .fakes import SETTINGS, FakeTransport


ORSIM_SETTINGS = {
    'DOMAIN': 'test_domain',
    'SIMULATION_LENGTH_IN_STEPS': 960,
    'STEP_INTERVAL': 30,
    'AGENT_LAUNCH_TIMEOUT': 15,
    'STEP_TIMEOUT': 60,
    'STEP_TIMEOUT_TOLERANCE': 0.1,
    'REFERENCE_TIME': '2020-01-01 04:00:00',
    'HEARTBEAT_INTERVAL': 5,
}

BEHAVIOR = {
    'email': 'dummy@example.invalid',
    'password': 'secret_password',
    'persona': {'domain': 'test_domain', 'role': 'dummy'},
    'profile': {},
    'response_rate': 1.0,
    'step_only_on_events': False,
    'steps_per_action': 1,
}


@pytest.fixture
def orsim_env():
    """Set (and restore) the process-wide backend.

    ``ORSimEnv.messenger_settings`` is a CLASS attribute, so leaving it set would
    leak into every later test in the session.  The settings point at a
    non-existent host on purpose: nothing in these tests may reach it, and if
    something ever did, the failure should be loud rather than a silent
    connection to a real local broker.
    """
    previous = ORSimEnv.messenger_settings
    ORSimEnv.set_backend(SETTINGS)
    try:
        yield
    finally:
        ORSimEnv.messenger_settings = previous


class DummyAgent(ORSimAgent):
    """The smallest concrete ORSimAgent: no app, no I/O, records its steps."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stepped = []

    def _create_app(self):
        return None

    @property
    def process_payload_on_init(self):
        return True

    def entering_market(self, time_step):
        self.active = True

    def step(self, time_step):
        self.stepped.append(time_step)
        return True

    def exiting_market(self):
        return None

    def estimate_next_event_time(self):
        return self.current_time

    def logout(self):
        return True


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------


def make_scheduler(fake):
    return ORSimScheduler('run_t', 'agent_scheduler', ORSIM_SETTINGS, transport=fake)


def test_scheduler_threads_the_transport_into_its_messenger(orsim_env):
    fake = FakeTransport()
    scheduler = make_scheduler(fake)

    assert scheduler.agent_messenger.client is fake


def test_scheduler_subscribes_to_its_own_topic_and_starts_the_loop(orsim_env):
    fake = FakeTransport()
    scheduler = make_scheduler(fake)

    assert fake.call_names == ['on_message', 'subscribe', 'loop_start']
    assert fake.subscriptions == [(('run_t/agent_scheduler/ORSimScheduler',), {'qos': 0})]
    # Bound methods compare equal, not identical -- `is` would be wrong here.
    assert fake.on_message == scheduler.on_receive_message


def test_scheduler_step_publishes_through_the_transport(orsim_env):
    """One real step, end to end, with no broker anywhere in the path.

    ``confirm_responses`` resolves immediately because the agent collection is
    empty (waiting == 0 on the first tally), so the barrier is satisfied without
    any agent having to reply.
    """
    fake = FakeTransport()
    scheduler = make_scheduler(fake)

    result = asyncio.run(scheduler.step())

    assert result == {'status': 'success', 'end_sim': False}
    assert len(fake.published) == 1
    topic, payload = fake.published[0]
    assert topic == 'run_t/agent_scheduler/ORSimAgent'
    assert json.loads(payload) == {'action': 'step', 'time_step': 0}


def test_scheduler_receives_an_agent_response_through_the_transport(orsim_env):
    """The inbound half: a delivered message updates the agent's step response.

    ``deliver`` stands in for the broker's receive loop.  ``_loop`` is still None
    at this point (it is created inside the first ``step()``), which is the path
    ``on_receive_message`` guards with ``if self._loop is not None``.
    """
    fake = FakeTransport()
    scheduler = make_scheduler(fake)
    scheduler.agent_collection['agent_1'] = {
        'spec': {'unique_id': 'agent_1'},
        'step_response': {0: {'reaction': 'waiting', 'did_step': False, 'run_time': 0}},
    }

    fake.deliver('run_t/agent_scheduler/ORSimScheduler', {
        'agent_id': 'agent_1',
        'time_step': 0,
        'action': 'completed',
        'did_step': True,
        'run_time': 0.5,
    })

    assert scheduler.agent_collection['agent_1']['step_response'][0] == {
        'reaction': 'completed',
        'did_step': True,
        'run_time': 0.5,
    }


def test_scheduler_without_a_transport_is_unchanged_by_default(orsim_env, monkeypatch):
    """``transport=None`` still builds the default path.

    Asserted against a stub PahoTransport rather than a patched-out paho, so the
    claim is specifically "Messenger was asked to build the default transport",
    not "some mock absorbed the calls".
    """
    built = []

    class StubPahoTransport(FakeTransport):
        def __init__(self, settings, credentials):
            super().__init__()
            built.append((settings, credentials))

    monkeypatch.setattr('orsim.messenger.messenger.PahoTransport', StubPahoTransport)

    scheduler = ORSimScheduler('run_t', 'agent_scheduler', ORSIM_SETTINGS)

    assert len(built) == 1
    assert built[0][0] is SETTINGS
    assert built[0][1] == {
        'email': 'run_t_agent_scheduler_ORSimScheduler',
        'password': 'secret_password',
    }
    assert isinstance(scheduler.agent_messenger.client, StubPahoTransport)


# ---------------------------------------------------------------------------
# Agent
# ---------------------------------------------------------------------------


def make_agent(fake):
    return DummyAgent(
        unique_id='dummy_1',
        run_id='run',
        reference_time='20200101000000',
        init_time_step=0,
        scheduler={'id': 'scheduler', 'orsim_settings': ORSIM_SETTINGS},
        behavior=BEHAVIOR,
        transport=fake,
    )


def test_agent_threads_the_transport_into_its_messenger(orsim_env):
    fake = FakeTransport()
    agent = make_agent(fake)

    assert agent.messenger.client is fake


def test_agent_construction_starts_the_loop_but_subscribes_to_nothing(orsim_env):
    """Agents pass channel_id=None, so the subscription happens later.

    ``start_listening`` is what subscribes an agent (to its whole handler table
    at once) and attaches its callback -- and it also spawns an eventlet
    greenlet, so it is deliberately not called here.  Construction alone must
    therefore show loop_start and nothing else.
    """
    fake = FakeTransport()
    make_agent(fake)

    assert fake.calls == [('loop_start', (), {})]


def test_agent_step_publishes_its_completion_through_the_transport(orsim_env):
    """The handler is driven directly, bypassing the greenlet entirely."""
    fake = FakeTransport()
    agent = make_agent(fake)
    fake.calls.clear()

    agent.handle_orsim_agent_message({'action': 'step', 'time_step': 3})

    assert agent.stepped == [3]
    assert len(fake.published) == 1
    topic, payload = fake.published[0]
    assert topic == 'run/scheduler/ORSimScheduler'

    body = json.loads(payload)
    assert body['agent_id'] == 'dummy_1'
    assert body['action'] == 'completed'
    assert body['time_step'] == 3
    assert body['did_step'] is True


def test_agent_without_a_transport_is_unchanged_by_default(orsim_env, monkeypatch):
    built = []

    class StubPahoTransport(FakeTransport):
        def __init__(self, settings, credentials):
            super().__init__()
            built.append((settings, credentials))

    monkeypatch.setattr('orsim.messenger.messenger.PahoTransport', StubPahoTransport)

    agent = DummyAgent(
        unique_id='dummy_1',
        run_id='run',
        reference_time='20200101000000',
        init_time_step=0,
        scheduler={'id': 'scheduler', 'orsim_settings': ORSIM_SETTINGS},
        behavior=BEHAVIOR,
    )

    assert len(built) == 1
    assert built[0][1] == {
        'email': 'run_scheduler_dummy_1',
        'password': 'secret_password',
    }
    assert isinstance(agent.messenger.client, StubPahoTransport)
