"""The conformance suite is only worth having if it REJECTS something.

WHAT THIS FILE IS FOR
---------------------
``orsim/messenger/conformance.py`` turns the prose contract in ``transport.py``
into twenty executable checks.  That is only an improvement if the checks
actually BIND -- and the way this project already learned that lesson was
expensive: round 1 shipped a test that pinned the abstract-method SET, which
certified any class declaring six method names and would have passed a Kafka
transport that moved not one message.

The lesson was then learned a SECOND time, which is why there are twenty checks
and not sixteen: an adversarial review of phase 1 wrote four transports that
each scored a full 16/16 including STRICT and each is fatally broken in a real
run (a dead re-subscribe, an eager rebalance, unserialised dispatch, and
keyword-incompatible signatures).  Their mutants are in
``conformance_doubles.py``; read their docstrings before trusting a green
report.

So this file does not merely run the suite.  It runs it against:

* a CONFORMING transport (``LoopbackTransport``) -- which must pass everything,
  or the suite is wrong;
* the incumbent ``PahoTransport``, on a functional paho double -- which must pass
  every REQUIRED check, because phase 1 changes nothing observable;
* TWENTY-ONE mutants, each violating exactly one rule -- each of which must be
  caught, by the EXACT set of checks that owns that rule.  ``test_every_check_is
  _bound_by_at_least_one_mutant`` makes that structural: a new check with no
  mutant fails this file rather than joining the suite as decoration.

It also pins two things about the HARNESS, because a suite is only as honest as
what drives it: a harness that omits the loop probe silently stops catching a
leaked receive thread, and a harness that does the ROUTING makes every check
vacuous.  Both are demonstrated rather than asserted in a comment.

NO BROKER, NO SOCKET.  ``tests/conftest.py``'s network guard is in force here as
everywhere else; the substrate is a dict, a lock and a queue per client.
"""

import pytest

from orsim.messenger.conformance import (
    CHECKS,
    REQUIRED,
    STRICT,
    ConformanceFailure,
    TransportHarness,
    run_conformance_suite,
)
from orsim.messenger.paho_transport import PahoTransport

from .conformance_doubles import (
    MUTANTS,
    LeakLoopThreadTransport,
    LoopbackTransport,
    Message,
    NoPublishVirtualTransport,
    PahoWorld,
    World,
    functional_paho,
)
from .fakes import CREDENTIALS, SETTINGS, FakeTransport


ALL_CHECK_IDS = {check.id for check in CHECKS}


@pytest.fixture
def loopback_world():
    world = World()
    try:
        yield world
    finally:
        world.close()


# ---------------------------------------------------------------------------
# Positive controls
# ---------------------------------------------------------------------------


def test_a_conforming_transport_passes_every_check(loopback_world):
    """The suite's floor: if this fails, the suite is wrong, not the transport.

    ``LoopbackTransport`` is deliberately ASYNCHRONOUS -- a real background thread,
    real per-subscriber fan-out -- so passing here means the checks tolerate the
    "arrived yet, or not at all?" ambiguity a network transport creates.  A
    synchronous double would let a suite pass that could never pass against a
    broker.
    """
    report = run_conformance_suite(loopback_world.harness('LoopbackTransport'))
    assert report.ok, report.summary()
    assert {r.id for r in report.results} == ALL_CHECK_IDS


def test_paho_transport_passes_every_required_check():
    """Phase 1 changes nothing observable, and the incumbent proves it.

    Driven by ``FakePahoClient``, a functional stand-in that reproduces the paho
    behaviours ``PahoTransport`` leans on -- str-in/bytes-out, verbatim ``'/'``
    topics, ordered publish-then-disconnect, a network thread that ends on
    ``disconnect()``.  ``tests.fakes.patched_broker`` cannot be used for this: its
    ``MagicMock`` records calls and delivers nothing, which is right for the
    construction-order assertions in ``test_paho_transport.py`` and useless for
    asking whether messages move.
    """
    world = PahoWorld(SETTINGS, CREDENTIALS)
    world.bind(PahoTransport)
    with functional_paho(lambda: world.broker):
        try:
            report = run_conformance_suite(
                world.harness('PahoTransport'), severities=(REQUIRED,))
        finally:
            world.close()
    assert report.ok, report.summary()


def test_paho_transport_fails_the_strict_check_and_that_is_the_point():
    """paho DROPS a message that arrives while ``on_message`` is unset.

    This is not a defect being papered over, it is the contract's own hazard being
    stated with evidence.  ``ORSimAgent.start_listening`` subscribes BEFORE it
    assigns ``on_message``, and agents pass ``channel_id=None`` so the receive loop
    is already running.  Under paho the window is microseconds wide -- SUBSCRIBE
    and PUBLISH share one ordered TCP connection -- so nobody has ever lost a
    message to it.  A transport that BLOCKS ``subscribe()`` for a group join, which
    LIFECYCLE CONTRACT point 4 requires of any remote broker, widens that window to
    the join latency; at boot the scheduler is broadcasting to ~1063 agents that
    are all joining at once, and a dropped step broadcast means the agent never
    replies, is counted unresponsive and is pruned.

    If this test ever goes green, either paho changed or the double stopped
    modelling it -- both are worth a failing test.
    """
    world = PahoWorld(SETTINGS, CREDENTIALS)
    world.bind(PahoTransport)
    with functional_paho(lambda: world.broker):
        try:
            report = run_conformance_suite(
                world.harness('PahoTransport'), severities=(REQUIRED, STRICT))
        finally:
            world.close()
    assert report.failed_ids == ['on_message_unset_buffers'], report.summary()


# ---------------------------------------------------------------------------
# The mutants: what makes the suite a specification rather than decoration
# ---------------------------------------------------------------------------


# CrashOnUnsetCallbackTransport reproduces its defect by letting
# ``TypeError: 'NoneType' object is not callable`` escape its receive-loop thread
# -- that escape IS the bug (it is what leaves an agent silently deaf), so the
# traceback pytest reports for it is the mutation working, not a problem.
@pytest.mark.filterwarnings('ignore::pytest.PytestUnhandledThreadExceptionWarning')
@pytest.mark.parametrize(
    'transport_cls, expected_failures',
    [pytest.param(cls, expected, id=cls.__name__)
     for cls, expected in MUTANTS if expected is not None],
)
def test_a_transport_that_breaks_one_rule_is_caught_by_that_rules_check(
        transport_cls, expected_failures):
    """Each mutant fails EXACTLY the checks that own the rule it breaks.

    Exact-set equality, not membership, and in both directions:

    * a check that stops binding shows up as a MISSING id -- the mutant goes green
      and this test fails;
    * a check that over-reaches shows up as an EXTRA id -- so a check cannot buy
      sensitivity by failing on unrelated transports, which is how a suite turns
      into noise nobody reads.

    Where a set holds two ids the second is an unavoidable consequence of the same
    defect (a transport that cannot carry ``'/'`` also cannot demonstrate topic
    fidelity on a ``'/'`` topic); each mutant's class docstring says which and why.

    Every one of these transports instantiates cleanly, satisfies the ABC, is
    accepted by ``Messenger`` and returns normally from every call.  That is the
    whole point: this is the failure class the prose contract cannot catch, and
    the reason more prose was never going to converge.
    """
    world = World(factory=lambda broker: transport_cls(broker), settle=0.6)
    try:
        report = run_conformance_suite(world.harness(transport_cls.__name__))
    finally:
        world.close()
    assert set(report.failed_ids) == expected_failures, report.summary()


def test_every_check_is_bound_by_at_least_one_mutant():
    """No check may exist without a transport that it, and it alone, rejects.

    This is the structural guard against round 1's mistake.  Adding a check without
    a mutant means nobody has demonstrated it can fail, which is indistinguishable
    from a check that cannot fail -- and the second kind is worse than no check at
    all, because it reports green.
    """
    covered = {'interface_members'}          # bound by NoPublishVirtualTransport
    for _, expected in MUTANTS:
        if expected:
            covered |= expected
    assert covered == ALL_CHECK_IDS, (
        'checks with no mutant proving they bind: '
        f'{sorted(ALL_CHECK_IDS - covered)}')


def test_a_virtually_registered_transport_missing_publish_is_named_by_the_first_failure():
    """``MessengerTransport.register()`` bypasses the abstract-method check.

    ``Messenger.__init__``'s ``isinstance`` guard -- whose comment says it turns a
    phase-2 omission into a construction-time ``TypeError`` -- answers ``True`` for
    an ABCMeta VIRTUAL subclass without ever computing ``__abstractmethods__``.  A
    phase-2 adapter that wraps an existing client class instead of subclassing is
    exactly the shape that reaches for ``register()``, and the resulting
    ``AttributeError`` surfaces inside a receive callback where
    ``except Exception: logging.warning; return`` eats it.

    A missing ``publish`` breaks everything downstream, so this mutant fails most
    of the suite.  The assertion that matters is that the report leads with the
    CAUSE: ``interface_members`` runs first and its detail names the member.
    """
    world = World(factory=lambda broker: NoPublishVirtualTransport(broker), settle=0.6)
    try:
        report = run_conformance_suite(world.harness('NoPublishVirtualTransport'))
    finally:
        world.close()
    assert report.failed_ids[0] == 'interface_members', report.summary()
    assert 'publish' in report.results[0].detail
    assert len(report.failed) > 1, (
        'expected the missing member to break downstream checks too; if it does '
        'not, the delivery checks are not really publishing')


# ---------------------------------------------------------------------------
# The harness is part of the contract
# ---------------------------------------------------------------------------


def test_the_loop_probe_is_what_catches_a_leaked_receive_thread():
    """``disconnect()`` that stops DELIVERY but leaks the thread is invisible without it.

    ``disconnect()`` is an agent's entire teardown -- ``Messenger.disconnect`` calls
    ``loop_stop`` only when a ``channel_id`` was given, and agents pass ``None``.  An
    implementor who reads the two names literally closes the consumer in
    ``disconnect()`` and joins the thread in ``loop_stop()``, so nothing arrives any
    more and a poll thread is leaked per agent -- ~1063 per run, into celery workers
    that live across many runs.

    Behaviourally that transport looks perfect.  Only ``loop_is_running`` sees it,
    which is why the harness hook exists and why this test asserts BOTH halves: a
    harness without the probe reports a clean run on a transport that leaks.
    """
    with_probe = World(factory=LeakLoopThreadTransport, settle=0.6)
    try:
        caught = run_conformance_suite(with_probe.harness('LeakLoopThread+probe'))
    finally:
        with_probe.close()
    assert set(caught.failed_ids) == {'disconnect_stops_receive_loop'}, caught.summary()

    without_probe = World(factory=LeakLoopThreadTransport, settle=0.6,
                          expose_loop_probe=False)
    try:
        missed = run_conformance_suite(without_probe.harness('LeakLoopThread-probe'))
    finally:
        without_probe.close()
    assert missed.ok, (
        'the leak is meant to be INVISIBLE without the probe; if this now fails, '
        'say so in the harness docs -- the hook stopped being the only detector.\n'
        + missed.summary())


def test_a_harness_that_routes_is_caught_by_the_anti_cheat():
    """A ``pump()`` that delivers messages itself makes every other check vacuous.

    This is the most likely way to get a green run out of a broken transport, and
    it is easy to do by accident when the transport under test is a recorder: read
    what it published, hand it to the other transports, watch everything pass.

    ``check_broadcast_fanout`` subscribes one transport to a DIFFERENT topic and
    requires that it receives nothing, so a harness fanning everything out to
    everyone fails there instead of certifying the transport.
    """
    transports = []

    def new_transport():
        transport = FakeTransport()
        transports.append(transport)
        return transport

    def routing_pump(deadline):
        # The forbidden thing, done deliberately: the HARNESS performs delivery.
        for source in list(transports):
            published, source.published[:] = list(source.published), []
            for topic, payload in published:
                body = payload.encode('utf-8') if isinstance(payload, str) else payload
                for target in list(transports):
                    if target.on_message is not None:
                        target.on_message(target, None, Message(topic, body))

    report = run_conformance_suite(
        TransportHarness(new_transport=new_transport, pump=routing_pump,
                         name='routing-harness'),
        severities=(REQUIRED,))
    assert 'broadcast_fanout' in report.failed_ids, report.summary()
    assert 'received' in dict(
        (r.id, r.detail) for r in report.failed)['broadcast_fanout']


def test_the_call_recorder_fake_is_reported_as_non_conforming():
    """``tests.fakes.FakeTransport`` is a recorder, not a transport, and the suite says so.

    It is the obvious candidate for "a known-good transport to run the suite
    against" and it is not one: it has no broker, no routing and no delivery -- its
    ``deliver()`` helper invokes ``on_message`` directly whether or not anything ever
    subscribed.  That is exactly right for the assertions it was built for (what did
    ``Messenger`` call, in what order) and it cannot satisfy a DELIVERY contract.

    Making it pass would require the routing harness the test above rejects.  So the
    honest result is asserted instead, and it is a useful one: it shows the suite
    distinguishes "implements the interface" from "moves messages", which is the
    entire distinction phase 2 needs.
    """
    report = run_conformance_suite(
        TransportHarness(new_transport=FakeTransport, pump=lambda deadline: None,
                         name='FakeTransport (recorder)'),
        severities=(REQUIRED,))

    assert not report.ok
    # It satisfies everything that is about SHAPE ...
    assert {'interface_members', 'on_message_reads_back'}.isdisjoint(report.failed_ids)
    # ... and fails everything that is about DELIVERY.
    assert {'broadcast_fanout', 'subscribe_in_effect_on_return', 'topic_fidelity',
            'slash_topics', 'payload_str_in_bytes_out',
            'disconnect_flushes_pending_publishes'}.issubset(report.failed_ids)


# ---------------------------------------------------------------------------
# Suite mechanics
# ---------------------------------------------------------------------------


def test_check_ids_are_unique_and_severities_are_known():
    """Ids are the public handle -- reports, parametrised tests, waivers."""
    ids = [check.id for check in CHECKS]
    assert len(ids) == len(set(ids))
    assert {check.severity for check in CHECKS} <= {REQUIRED, STRICT}
    assert all(check.title for check in CHECKS), 'every check needs a docstring'


def test_one_failing_check_does_not_hide_the_others(loopback_world):
    """Checks are independent: the runner records a failure and carries on.

    A suite that aborts on the first failure turns a transport with three defects
    into three debugging rounds, and phase 2 is exactly the case where several are
    likely at once.
    """
    def exploding(harness):
        """Deliberately broken check."""
        raise RuntimeError('boom')

    from orsim.messenger.conformance import Check, check_broadcast_fanout

    checks = (
        Check('exploder', REQUIRED, 'n/a', exploding),
        Check('broadcast_fanout', REQUIRED, 'DELIVERY SEMANTICS 1',
              check_broadcast_fanout),
    )
    report = run_conformance_suite(loopback_world.harness('Loopback'), checks=checks)
    assert report.failed_ids == ['exploder']
    assert 'boom' in report.results[0].detail
    assert report.results[1].passed


def test_the_report_raises_with_the_whole_summary(loopback_world):
    """``raise_if_failed`` must carry every verdict, not just the first."""
    from orsim.messenger.conformance import Check

    def always_fails(harness):
        """Always fails."""
        raise ConformanceFailure('a distinctive reason')

    report = run_conformance_suite(
        loopback_world.harness('Loopback'),
        checks=(Check('always_fails', REQUIRED, 'n/a', always_fails),))
    with pytest.raises(ConformanceFailure) as excinfo:
        report.raise_if_failed()
    assert 'a distinctive reason' in str(excinfo.value)
    assert 'always_fails' in str(excinfo.value)


def test_the_suite_refuses_a_bare_callable_instead_of_a_harness():
    """A factory is not a harness; without ``pump`` the checks become timing luck."""
    with pytest.raises(TypeError):
        run_conformance_suite(LoopbackTransport)
