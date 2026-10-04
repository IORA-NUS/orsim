"""An EXECUTABLE conformance suite for :class:`~orsim.messenger.transport.MessengerTransport`.

WHY THIS MODULE EXISTS
----------------------
``transport.py`` states the transport contract in English prose: three long
docstring sections (DELIVERY SEMANTICS, LIFECYCLE CONTRACT, PAYLOAD CONTRACT)
plus per-method notes.  That prose is accurate and it is the reason the seam is
implementable at all -- but prose cannot be machine-checked, so every adversarial
review of it finds more gaps, and the count has grown at each pass (5, then 10,
then 16 findings).  Adding prose does not converge; the reviewer and the
implementor are reading the same unenforced text.

This module converts the parts of that contract that are BEHAVIOUR into
executable checks.  A transport is no longer validated by someone reading the
docstrings carefully enough; it is validated by running it::

    from orsim.messenger.conformance import TransportHarness, run_conformance_suite

    harness = TransportHarness(
        new_transport=lambda: KafkaTransport(SETTINGS, CREDENTIALS),
        pump=my_pump,                      # see HARNESS RULES below
        loop_is_running=my_loop_probe,     # optional but recommended
        name='KafkaTransport',
    )
    run_conformance_suite(harness).raise_if_failed()

Every check corresponds to a failure mode that has actually been argued for or
reproduced against this codebase, and every one of them is SILENT: the transport
constructs, ``Messenger`` accepts it, ``subscribe()`` and ``publish()`` return
normally, and the simulation then stalls at ``STEP_TIMEOUT`` per step with no
exception anywhere.  That is the class of bug phase 1 exists to prevent, and it
is precisely the class that "the ABC has six abstract methods and I implemented
all six" does not prevent.

DISPATCH SERIALISATION -- a requirement this module OWNS
-------------------------------------------------------
``transport.py``'s CONCURRENCY CONTRACT says which calls are re-entered from
``on_message`` and which runtime the loop lives in.  It does not say how many
``on_message`` invocations may be in flight AT ONCE, and the framework depends
on the answer being ONE:

    ``openride_scheduler.py::OpenRideScheduler._handle_reply`` -- the hot path
    for every agent reply of every step -- states in its own docstring that it
    "runs on the paho-mqtt thread", and on that basis does ``self._did_step_count
    += 1`` and ``self._outstanding -= 1`` with NO lock.  The class comment above
    those fields spells the assumption out: "Individual set/dict/int operations
    are GIL-atomic".  A read-modify-write is not one operation, so that is only
    true while no two dispatches overlap.  Two concurrent replies lose an
    increment, the barrier's ``_outstanding`` never reaches zero, and the step
    burns the full ``STEP_TIMEOUT`` -- intermittently, under load, with nothing
    logged.

So: **a transport must dispatch ``on_message`` serially -- one message at a
time, never two callbacks concurrently.**  :func:`check_dispatch_is_serialised`
enforces it.  The requirement is stated HERE, executably, rather than only as
prose; ``transport.py``'s CONCURRENCY CONTRACT does not yet carry it in words
and should be amended by whoever owns that file.  What is NOT required is that
every dispatch come from the same OS thread -- see the exclusion list below.

WHAT THIS SUITE DOES **NOT** COVER
----------------------------------
It checks a transport's OBSERVABLE MESSAGING BEHAVIOUR.  Keep this list honest:
an over-trusted green report is this module's own failure mode, and the holes
below were each found by someone writing a transport that scored full marks and
was fatally broken.  It deliberately does not check:

* **Naming, topic legality or broker administration.**  Whether a topic string
  is legal for your broker is a property of the broker, not of the transport's
  observable behaviour.  What the suite CAN check -- and does -- is that a
  transport carries the framework's real topic strings, which all contain
  ``'/'``, and hands them back unchanged.  See :func:`check_slash_topics` and
  :func:`check_topic_fidelity`.
* **Concurrency-runtime fitness.**  ORSim agents run in celery workers started
  with ``--pool eventlet``, ~20-40 agents sharing one OS thread, so a receive
  loop that blocks in a C extension starves every co-resident agent.  That is a
  real and recorded hazard, but it is a property of the process the transport
  runs in, not something a single-process functional suite can observe.  It
  stays prose in ``transport.py``.  :func:`check_reentrancy_from_on_message` is
  the nearest executable proxy: it catches the specific deadlock shape that the
  eventlet single-thread model makes fatal.
* **Load, throughput and ordering under concurrency.**  A transport that passes
  here can still be too slow.
* **The THREAD IDENTITY of dispatch.**  :func:`check_dispatch_is_serialised`
  requires mutual exclusion and nothing more.  Requiring one fixed thread would
  reject a transport that is conforming and likely: CONCURRENCY CONTRACT point 2
  pushes an implementor to BUFFER messages that arrive before ``on_message`` is
  set, and the natural place to release that buffer is the ``on_message``
  setter -- i.e. on the assigning thread.  Serialisation is what the unlocked
  counters above actually need.  If you need same-thread affinity for some other
  reason, that is yours to assert, not this suite's.
* **Subscription changes under concurrent traffic, beyond ONE shape.**
  :func:`check_unsubscribe_does_not_drop_in_flight_traffic` publishes from a
  second transport on a second thread at roughly 1 kHz for a few tens of
  milliseconds while ``unsubscribe()`` executes.  That catches a transient wide
  enough to swallow a publish; it cannot catch one NARROWER than the flood
  interval, and it says nothing about ``subscribe()`` under traffic, about a
  change driven from inside ``on_message`` while a third party publishes, or
  about the same transport being published to from two threads at once.
* **Multi-consumer group dynamics.**  Partition reassignment across a GROUP of
  consumers, a member joining or leaving mid-run, and the delivery gap that a
  cooperative or eager assignor opens for the OTHER members are all invisible
  to a suite whose transports each subscribe for themselves.
  :func:`check_broadcast_fanout` establishes that subscribers are not competing
  consumers; it does not establish that a rebalance is harmless.
* **Ordering ACROSS topics.**  Each check asserts order only within one topic.
  The framework never depends on a cross-topic order, so this is a known and
  accepted gap rather than an oversight -- but do not read a green run as
  licence to assume it.
* **Everything ``Messenger`` does that is not a transport call.**
  :func:`check_messenger_drives_the_transport` drives a real ``Messenger`` for
  the ``channel_id`` shapes that exercise call shapes nothing else reaches
  (``subscribe(topic, qos=0)``, ``loop_stop(force=True)``).  The list-valued
  ``channel_id`` branch is deliberately NOT repeated there: the call it makes is
  byte-identical to :func:`check_subscribe_shapes`'s, and repeating it would
  only make two checks fail for one defect.

So: passing this suite is NECESSARY, not sufficient.  Say that when you quote a
green result.

HARNESS RULES -- READ THIS BEFORE WRITING A HARNESS
---------------------------------------------------
The suite drives the transport through its PUBLIC contract only.  It needs one
thing it cannot do itself: knowing when asynchronous delivery has settled, so
that "the message did not arrive" can be distinguished from "the message has
not arrived YET".  That is :meth:`TransportHarness.pump`.

**``pump()`` may only turn the crank on delivery the transport already
implements.  It must never perform routing, fan-out, subscription filtering,
payload encoding or topic mapping itself.**  A harness that routes makes the
whole suite vacuous: every check then passes because the HARNESS satisfied it,
whatever the transport did.  Concretely, a legitimate ``pump()`` is
"block until this broker double's queues are empty", or "sleep 100 ms", or
"poll the consumer until it reports no lag".  An illegitimate one is
"look at what the transport recorded and deliver it to the other transports".

The suite carries one anti-cheat for exactly that mistake:
:func:`check_broadcast_fanout` includes a transport subscribed to a DIFFERENT
topic and requires that it receives nothing.  A harness that fans everything out
to everyone fails there rather than passing silently.

Two further requirements on the factory:

* ``new_transport()`` returns a FRESH, fully connected transport each time --
  connected by the end of its own ``__init__``, exactly as LIFECYCLE CONTRACT
  point 1 requires.  The suite never calls ``connect()`` or
  ``username_pw_set()``, because the framework never does.
* All transports the factory returns must share one substrate (one broker), or
  :func:`check_broadcast_fanout` cannot mean anything.

The suite calls ``loop_start()`` itself, mirroring ``Messenger.__init__``, and
calls ``disconnect()`` on every transport it created when a check ends.

SEVERITY LEVELS
---------------
``REQUIRED``
    Every transport must pass.  A failure here is a transport that will stall
    the simulation.
``STRICT``
    Required of any transport whose ``subscribe()`` costs a network round trip
    -- i.e. every remote-broker transport, which is the whole point of phase 2.

    There is exactly one strict check today, :func:`check_on_message_unset_buffers`,
    and it exists because of a real tension worth stating plainly rather than
    hiding behind a severity label.  ``transport.py``'s CONCURRENCY CONTRACT
    point 2 now requires buffering UNCONDITIONALLY ("Dropping them is not a
    tolerable choice here").  ``PahoTransport`` does not buffer -- paho drops an
    inbound message when ``on_message`` is unset -- so the incumbent violates the
    rule as written, and has done so harmlessly for the life of the project
    because the window is microseconds wide: SUBSCRIBE and PUBLISH share one
    ordered TCP connection.  A transport that BLOCKS ``subscribe()`` for a group
    join, which LIFECYCLE CONTRACT point 4 requires of a remote broker, widens
    that window to the join latency and the same behaviour stops being benign.

    Marking it STRICT rather than REQUIRED is therefore a deliberate,
    documented grandfathering of one implementation, not a softening of the
    rule: run ``severities=(REQUIRED, STRICT)`` for anything that is not paho.
    ``tests/test_conformance_suite.py`` asserts BOTH halves -- that
    ``PahoTransport`` passes every required check, and that it fails this one for
    paho's actual reason -- so the exemption stays visible instead of decaying
    into an assumption.

TOPIC CHOICE IS DELIBERATE
--------------------------
Most checks use slash-free topics (``conformance_topic_a`` and friends) even
though every real ORSim topic contains ``'/'``.  That is not carelessness: it
means a transport whose broker cannot carry ``'/'`` fails exactly the two checks
that are ABOUT that (:func:`check_slash_topics`, :func:`check_topic_fidelity`)
instead of failing all twenty, so the report names the actual defect instead
of reporting a transport that is uniformly dead.

This module is pure stdlib and imports nothing broker-specific, so a consumer
can import it without pulling in paho, Kafka or a test framework.  It raises
:class:`ConformanceFailure` (an ``AssertionError`` subclass) rather than using
``assert``, so it keeps working under ``python -O``.
"""

import json
import threading
import time
import traceback
from collections import Counter
from contextlib import contextmanager

from .transport import MessengerTransport


__all__ = [
    'ConformanceFailure',
    'TransportHarness',
    'Check',
    'CheckResult',
    'ConformanceReport',
    'CHECKS',
    'REQUIRED',
    'STRICT',
    'run_conformance_suite',
]


REQUIRED = 'required'
STRICT = 'strict'

#: The six members ``MessengerTransport`` declares abstract -- "what the
#: framework actually invokes".  Named here rather than read from
#: ``__abstractmethods__`` because a class registered as a VIRTUAL subclass
#: (``MessengerTransport.register(cls)``) satisfies ``isinstance`` while
#: defining none of them, and that is one of the things this suite catches.
MANDATORY_MEMBERS = (
    'disconnect', 'loop_start', 'loop_stop', 'publish', 'subscribe', 'unsubscribe',
)

# Slash-free topics for the mechanics checks -- see TOPIC CHOICE IS DELIBERATE.
TOPIC_A = 'conformance_topic_a'
TOPIC_B = 'conformance_topic_b'
TOPIC_C = 'conformance_topic_c'
TOPIC_REPLY = 'conformance_topic_reply'
TOPIC_ELSEWHERE = 'conformance_topic_elsewhere'

# The real shapes, built the way orsim_scheduler.py, orsim_agent.py and
# orsim_app.py build them.  Used only where the '/' is the point.
BROADCAST_TOPIC = 'run_20260911_120000/scheduler_1/ORSimAgent'
SCHEDULER_TOPIC = 'run_20260911_120000/scheduler_1/ORSimScheduler'
AGENT_TOPIC = 'run_20260911_120000/truck_0007'

DEFAULT_SETTLE_SECONDS = 2.0

# How many times :func:`_settle_until` will pump before giving up.  See its
# docstring: it exists because a fixed-sleep pump() is legal.
SETTLE_ATTEMPTS = 4

# check_unsubscribe_does_not_drop_in_flight_traffic: how hard, and for how long,
# traffic flows on an untouched topic while a subscription change executes.  The
# warm-up exists so the flood is demonstrably IN FLIGHT before the change
# starts; the interval sets the narrowest transient the check can see.
IN_FLIGHT_PUBLISH_INTERVAL = 0.001
IN_FLIGHT_WARMUP_SECONDS = 0.02
# ... and for how long it keeps flowing AFTER the change returns.  Without a
# cooldown, whether a transport that destroys the whole subscription set loses a
# message here depends on a microsecond race, which would make this check's
# verdict on that transport nondeterministic.
IN_FLIGHT_COOLDOWN_SECONDS = 0.02

# check_dispatch_is_serialised: how long the first callback holds the dispatcher
# open (a cap, released early in the normal path) and how long the check then
# waits for a second callback to overlap it.
DISPATCH_HOLD_SECONDS = 0.5
DISPATCH_OVERLAP_WINDOW_SECONDS = 0.15


class ConformanceFailure(AssertionError):
    """A transport violated a contract requirement.

    Subclasses ``AssertionError`` so a consumer's pytest run reports it the way
    it reports any other failed expectation, while still being catchable by
    type when the suite is driven programmatically.
    """


# ---------------------------------------------------------------------------
# Harness
# ---------------------------------------------------------------------------


class TransportHarness:
    """Everything the suite needs from a consumer, and nothing more.

    Args:
        new_transport (callable): zero-argument factory returning a FRESH,
            already-connected transport.  Every transport it returns must share
            one broker/substrate.
        pump (callable, optional): ``pump(deadline_seconds)`` -- block until
            delivery that is already in flight has settled, or until the
            deadline passes.  See this module's HARNESS RULES: it may drive
            delivery, never implement it.  Defaults to a fixed
            ``time.sleep(0.1)``, which is enough for a transport that delivers
            synchronously and NOT enough for most real ones.
        loop_is_running (callable, optional): ``loop_is_running(transport)`` ->
            ``True``/``False``/``None``.  ``None`` means "cannot tell", and the
            checks that would use it degrade to their observable-behaviour form.
            Supplying it is what turns :func:`check_disconnect_stops_receive_loop`
            from "delivery stopped" into "delivery stopped AND the thread is
            gone" -- the leak the ``disconnect()`` docstring warns about is
            invisible without it.
        name (str, optional): what to call this transport in the report.
        settle (float, optional): default pump deadline, seconds.
        on_check_start (callable, optional): ``on_check_start(check)``, invoked
            by :func:`run_conformance_suite` before each check.  Checks reuse a
            small set of topic names, so a harness whose substrate keeps state
            between checks (a real broker, or an in-memory double) should use
            this to hand out a clean one -- otherwise a transport that replays
            history leaks one check's traffic into the next and the report
            blames the wrong semantic.  Optional because a conforming transport
            does not need it: delivery is forward-only, so a fresh transport
            subscribing in check N never sees check N-1's messages.
    """

    def __init__(self, new_transport, pump=None, loop_is_running=None,
                 name=None, settle=DEFAULT_SETTLE_SECONDS, on_check_start=None):
        if not callable(new_transport):
            raise TypeError('new_transport must be a zero-argument callable')
        self._new_transport = new_transport
        self._pump = pump
        self._loop_is_running = loop_is_running
        self._on_check_start = on_check_start
        self.name = name or getattr(new_transport, '__name__', 'transport')
        self.settle = settle

    def new_transport(self):
        """Return a fresh, connected transport."""
        return self._new_transport()

    def pump(self, deadline=None):
        """Block until in-flight delivery has settled (or the deadline passes)."""
        if self._pump is None:
            time.sleep(0.1)
            return
        self._pump(self.settle if deadline is None else deadline)

    def loop_is_running(self, transport):
        """``True``/``False`` if the harness can tell, else ``None``."""
        if self._loop_is_running is None:
            return None
        return self._loop_is_running(transport)

    def begin_check(self, check):
        """Tell the harness a new check is starting (see ``on_check_start``)."""
        if self._on_check_start is not None:
            self._on_check_start(check)


class _Inbox:
    """An ``on_message`` callback that records what was delivered.

    Signature is paho's ``(client, userdata, message)``; neither of ORSim's two
    real callbacks reads the first two arguments, and neither does this.
    """

    def __init__(self):
        self.messages = []

    def __call__(self, client, userdata, message):
        self.messages.append(message)

    @property
    def topics(self):
        return [m.topic for m in self.messages]

    @property
    def payloads(self):
        return [m.payload for m in self.messages]

    def decoded(self):
        """Every payload JSON-decoded exactly the way ORSim's handlers do."""
        return [json.loads(m.payload.decode('utf-8')) for m in self.messages]


class _Session:
    """Tracks the transports a check created so they are all torn down."""

    def __init__(self, harness):
        self.harness = harness
        self.transports = []

    def transport(self, on_message=None, start=True, subscribe=None):
        t = self.harness.new_transport()
        self.transports.append(t)
        if on_message is not None:
            t.on_message = on_message
        if start:
            t.loop_start()
        if subscribe is not None:
            t.subscribe(subscribe)
        return t

    def close(self):
        for t in reversed(self.transports):
            try:
                t.disconnect()
            except Exception:  # teardown must never mask the check's verdict
                pass


@contextmanager
def _session(harness):
    session = _Session(harness)
    try:
        yield session
    finally:
        session.close()


def _require(condition, message):
    if not condition:
        raise ConformanceFailure(message)


def _body(n):
    return json.dumps({'seq': n})


def _settle_until(harness, predicate, attempts=SETTLE_ATTEMPTS):
    """``pump()`` until ``predicate()`` holds, or the attempts run out.

    A conforming ``pump()`` blocks until in-flight delivery has settled, so the
    first call is always enough and the extra attempts cost nothing.  But the
    HARNESS RULES deliberately permit a weaker one -- "sleep 100 ms" is listed as
    legitimate, and the phase-2 Kafka harness really is ``time.sleep(min(deadline,
    0.4))`` -- and a fixed sleep gives delivery exactly ONE window.  A check that
    pumps once then asserts is betting its whole verdict on that window being wide
    enough on the day, against a shared broker, which is a flake rather than a
    finding.  (Observed: one grammar-mode Kafka run reported zero delivery on a
    step that passed on the next two runs and in isolation.)

    This does NOT weaken anything: a transport that never delivers still fails,
    and the predicate is evaluated unchanged.  Use it for any assertion in a
    MULTI-STEP check, where the interesting window is the last of several.
    """
    for _ in range(max(1, attempts)):
        harness.pump()
        if predicate():
            return True
    return False


def _seqs(inbox):
    """The ``seq`` field of every delivered payload.

    Deliberately LENIENT about the payload type: it accepts ``str`` as well as
    ``bytes``.  The bytes requirement has exactly one owner,
    :func:`check_payload_str_in_bytes_out`, so that a transport which hands back
    a ``str`` fails THAT check by name instead of failing nine delivery checks
    that are really about something else.  A report that names one defect is
    worth more than one that says everything is broken.
    """
    out = []
    for message in inbox.messages:
        payload = message.payload
        if isinstance(payload, (bytes, bytearray)):
            payload = bytes(payload).decode('utf-8')
        out.append(json.loads(payload)['seq'])
    return out


# ---------------------------------------------------------------------------
# The checks
# ---------------------------------------------------------------------------


def check_interface_members(harness):
    """The six framework-called members exist and are callable.

    ``Messenger.__init__``'s ``isinstance`` guard is satisfied by an ABCMeta
    VIRTUAL subclass (``MessengerTransport.register(cls)``), which performs no
    ``__abstractmethods__`` check -- so a transport can pass construction while
    missing ``publish`` entirely, and the AttributeError then surfaces inside a
    receive callback where ``except Exception: logging.warning; return`` eats it.
    """
    with _session(harness) as s:
        t = s.transport(start=False)
        _require(
            isinstance(t, MessengerTransport),
            'the factory returned an object that is not a MessengerTransport; '
            'Messenger.__init__ rejects it with TypeError.',
        )
        missing = [n for n in MANDATORY_MEMBERS if not callable(getattr(t, n, None))]
        _require(
            not missing,
            f'missing or non-callable mandatory member(s): {missing}. These six '
            'are exactly what the framework invokes.',
        )


def check_broadcast_fanout(harness):
    """Every subscriber of a topic gets its OWN COPY of every message.

    ``ORSimScheduler.step`` publishes ONE message to the broadcast topic and
    then waits for a reply from EVERY agent subscribed to it.  Under
    competing-consumer delivery -- Kafka's default with a shared ``group.id``,
    the shape every tutorial shows -- one agent of ~1063 receives the step, the
    barrier never resolves and every step burns the full ``STEP_TIMEOUT``.

    Also the suite's anti-cheat: ``bystander`` subscribes to a different topic
    and must receive nothing.  A harness that does the routing itself, instead
    of driving routing the transport implements, fails here.
    """
    with _session(harness) as s:
        first, second, bystander = _Inbox(), _Inbox(), _Inbox()
        s.transport(on_message=first, subscribe=TOPIC_A)
        s.transport(on_message=second, subscribe=TOPIC_A)
        s.transport(on_message=bystander, subscribe=TOPIC_ELSEWHERE)
        publisher = s.transport()
        harness.pump()

        for n in range(3):
            publisher.publish(TOPIC_A, _body(n))
        harness.pump()

        for label, inbox in (('first', first), ('second', second)):
            _require(
                _seqs(inbox) == [0, 1, 2],
                f'DELIVERY SEMANTICS: the {label} subscriber of {TOPIC_A!r} received '
                f'{_seqs(inbox)} of the 3 messages published to it. Subscribers are '
                'BROADCAST receivers, not competing consumers sharing a work queue.',
            )
        _require(
            bystander.messages == [],
            f'a transport subscribed only to {TOPIC_ELSEWHERE!r} received '
            f'{len(bystander.messages)} message(s) published to {TOPIC_A!r}. Either '
            'the transport ignores its subscription set, or the HARNESS is doing the '
            'routing -- see this module\'s HARNESS RULES, a routing harness makes '
            'every other check vacuous.',
        )


def check_subscribe_is_additive_and_idempotent(harness):
    """``subscribe`` ADDS to the subscription set and never replaces it.

    Kafka's ``consumer.subscribe(topics)`` does the opposite.  Against a
    replacing transport an agent's nap/wake cycle
    (``OpenRideAgent._unsubscribe_broadcast`` then ``::_resubscribe_broadcast``,
    which re-adds the broadcast topic ALONE) ends with the agent's own app topic
    gone -- and the wake path is itself driven by an app-topic event, so the
    agent can never be woken again, for the rest of the run, with no error.

    Re-subscribing to a topic already held is a no-op, not a second delivery.
    """
    with _session(harness) as s:
        inbox = _Inbox()
        t = s.transport(on_message=inbox)
        publisher = s.transport()

        t.subscribe(TOPIC_A)
        harness.pump()
        t.subscribe(TOPIC_B)
        harness.pump()
        t.subscribe(TOPIC_A)          # idempotent: must not double-deliver
        harness.pump()

        publisher.publish(TOPIC_A, _body(1))
        publisher.publish(TOPIC_B, _body(2))
        harness.pump()

        counts = Counter(inbox.topics)
        _require(
            counts.get(TOPIC_A, 0) >= 1 and counts.get(TOPIC_B, 0) >= 1,
            f'subscribe is not ADDITIVE: after subscribe({TOPIC_A!r}) then '
            f'subscribe({TOPIC_B!r}), delivery was {dict(counts)}. A later subscribe '
            'must not cancel an earlier one.',
        )
        _require(
            counts.get(TOPIC_A, 0) == 1,
            f'subscribe is not IDEMPOTENT: subscribing to {TOPIC_A!r} twice produced '
            f'{counts.get(TOPIC_A, 0)} copies of one published message. Every ORSim '
            'handler is written for exactly-one delivery; a duplicate step broadcast '
            'makes an agent reply twice for one time step.',
        )


def check_unsubscribe_removes_exactly_one_topic(harness):
    """``unsubscribe(topic)`` removes that topic and leaves the others live.

    Its only caller, ``OpenRideAgent._unsubscribe_broadcast``, drops the step
    broadcast when an agent naps and depends on the agent's OWN app topic still
    delivering -- that is what wakes it up again.

    SCOPE, stated because this check used to claim more than it does.  It
    publishes BEFORE and AFTER the subscription change, never DURING it, so it
    observes only the END STATE.  A transport that re-declares its whole
    subscription set on every change -- drop everything, then re-add what is
    still wanted, which is what an eager Kafka assignor does -- reaches the
    right end state and passes here while losing anything published inside its
    rebalance window.  That TRANSIENT is
    :func:`check_unsubscribe_does_not_drop_in_flight_traffic`'s job, and it is a
    separate check precisely because this one cannot see it.
    """
    with _session(harness) as s:
        inbox = _Inbox()
        t = s.transport(on_message=inbox)
        publisher = s.transport()

        t.subscribe(TOPIC_A)
        t.subscribe(TOPIC_B)
        harness.pump()
        t.unsubscribe(TOPIC_A)
        harness.pump()

        publisher.publish(TOPIC_A, _body(1))
        publisher.publish(TOPIC_B, _body(2))
        harness.pump()

        counts = Counter(inbox.topics)
        _require(
            counts.get(TOPIC_A, 0) == 0,
            f'unsubscribe({TOPIC_A!r}) did not take effect: {counts.get(TOPIC_A, 0)} '
            'message(s) still delivered on it.',
        )
        _require(
            counts.get(TOPIC_B, 0) == 1,
            f'unsubscribe({TOPIC_A!r}) also disturbed {TOPIC_B!r}, which received '
            f'{counts.get(TOPIC_B, 0)} of 1 message. Removing one topic must not '
            'perturb delivery on the others.',
        )


def check_resubscribe_after_unsubscribe(harness):
    """A topic UNSUBSCRIBED and then SUBSCRIBED AGAIN delivers again.

    This is the nap/wake cycle, the single call pattern the contract cites most,
    and until an adversarial review of phase 1 went looking for it nothing in
    this suite exercised it: every other check subscribes to a topic at most
    once.  The pattern in the app is two lines of
    ``apps/container_logistics/openride_agent.py``: ``_unsubscribe_broadcast``
    (line 112) drops the step broadcast when an agent naps, and
    ``_resubscribe_broadcast`` (line 120) re-adds that same topic on wake, then
    sets ``self._broadcast_subscribed = True``.  There is no retry, because a
    normal return is taken as proof the subscription is live.

    So a transport whose ``unsubscribe()`` poisons a topic -- a dead-set, a
    tombstone, a consumer closed per topic and never reopened -- returns
    normally from the re-subscribe and activates nothing.  Every agent that ever
    reported ``sleep_steps_hint() == -1`` is then permanently deaf to the step
    broadcast, is counted unresponsive and is pruned.  On a 1000-truck run that
    is most of the fleet, arriving gradually, with no error anywhere.

    Membership rather than exact sequence, deliberately: a transport that
    REPLAYS history on subscribe also re-delivers the earlier message here, and
    that defect belongs to :func:`check_forward_only_delivery`.
    """
    with _session(harness) as s:
        inbox = _Inbox()
        t = s.transport(on_message=inbox)
        publisher = s.transport()

        t.subscribe(TOPIC_A)                  # the step broadcast
        t.subscribe(TOPIC_B)                  # the agent's own app topic
        harness.pump()

        publisher.publish(TOPIC_A, _body(1))
        publisher.publish(TOPIC_B, _body(2))
        harness.pump()
        _require(
            {1, 2} <= set(_seqs(inbox)),
            f'setup failed: with both {TOPIC_A!r} and {TOPIC_B!r} subscribed, '
            f'delivery was {_seqs(inbox)} and should contain 1 and 2. subscribe() '
            'is not additive -- see the subscribe_is_additive_and_idempotent '
            'check, which owns that rule.',
        )

        t.unsubscribe(TOPIC_A)                # the nap
        harness.pump()
        t.subscribe(TOPIC_A)                  # the wake
        harness.pump()

        publisher.publish(TOPIC_A, _body(3))
        publisher.publish(TOPIC_B, _body(4))
        _settle_until(harness, lambda: {3, 4} <= set(_seqs(inbox)))

        delivered = set(_seqs(inbox))
        _require(
            4 in delivered,
            f'after unsubscribe({TOPIC_A!r}) then subscribe({TOPIC_A!r}), the '
            f'UNTOUCHED topic {TOPIC_B!r} stopped delivering (got '
            f'{sorted(delivered)}). A nap/wake cycle must not cost the agent its '
            'own app topic -- the wake path is itself driven by an app-topic '
            'event, so an agent that loses it can never be woken again.',
        )
        _require(
            3 in delivered,
            f'a topic unsubscribed and then subscribed again did not deliver (got '
            f'{sorted(delivered)}, expected 3 to be present). unsubscribe() must '
            'not be a one-way door: OpenRideAgent._resubscribe_broadcast re-adds '
            'the step broadcast on every wake and takes a normal return as proof '
            'it is live, so it never retries. An agent that naps once is then '
            'deaf for the rest of the run and is pruned as unresponsive.',
        )


def check_unsubscribe_does_not_drop_in_flight_traffic(harness):
    """A subscription change must not disturb other topics -- not even TRANSIENTLY.

    :func:`check_unsubscribe_removes_exactly_one_topic` checks the END STATE and
    therefore cannot see this.  An ``unsubscribe()`` that re-declares the whole
    subscription set -- drop everything, then re-add what is still wanted, which
    is exactly what Kafka's EAGER assignor does on a rebalance -- lands on the
    right end state and loses every message published to the untouched topics
    while the window is open.

    That window is where the agent's wake event lives.  ``OpenRideAgent``
    unsubscribes from the step broadcast the instant an agent naps, and the very
    next thing the scheduler may do is publish to that agent's OWN topic to wake
    it.  A wake notice lost there is not redelivered: the agent sleeps for the
    rest of the run.

    How it is done: a second transport publishes to the untouched topic from a
    second thread, continuously, while ``unsubscribe()`` executes on this one.
    Every message the publisher accepted must arrive.  Two things worth knowing
    before reading a failure here -- the publisher is a SEPARATE transport, so
    this does not require one transport to be publish-safe from two threads at
    once, and the check can only see a transient WIDER than
    ``IN_FLIGHT_PUBLISH_INTERVAL``.
    """
    with _session(harness) as s:
        inbox = _Inbox()
        t = s.transport(on_message=inbox)
        publisher = s.transport()
        t.subscribe(TOPIC_A)                  # the topic that gets removed
        t.subscribe(TOPIC_B)                  # the topic that must not notice
        harness.pump()

        sent = []
        publish_errors = []
        stop = threading.Event()

        def flood():
            seq = 0
            while not stop.is_set():
                seq += 1
                try:
                    publisher.publish(TOPIC_B, _body(seq))
                except Exception as exc:      # noqa: BLE001 - reported verbatim
                    publish_errors.append(exc)
                    return
                sent.append(seq)
                time.sleep(IN_FLIGHT_PUBLISH_INTERVAL)

        flooder = threading.Thread(target=flood, daemon=True,
                                   name='conformance-in-flight-publisher')
        flooder.start()
        try:
            # Prove the traffic is genuinely in flight BEFORE the change starts,
            # so a failure cannot be read as "the flood had not begun yet", and
            # keep it flowing AFTER the call returns so the verdict never turns
            # on a microsecond race at the boundary.
            time.sleep(IN_FLIGHT_WARMUP_SECONDS)
            t.unsubscribe(TOPIC_A)
            time.sleep(IN_FLIGHT_COOLDOWN_SECONDS)
        finally:
            stop.set()
            flooder.join(timeout=5.0)
        # Both of these are decided before settling: there is nothing to wait
        # for if the publisher never got off the ground, and a 4-attempt pump on
        # a predicate that cannot hold is just lost time.
        if publish_errors:
            raise ConformanceFailure(
                'publishing from a second thread while unsubscribe() ran on '
                f'another raised {publish_errors[0]!r}. publish() is already '
                'required to be callable from the receive-loop thread '
                '(CONCURRENCY CONTRACT 1); a transport the scheduler cannot '
                'publish through while an agent changes its subscriptions cannot '
                'carry this framework.')
        _require(
            sent,
            'setup failed: not one message was published during the window, so '
            'nothing about the transient was observed. Raise '
            'IN_FLIGHT_WARMUP_SECONDS or check that publish() is not blocking.')

        _settle_until(harness, lambda: set(sent) <= set(_seqs(inbox)))

        got = set(_seqs(inbox))
        missing = sorted(set(sent) - got)
        _require(
            not missing,
            f'{len(missing)} of the {len(sent)} message(s) published to the '
            f'UNTOUCHED topic {TOPIC_B!r} while unsubscribe({TOPIC_A!r}) was '
            f'executing never arrived (missing seq {missing[:10]}'
            f'{" ..." if len(missing) > 10 else ""}). Removing one topic must not '
            'open a delivery gap on the others. If your consumer re-declares its '
            'whole subscription set on every change -- an eager rebalance -- the '
            'end state is right and the transient is fatal: the scheduler wakes a '
            'napping agent by publishing to that agent\'s own topic, and a wake '
            'notice lost here is never redelivered.',
        )


def check_subscribe_shapes(harness):
    """Both live call shapes work: ``subscribe(str, qos)`` and ``subscribe([(t, q)])``.

    ``Messenger.__init__`` uses the string form, ``ORSimAgent.start_listening``
    passes a bare list of ``(topic, qos)`` tuples, and
    ``OpenRideAgent._resubscribe_broadcast`` uses the string form again.  A
    transport that handles only one of them subscribes to ``str(list)`` -- a
    topic nothing ever publishes to -- and receives nothing, silently.
    """
    with _session(harness) as s:
        inbox = _Inbox()
        t = s.transport(on_message=inbox)
        publisher = s.transport()

        t.subscribe(TOPIC_A)
        harness.pump()
        t.subscribe([(TOPIC_B, 0), (TOPIC_C, 0)])
        harness.pump()

        for n, topic in enumerate((TOPIC_A, TOPIC_B, TOPIC_C)):
            publisher.publish(topic, _body(n))
        harness.pump()

        _require(
            sorted(inbox.topics) == sorted([TOPIC_A, TOPIC_B, TOPIC_C]),
            'both subscribe shapes must work: after subscribe(str) and '
            'subscribe([(topic, qos), ...]) delivery was '
            f'{sorted(inbox.topics)}, expected all three topics.',
        )


def check_subscribe_in_effect_on_return(harness):
    """A subscription is live BY THE TIME ``subscribe()`` RETURNS.

    Note there is deliberately no ``pump()`` between ``subscribe()`` and the
    publish here -- that is the whole check.  ``ORSimAgent.start_listening``
    subscribes, attaches ``on_message`` and then IMMEDIATELY publishes its
    ``'ready'`` reply and takes its first step.  Kafka's ``consumer.subscribe()``
    is a purely local declaration -- group join and partition assignment happen
    on a later ``poll()``, hundreds of milliseconds afterwards -- so a transport
    that defers activation announces itself ready, misses the next step
    broadcasts, is counted unresponsive and is pruned.
    """
    with _session(harness) as s:
        inbox = _Inbox()
        t = s.transport(on_message=inbox)
        publisher = s.transport()
        harness.pump()                       # settle everything BEFORE the subscribe

        t.subscribe(TOPIC_A)                 # no pump here: on purpose
        publisher.publish(TOPIC_A, _body(1))
        harness.pump()

        _require(
            _seqs(inbox) == [1],
            'a message published immediately after subscribe() returned was not '
            f'delivered (got {_seqs(inbox)}). LIFECYCLE CONTRACT point 4: if '
            'activation costs a round trip, subscribe() must BLOCK for it -- and it '
            'must do so without depending on progress of its own receive loop, '
            'because subscribe() is also re-entered from inside on_message.',
        )


def check_topic_fidelity(harness):
    """``message.topic`` is BYTE-IDENTICAL to the string passed to ``publish()``.

    Both consumers dispatch on exact string equality with NO else branch:
    ``ORSimAgent.on_receive_message`` does ``self.message_handlers.get(message.topic)``
    and ``ORSimScheduler.on_receive_message`` does
    ``if message.topic == f'{run_id}/{scheduler_id}/ORSimScheduler'``.

    This is the check that catches the obvious remedy for a broker that forbids
    ``'/'``: sanitising to ``'.'`` on the way out and handing back the sanitised
    name on the way in.  Every message then arrives, is JSON-decoded, updates
    the payload cache -- and is dropped by a handler lookup that misses, with
    nothing logged.
    """
    with _session(harness) as s:
        for topic in (BROADCAST_TOPIC, AGENT_TOPIC, TOPIC_A):
            inbox = _Inbox()
            t = s.transport(on_message=inbox)
            publisher = s.transport()
            t.subscribe(topic)
            harness.pump()
            publisher.publish(topic, _body(1))
            harness.pump()

            _require(
                len(inbox.messages) == 1,
                f'nothing was delivered on {topic!r} (got {len(inbox.messages)} '
                'messages), so topic fidelity cannot be established.',
            )
            got = inbox.messages[0].topic
            _require(
                isinstance(got, str) and got == topic,
                f'message.topic came back as {got!r}, not the {topic!r} that was '
                'published. Both ORSim handlers dispatch on exact string equality '
                'with no else branch, so a rewritten topic is silently discarded: '
                'the agent never replies and the step times out.',
            )


def check_slash_topics(harness):
    """Topics containing ``'/'`` are carried -- every real ORSim topic does.

    The framework builds ``f'{run_id}/{scheduler_id}/ORSimAgent'``,
    ``f'{run_id}/{scheduler_id}/ORSimScheduler'`` and one
    ``f'{run_id}/{manager.get_id()}'`` per agent, so a 1000-truck run needs
    ~1060 of them and they are created implicitly by first use.  Kafka forbids
    ``'/'`` in a topic name and rejects it only through an ASYNCHRONOUS delivery
    report -- ``produce()`` and ``subscribe()`` both return normally -- so a
    transport can boot every agent, accept every publish, and move not one
    message.

    This check asserts only that delivery HAPPENS; the topic string itself is
    :func:`check_topic_fidelity`'s job.  A transport that maps ``'/'`` to
    something its broker accepts and maps it back passes both.
    """
    # One subscriber per topic, one subscribe() call each, string form only:
    # this check must fail for a transport that cannot carry '/' and for no other
    # reason, so it deliberately exercises neither the additive property nor the
    # list-of-tuples shape -- those have their own checks.
    with _session(harness) as s:
        publisher = s.transport()
        inboxes = {}
        for n, topic in enumerate((BROADCAST_TOPIC, SCHEDULER_TOPIC, AGENT_TOPIC), 1):
            inbox = _Inbox()
            inboxes[topic] = (n, inbox)
            subscriber = s.transport(on_message=inbox)
            subscriber.subscribe(topic)
        harness.pump()

        for topic, (n, _) in inboxes.items():
            publisher.publish(topic, _body(n))
        harness.pump()

        delivered = {topic: _seqs(inbox) for topic, (n, inbox) in inboxes.items()}
        expected = {topic: [n] for topic, (n, _) in inboxes.items()}
        _require(
            delivered == expected,
            f'a topic containing "/" did not carry its message: {delivered}, '
            f'expected {expected}. EVERY topic the framework uses contains "/" -- '
            'the two scheduler topics plus one per agent, ~1060 of them on a '
            '1000-truck run, all created implicitly by first use. If your broker '
            'forbids the character, map it injectively on publish, subscribe AND '
            'unsubscribe, and map it back before on_message.',
        )


def check_payload_str_in_bytes_out(harness):
    """``publish()`` is handed ``str``; ``message.payload`` arrives as ``bytes``.

    All 12 call sites pass ``json.dumps(...)`` output, and both handlers do
    ``json.loads(message.payload.decode('utf-8'))``.  Handing back the ``str``
    raises ``AttributeError: 'str' object has no attribute 'decode'`` -- which
    kills the agent's receive thread on its first message and, on the OpenRide
    scheduler side, is swallowed by ``except Exception`` so that every reply
    vanishes and every step times out.
    """
    with _session(harness) as s:
        inbox = _Inbox()
        s.transport(on_message=inbox, subscribe=TOPIC_A)
        publisher = s.transport()
        harness.pump()

        sent = json.dumps({'seq': 1, 'unicode': 'depot_é', 'nested': {'a': [1, 2]}})
        publisher.publish(TOPIC_A, sent)
        harness.pump()

        _require(len(inbox.messages) == 1,
                 f'expected 1 message, got {len(inbox.messages)}')
        payload = inbox.messages[0].payload
        _require(
            isinstance(payload, (bytes, bytearray)),
            f'message.payload is {type(payload).__name__}, must be bytes. publish() '
            'takes a str and the round trip is ASYMMETRIC -- a transport whose wire '
            "format is bytes must encode here, because both handlers call .decode().",
        )
        _require(
            bytes(payload).decode('utf-8') == sent,
            'the payload did not round-trip: '
            f'{bytes(payload).decode("utf-8", "replace")!r} != {sent!r}',
        )


def check_forward_only_delivery(harness):
    """A subscriber sees what is published AFTER its ``subscribe()``, and nothing earlier.

    MQTT has no history to replay (retained messages are unused in ORSim).  A
    Kafka consumer with a committed offset does the opposite: on a celery worker
    restart mid-run, a rejoining agent replays step broadcasts for time steps
    already past and re-replies for them, corrupting
    ``agent_collection[...]['step_response']`` for steps the scheduler has closed.
    """
    with _session(harness) as s:
        # `warm` exists so the topic has a live subscriber before the history is
        # written -- a broker that discards a message with no subscribers would
        # otherwise leave nothing for a replaying transport to replay, and the
        # check would pass for the wrong reason.
        s.transport(on_message=_Inbox(), subscribe=TOPIC_A)
        publisher = s.transport()
        harness.pump()

        publisher.publish(TOPIC_A, _body(1))     # history, before `late` subscribes
        harness.pump()

        inbox = _Inbox()
        late = s.transport(on_message=inbox)
        late.subscribe(TOPIC_A)
        harness.pump()
        publisher.publish(TOPIC_A, _body(2))
        harness.pump()

        _require(
            _seqs(inbox) == [2],
            f'a subscriber received {_seqs(inbox)}, expected [2] only. Delivery is '
            'FORWARD-ONLY from the moment of subscribe: a transport must start at '
            'the latest offset and must not commit or restore offsets across a '
            'restart.',
        )


def check_on_message_reads_back(harness):
    """A read of ``transport.on_message`` returns the callback last assigned.

    ``MessengerTransport`` declares ``on_message = None`` as a CLASS attribute so
    that plain assignment works on any transport -- and that class attribute
    shadows ``__getattr__``.  So a transport that forwards only the WRITE (through
    ``__setattr__``, to a wrapped client) reads back ``None`` inside its own
    receive loop and drops every inbound message with nothing raised.
    ``PahoTransport`` avoids this with a read-through property; a phase-2 adapter
    wrapping an existing client class is the shape most likely to reintroduce it.
    """
    with _session(harness) as s:
        t = s.transport(start=False)
        _require(
            getattr(t, 'on_message', 'MISSING') is None,
            'a fresh transport must read on_message as None, got '
            f'{getattr(t, "on_message", "MISSING")!r}',
        )

        def callback(client, userdata, message):
            pass

        t.on_message = callback
        _require(
            t.on_message is callback,
            'transport.on_message did not read back the callback that was assigned '
            f'(got {t.on_message!r}). Forwarding the write alone is not enough: '
            'provide a read-through property.',
        )
        t.on_message = None
        _require(t.on_message is None, 'on_message could not be cleared')


def check_on_message_unset_does_not_break_the_loop(harness):
    """A message arriving while ``on_message`` is ``None`` must not kill the receive loop.

    ``ORSimAgent.start_listening`` subscribes BEFORE it assigns ``on_message``,
    and agents build their Messenger with ``channel_id=None`` so the receive loop
    is already running by then.  A transport that calls ``self.on_message(...)``
    unguarded raises ``TypeError: 'NoneType' object is not callable`` on its poll
    thread, the thread dies, and the agent is silently deaf for the rest of the
    run.

    This is the REQUIRED half: the loop survives.  Whether the orphaned message
    itself is kept is :func:`check_on_message_unset_buffers` (STRICT).
    """
    with _session(harness) as s:
        t = s.transport()                    # on_message deliberately not assigned
        publisher = s.transport()
        t.subscribe(TOPIC_A)
        harness.pump()

        publisher.publish(TOPIC_A, _body(1))  # lands while on_message is None
        harness.pump()

        inbox = _Inbox()
        t.on_message = inbox
        harness.pump()
        publisher.publish(TOPIC_A, _body(2))
        harness.pump()

        _require(
            2 in _seqs(inbox),
            'after a message arrived while on_message was None, a LATER message was '
            f'not delivered either (got {_seqs(inbox)}). The receive loop did not '
            'survive the unset callback.',
        )


def check_on_message_unset_buffers(harness):
    """STRICT: a message arriving while ``on_message`` is ``None`` must not be LOST.

    Under paho the window is microseconds -- SUBSCRIBE and PUBLISH share one
    ordered TCP connection -- so paho drops such a message and nobody has ever
    noticed.  LIFECYCLE CONTRACT point 4 requires a remote transport to BLOCK
    ``subscribe()`` until the subscription is live; that widens the window to the
    group-join latency, hundreds of milliseconds, and at boot the scheduler is
    broadcasting to ~1063 agents that are all joining at once.  A dropped step
    broadcast there means the agent never replies, is counted unresponsive and is
    pruned.

    ``PahoTransport`` fails this check ON PURPOSE and that failure is asserted in
    ``tests/test_conformance_suite.py``: it records paho's real behaviour rather
    than papering over it.  Any transport whose subscribe costs a round trip must
    buffer instead.
    """
    with _session(harness) as s:
        t = s.transport()
        publisher = s.transport()
        t.subscribe(TOPIC_A)
        harness.pump()

        publisher.publish(TOPIC_A, _body(1))
        harness.pump()

        inbox = _Inbox()
        t.on_message = inbox
        harness.pump()

        _require(
            _seqs(inbox) == [1],
            f'a message that arrived while on_message was None was lost (got '
            f'{_seqs(inbox)}, expected [1]). Buffer inbound messages until a '
            'callback is assigned, or do not begin consuming until then.',
        )


def check_reentrancy_from_on_message(harness):
    """``subscribe``/``unsubscribe``/``publish`` are re-entered from INSIDE ``on_message``.

    Reproduced against the real code: delivering one step broadcast through
    ``ORSimAgent.on_receive_message`` re-enters the transport as
    ``unsubscribe(broadcast)``, ``subscribe(broadcast)`` and
    ``publish(scheduler_topic)`` -- via ``OpenRideAgent._wrap_app_topic_handler``,
    ``::_unsubscribe_broadcast``, ``::_resubscribe_broadcast`` and the reply
    publish -- all with ``on_receive_message`` still on the stack.

    Combine that with LIFECYCLE CONTRACT point 4's blocking ``subscribe()`` and
    the naive Kafka implementation deadlocks: subscribe() waits for an assignment
    that only the poll loop can advance, and the poll loop is the thread now
    inside subscribe().  It returns after its timeout, unsubscribed, with a log
    warning; with ~1063 agents napping and waking throughout a run that is a
    permanent stall.

    A failure here reads as "no reply arrived within the pump deadline", which
    covers both the deadlock and the quieter variant where the re-entrant call
    is silently dropped.
    """
    with _session(harness) as s:
        replies = _Inbox()
        s.transport(on_message=replies, subscribe=TOPIC_REPLY)
        publisher = s.transport()
        t = s.transport(start=False)
        errors = []

        def reentrant(client, userdata, message):
            try:
                t.subscribe(TOPIC_C)
                t.unsubscribe(TOPIC_C)
                t.publish(TOPIC_REPLY, _body(99))
            except Exception as exc:          # noqa: BLE001 - reported verbatim
                errors.append(exc)

        t.on_message = reentrant
        t.loop_start()
        t.subscribe(TOPIC_A)
        harness.pump()

        publisher.publish(TOPIC_A, _body(1))
        harness.pump()

        # NB: the message is built only when the check has already failed --
        # _require evaluates its argument eagerly, and errors[0] would raise
        # IndexError on the passing path.
        if errors:
            raise ConformanceFailure(
                'calling subscribe/unsubscribe/publish from inside on_message '
                f'raised {errors[0]!r}. All three are re-entered from the '
                "transport's own receive callback by OpenRideAgent on every nap "
                'and wake.')
        _require(
            _seqs(replies) == [99],
            'the reply published from inside on_message never arrived (got '
            f'{_seqs(replies)}). Either the re-entrant call deadlocked against the '
            'receive loop -- the shape a blocking Kafka subscribe() produces, since '
            'only the poll thread can advance the assignment it waits for -- or it '
            'was silently dropped.',
        )


def check_dispatch_is_serialised(harness):
    """``on_message`` is dispatched ONE MESSAGE AT A TIME -- never two at once.

    See this module's DISPATCH SERIALISATION section for the full argument.  In
    short: ``OpenRideScheduler._handle_reply`` runs on the transport's dispatch
    thread and does ``self._did_step_count += 1`` / ``self._outstanding -= 1``
    with no lock, on the stated basis that "individual set/dict/int operations
    are GIL-atomic".  A read-modify-write is not one operation.  Two concurrent
    replies lose an increment; ``_outstanding`` never reaches zero; the step
    burns the full ``STEP_TIMEOUT``.  It happens only under load, only
    sometimes, and logs nothing -- the signature of every bug this suite exists
    for.

    The shape that fails here is not exotic.  A transport that hands each
    message to a worker pool, or that dispatches with a watchdog so a slow
    handler cannot stall its poll loop, is unserialised by construction and
    looks perfect at low load.

    Mechanism: the first callback BLOCKS, holding the dispatcher open, while the
    check waits for a second to overlap it.  A serialised transport cannot
    produce the overlap; an unserialised one does so immediately.  Only mutual
    exclusion is asserted, not a fixed thread -- see WHAT THIS SUITE DOES NOT
    COVER for why.
    """
    with _session(harness) as s:
        lock = threading.Lock()
        state = {'active': 0, 'max_active': 0, 'delivered': 0}
        threads = set()
        first_in = threading.Event()
        release = threading.Event()

        def callback(client, userdata, message):
            with lock:
                state['active'] += 1
                state['max_active'] = max(state['max_active'], state['active'])
                threads.add(threading.get_ident())
                hold = state['active'] == 1 and not first_in.is_set()
            if hold:
                first_in.set()
                # A cap, not the mechanism: the check sets `release` well before
                # this expires, so a conforming transport never waits it out.
                release.wait(DISPATCH_HOLD_SECONDS)
            with lock:
                state['active'] -= 1
                state['delivered'] += 1

        s.transport(on_message=callback, subscribe=TOPIC_A)
        publisher = s.transport()
        harness.pump()

        for n in range(3):
            publisher.publish(TOPIC_A, _body(n))

        # Deliberately no pump() before the overlap window: a pump would block
        # on the held callback and spend its whole deadline. A harness whose
        # delivery happens only inside pump() simply never sets `first_in`, waits
        # out this bound once, and is then measured with `release` already set --
        # correct, just slower.
        first_in.wait(1.0)
        time.sleep(DISPATCH_OVERLAP_WINDOW_SECONDS)
        release.set()

        _settle_until(harness, lambda: state['delivered'] == 3)
        deadline = time.time() + 1.0
        while state['delivered'] < 3 and time.time() < deadline:
            time.sleep(0.005)

        _require(
            state['delivered'] == 3,
            f'setup failed: {state["delivered"]} of 3 messages were dispatched, so '
            'nothing about serialisation was observed. A callback that blocks '
            'briefly must not cost delivery.')
        _require(
            state['max_active'] == 1,
            f'{state["max_active"]} on_message callbacks were running CONCURRENTLY '
            f'(across {len(threads)} thread(s)). Dispatch must be serialised: '
            'OpenRideScheduler._handle_reply runs on this thread and updates the '
            'step barrier counters with no lock, so two overlapping dispatches '
            'lose an increment, _outstanding never reaches zero and the step '
            'burns the full STEP_TIMEOUT -- intermittently, under load, silently. '
            'Dispatch from a single consumer loop, or serialise the hand-off '
            'yourself; do not hand each message to a worker pool.',
        )


def check_disconnect_stops_receive_loop(harness):
    """``disconnect()`` is an agent's ENTIRE teardown: it must stop the loop too.

    ``Messenger.disconnect`` calls ``loop_stop`` only ``if self.channel_id is not
    None``, and agents build their Messenger with ``channel_id=None``
    (``ORSimAgent.__init__``), so for every agent in every run ``disconnect()`` is
    the only teardown call that ever arrives.  paho satisfies this by accident:
    ``loop_forever``'s ``should_exit()`` observes the disconnecting state and the
    network thread ends on its own.

    An implementor who reads the two names literally -- consumer close in
    ``disconnect()``, thread join in ``loop_stop()`` -- leaks a poll thread per
    agent.  Celery workers here are long-lived, ~1063 agents per run, so the leak
    accumulates across runs until a worker restart.  That variant is INVISIBLE to
    the delivery assertion below; it is caught only when the harness supplies
    ``loop_is_running``.
    """
    with _session(harness) as s:
        inbox = _Inbox()
        t = s.transport(on_message=inbox, subscribe=TOPIC_A)
        publisher = s.transport()
        harness.pump()

        publisher.publish(TOPIC_A, _body(1))
        harness.pump()
        _require(_seqs(inbox) == [1],
                 f'setup failed: expected [1] before disconnect, got {_seqs(inbox)}')

        t.disconnect()
        harness.pump()
        publisher.publish(TOPIC_A, _body(2))
        harness.pump()

        _require(
            _seqs(inbox) == [1],
            f'a disconnected transport kept delivering (got {_seqs(inbox)}). '
            'disconnect() must close the connection AND stop the receive loop.',
        )
        running = harness.loop_is_running(t)
        _require(
            running is not True,
            'delivery stopped but the receive loop is still running after '
            'disconnect(): the poll thread is leaked. One per agent, ~1063 agents '
            'per run, in celery workers that live across many runs.',
        )


def check_disconnect_flushes_pending_publishes(harness):
    """A publish accepted before ``disconnect()`` must still be delivered.

    ``publish()``'s return value is not load-bearing anywhere, so a transport may
    buffer -- but there is a real call site with ZERO delay between the publish
    and the teardown: ``OpenRideAgent.start_listening`` calls
    ``super().start_listening()``, which publishes the agent's boot reply, and
    then for a failed agent immediately calls ``stop_listening()`` ->
    ``Messenger.disconnect()`` -> ``transport.disconnect()``.  With librdkafka's
    default 5 ms linger that reply is still in the local queue and is PURGED on
    destroy; the scheduler never records the agent and burns ``STEP_TIMEOUT``
    waiting for it.  This is the same shape as the run that lost 3,247 events and
    still reported ``ok: true``.
    """
    with _session(harness) as s:
        inbox = _Inbox()
        s.transport(on_message=inbox, subscribe=TOPIC_A)
        sender = s.transport()
        harness.pump()

        sender.publish(TOPIC_A, _body(1))
        sender.disconnect()
        harness.pump()

        _require(
            _seqs(inbox) == [1],
            f'a message published immediately before disconnect() was lost (got '
            f'{_seqs(inbox)}). disconnect() must flush outbound publishes already '
            'accepted, and release send-side resources, not only stop the receive '
            'loop.',
        )


def check_messenger_drives_the_transport(harness):
    """The transport survives being driven by the real ``Messenger``, not just by this suite.

    Everything above calls the transport the way the SUITE calls it.  The
    framework calls it in two shapes nothing else here reaches, and both are
    keyword calls, so a signature that differs by a parameter NAME breaks them
    while satisfying every positional call in this file:

    * ``Messenger.__init__`` does ``self.client.subscribe(channel_id, qos=0)``.
      ``ORSimScheduler`` constructs its Messenger with a ``channel_id`` string,
      so this is the scheduler's own boot path.
    * ``Messenger.disconnect`` does ``self.client.loop_stop(force=True)`` -- and
      wraps it in ``except Exception: logging.exception``, so a transport that
      rejects ``force`` has its receive loop silently left running.

    It also drives the shape every AGENT uses, which is the opposite one:
    ``channel_id=None``, so ``Messenger.__init__`` calls ``loop_start()`` and
    subscribes to NOTHING, and the real subscribe arrives later from
    ``ORSimAgent.start_listening`` -- subscribe first, ``on_message`` after,
    onto an already-running loop -- followed by the reply publish through
    ``messenger.client.publish``.

    ``Messenger`` is imported inside the function on purpose: it pulls
    ``paho_transport`` (and therefore paho) with it, and this module promises to
    be importable without any broker library present.
    """
    from .messenger import Messenger

    with _session(harness) as s:
        # 1. Messenger.disconnect's exact call, made directly -- its failure is
        #    swallowed there, so it has to be asserted here.
        probe = s.transport()
        try:
            probe.loop_stop(force=True)
        except TypeError as exc:
            raise ConformanceFailure(
                f'loop_stop(force=True) raised {exc!r}. Messenger.disconnect() '
                'makes exactly that call and logs-and-swallows what it raises, so '
                'the receive loop is left running with nothing to show for it. '
                'Keep the parameter NAME: the contract signature is '
                'loop_stop(self, force=False).')

        # 2. The scheduler shape: channel_id as a str -> subscribe(topic, qos=0).
        inbox = _Inbox()
        scheduler_side = s.transport(start=False)      # Messenger calls loop_start
        publisher = s.transport()
        try:
            scheduler = Messenger({}, {}, channel_id=TOPIC_A, on_message=inbox,
                                  transport=scheduler_side)
        except Exception as exc:                       # noqa: BLE001
            raise ConformanceFailure(
                f'Messenger(channel_id={TOPIC_A!r}, transport=...) raised '
                f'{exc!r}. That constructor assigns on_message, calls '
                'subscribe(channel_id, qos=0) and then loop_start(); it is how '
                'ORSimScheduler boots.')
        harness.pump()
        publisher.publish(TOPIC_A, _body(1))
        _settle_until(harness, lambda: _seqs(inbox) == [1])
        _require(
            _seqs(inbox) == [1],
            'a Messenger built with a str channel_id received '
            f'{_seqs(inbox)}, expected [1]. Messenger subscribes with '
            'subscribe(channel_id, qos=0) -- qos by KEYWORD -- and assigns '
            'on_message BEFORE subscribing, which is the reverse of the agent '
            'path; a transport must carry both orders.')

        # 3. The agent shape: channel_id=None, subscribe later, then reply.
        replies = _Inbox()
        s.transport(on_message=replies, subscribe=TOPIC_REPLY)
        agent_inbox = _Inbox()
        agent_side = s.transport(start=False)
        try:
            agent = Messenger({}, {}, channel_id=None, transport=agent_side)
        except Exception as exc:                       # noqa: BLE001
            raise ConformanceFailure(
                f'Messenger(channel_id=None, transport=...) raised {exc!r}. Every '
                'agent in every run is built this way: loop_start() with no '
                'subscription at all.')
        agent.client.subscribe(TOPIC_B)                # start_listening's order:
        agent.client.on_message = agent_inbox          # subscribe, THEN callback
        harness.pump()
        publisher.publish(TOPIC_B, _body(2))
        agent.client.publish(TOPIC_REPLY, _body(3))
        _settle_until(harness, lambda: _seqs(agent_inbox) == [2]
                      and _seqs(replies) == [3])
        _require(
            _seqs(agent_inbox) == [2],
            f'an agent-shaped Messenger received {_seqs(agent_inbox)}, expected '
            '[2]. ORSimAgent.start_listening subscribes on a loop that '
            'Messenger.__init__ has already started, and assigns on_message only '
            'afterwards.')
        _require(
            _seqs(replies) == [3],
            f'the reply published through messenger.client never arrived (got '
            f'{_seqs(replies)}, expected [3]). ORSimAgent publishes every step '
            'reply this way.')

        # 4. Teardown through Messenger. It resolves loop_stop/disconnect OUTSIDE
        #    its try block, so a missing member surfaces as AttributeError here
        #    rather than being tidied away as a successful teardown.
        for label, messenger in (('scheduler-shaped', scheduler),
                                 ('agent-shaped', agent)):
            try:
                messenger.disconnect()
            except Exception as exc:                   # noqa: BLE001
                raise ConformanceFailure(
                    f'Messenger.disconnect() raised {exc!r} on the {label} '
                    'Messenger. It is an agent\'s entire teardown path.')


# ---------------------------------------------------------------------------
# Registry, runner, report
# ---------------------------------------------------------------------------


class Check:
    """One executable contract requirement.

    Attributes:
        id (str): stable identifier, used in reports and in parametrised tests.
        severity (str): :data:`REQUIRED` or :data:`STRICT`.
        contract (str): where the requirement is written in ``transport.py``.
        fn (callable): ``fn(harness)``, raising :class:`ConformanceFailure`.
    """

    __slots__ = ('id', 'severity', 'contract', 'fn')

    def __init__(self, id, severity, contract, fn):  # noqa: A002 - mirrors the field name
        self.id = id
        self.severity = severity
        self.contract = contract
        self.fn = fn

    @property
    def title(self):
        return (self.fn.__doc__ or '').strip().splitlines()[0]

    def __repr__(self):
        return f'<Check {self.id} [{self.severity}]>'


CHECKS = (
    Check('interface_members', REQUIRED, 'class docstring MANDATORY',
          check_interface_members),
    Check('broadcast_fanout', REQUIRED, 'DELIVERY SEMANTICS 1',
          check_broadcast_fanout),
    Check('subscribe_is_additive_and_idempotent', REQUIRED, 'subscribe() ADDITIVE',
          check_subscribe_is_additive_and_idempotent),
    Check('unsubscribe_removes_exactly_one_topic', REQUIRED, 'unsubscribe()',
          check_unsubscribe_removes_exactly_one_topic),
    Check('resubscribe_after_unsubscribe', REQUIRED, 'unsubscribe()/subscribe() cycle',
          check_resubscribe_after_unsubscribe),
    Check('unsubscribe_does_not_drop_in_flight_traffic', REQUIRED,
          'unsubscribe() (no transient)',
          check_unsubscribe_does_not_drop_in_flight_traffic),
    Check('subscribe_shapes', REQUIRED, 'PROVENANCE subscribe()',
          check_subscribe_shapes),
    Check('subscribe_in_effect_on_return', REQUIRED, 'LIFECYCLE CONTRACT 4',
          check_subscribe_in_effect_on_return),
    Check('topic_fidelity', REQUIRED, 'TOPIC CONTRACT (round trip)',
          check_topic_fidelity),
    Check('slash_topics', REQUIRED, 'TOPIC CONTRACT (grammar)',
          check_slash_topics),
    Check('payload_str_in_bytes_out', REQUIRED, 'PAYLOAD CONTRACT',
          check_payload_str_in_bytes_out),
    Check('forward_only_delivery', REQUIRED, 'DELIVERY SEMANTICS (forward-only)',
          check_forward_only_delivery),
    Check('on_message_reads_back', REQUIRED, 'on_message attribute',
          check_on_message_reads_back),
    Check('on_message_unset_does_not_break_the_loop', REQUIRED, 'CONCURRENCY CONTRACT 2',
          check_on_message_unset_does_not_break_the_loop),
    Check('on_message_unset_buffers', STRICT, 'CONCURRENCY CONTRACT 2',
          check_on_message_unset_buffers),
    Check('reentrancy_from_on_message', REQUIRED, 'CONCURRENCY CONTRACT 1',
          check_reentrancy_from_on_message),
    Check('dispatch_is_serialised', REQUIRED, 'DISPATCH SERIALISATION',
          check_dispatch_is_serialised),
    Check('disconnect_stops_receive_loop', REQUIRED, 'LIFECYCLE CONTRACT 6',
          check_disconnect_stops_receive_loop),
    Check('disconnect_flushes_pending_publishes', REQUIRED, 'DURABILITY CONTRACT',
          check_disconnect_flushes_pending_publishes),
    Check('messenger_drives_the_transport', REQUIRED, 'PROVENANCE Messenger',
          check_messenger_drives_the_transport),
)


class CheckResult:
    """The verdict on one check."""

    __slots__ = ('check', 'passed', 'detail', 'seconds')

    def __init__(self, check, passed, detail, seconds):
        self.check = check
        self.passed = passed
        self.detail = detail
        self.seconds = seconds

    @property
    def id(self):
        return self.check.id

    @property
    def severity(self):
        return self.check.severity

    def __repr__(self):
        return f'<CheckResult {self.id} {"PASS" if self.passed else "FAIL"}>'


class ConformanceReport:
    """The verdicts for a whole run, plus a human-readable summary."""

    def __init__(self, harness_name, results):
        self.harness_name = harness_name
        self.results = list(results)

    @property
    def failed(self):
        return [r for r in self.results if not r.passed]

    @property
    def passed(self):
        return [r for r in self.results if r.passed]

    @property
    def failed_ids(self):
        return [r.id for r in self.failed]

    @property
    def ok(self):
        return not self.failed

    def summary(self):
        lines = [
            f'MessengerTransport conformance: {self.harness_name} -- '
            f'{len(self.passed)} passed, {len(self.failed)} failed '
            f'of {len(self.results)}',
        ]
        for result in self.results:
            mark = 'PASS' if result.passed else 'FAIL'
            lines.append(f'  [{mark}] {result.id} ({result.severity}) '
                         f'[{result.check.contract}]')
            if not result.passed:
                for line in str(result.detail).splitlines():
                    lines.append(f'         {line}')
        return '\n'.join(lines)

    def raise_if_failed(self):
        """Raise :class:`ConformanceFailure` carrying the full summary."""
        if self.failed:
            raise ConformanceFailure(self.summary())
        return self

    def __repr__(self):
        return (f'<ConformanceReport {self.harness_name} '
                f'{len(self.passed)}/{len(self.results)} passed>')


def run_conformance_suite(harness, severities=(REQUIRED, STRICT), checks=CHECKS):
    """Run every selected check against ``harness`` and return a report.

    Checks are independent: a failure is recorded and the run continues, so one
    broken semantic does not hide the other nineteen.  Unexpected exceptions are
    recorded as failures with their traceback rather than propagating -- an
    implementation that raises ``NotImplementedError`` from ``unsubscribe`` is a
    conformance failure, not a suite crash.

    Args:
        harness (TransportHarness): the transport under test.
        severities (tuple): which levels to run.  Pass ``(REQUIRED,)`` for a
            local/in-process transport, both for anything talking to a broker.
        checks (tuple): override the check set (mostly for the suite's own tests).

    Returns:
        ConformanceReport
    """
    if not isinstance(harness, TransportHarness):
        raise TypeError('harness must be a TransportHarness')

    results = []
    for check in checks:
        if check.severity not in severities:
            continue
        started = time.time()
        try:
            harness.begin_check(check)
            check.fn(harness)
        except ConformanceFailure as failure:
            results.append(CheckResult(check, False, str(failure), time.time() - started))
        except Exception:  # noqa: BLE001 - any escape is a conformance failure
            results.append(CheckResult(
                check, False,
                'the check raised instead of returning a verdict:\n'
                + traceback.format_exc().rstrip(),
                time.time() - started,
            ))
        else:
            results.append(CheckResult(check, True, None, time.time() - started))
    return ConformanceReport(harness.name, results)
