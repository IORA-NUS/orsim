"""Doubles for exercising :mod:`orsim.messenger.conformance` -- and for BINDING it.

WHY THIS MODULE EXISTS
----------------------
A conformance suite is only worth having if it REJECTS something.  A suite that
passes every implementation handed to it is not a specification, it is
decoration -- which is the defect the first round of this work shipped, when a
test pinned the abstract-method SET and therefore certified any class that
declared six method names.

So this module supplies three things:

1. :class:`InMemoryBroker` + :class:`LoopbackTransport` -- a CONFORMING transport
   with a real background receive loop, real per-subscriber fan-out and real
   asynchrony.  It is the positive control: if the suite fails this, the suite is
   wrong.
2. :class:`FakePahoClient` -- a functional stand-in for ``paho.mqtt.client.Client``
   that reproduces the behaviours ``PahoTransport`` relies on (str-in/bytes-out,
   ordered publish-then-disconnect, network thread that ends on disconnect) AND
   the one behaviour it gets wrong (an inbound message arriving while
   ``on_message`` is unset is DROPPED).  Running the suite against
   ``PahoTransport`` on top of this is what proves the suite passes the incumbent
   at the REQUIRED level and, at the STRICT level, still has teeth.
3. Twenty-one MUTANTS, each breaking exactly ONE rule.  Every check in the suite
   has at least one mutant that it -- and, wherever the semantics permit,
   ONLY it -- catches.  ``tests/test_conformance_suite.py`` asserts the exact
   failure set for each, so a check that stops binding fails the test rather
   than quietly passing everything again.

   Four of them (:class:`ResubscribeIsDeadTransport`,
   :class:`EagerRebalanceTransport`, :class:`UnserialisedDispatchTransport`,
   :class:`KeywordUnfriendlyTransport`) were written by an ADVERSARIAL review of
   phase 1 that beat the suite: each scored a full 16/16 including STRICT and
   each is fatally broken in a real run.  They are the reason this file grew, and
   they are the reason to distrust a green report that has not been attacked.

None of this opens a socket: the broker is a dict, a lock and a queue per client.
``tests/conftest.py``'s network guard therefore stays satisfied without any
patching, which is the same discipline ``tests/fakes.py`` established.

WHY NOT ``tests.fakes.FakeTransport``
-------------------------------------
The obvious candidate for "a known-good transport to prove the suite passes" is
the existing ``FakeTransport``.  It is not one, and the difference matters:
``FakeTransport`` is a call RECORDER.  It has no broker, no routing and no
delivery -- its ``deliver()`` helper invokes ``on_message`` directly whether or
not anything ever subscribed to the topic.  It is exactly right for the
assertions it was built for (what did ``Messenger`` call, in what order) and it
cannot conform to a DELIVERY contract, because it implements no delivery.

Making it pass would mean writing a harness whose ``pump()`` does the routing --
the one thing the conformance module's HARNESS RULES forbid, because then every
check passes on the harness's behalf.  ``test_conformance_suite.py`` demonstrates
both halves of that instead of pretending otherwise: the recorder is reported as
non-conforming, and a routing harness is caught by the suite's anti-cheat.
"""

import queue
import threading
import time
from contextlib import contextmanager
from types import SimpleNamespace
from unittest import mock

from orsim.messenger.conformance import TransportHarness
from orsim.messenger.transport import MessengerTransport


# ---------------------------------------------------------------------------
# Substrate
# ---------------------------------------------------------------------------


class Message:
    """The two attributes ORSim's two ``on_receive_message`` callbacks read."""

    __slots__ = ('topic', 'payload')

    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload

    def __repr__(self):
        return f'Message(topic={self.topic!r}, payload={self.payload!r})'


class InMemoryBroker:
    """A pub/sub broker in a dict, with an explicit consumer-GROUP knob.

    The group knob is not decoration: it is how :class:`CompetingConsumerTransport`
    reproduces Kafka's default delivery without anyone having to install Kafka.
    ``group=None`` (what a conforming transport passes) means every subscriber of
    a topic gets its own copy; a shared group name means the members of that
    group take turns, which is the failure ``DELIVERY SEMANTICS`` is written to
    prevent.

    ``history`` exists for the same reason: only :class:`ReplayOnSubscribeTransport`
    reads it, so that "replays from a committed offset" is a thing a transport can
    actually DO here rather than a thing the docstring merely warns about.
    """

    def __init__(self):
        self._lock = threading.RLock()
        self._clients = []
        self._subs = {}          # id(client) -> {topic: group_or_None}
        self._rr = {}            # (topic, group) -> next index
        self.history = {}        # topic -> [payload]

    # -- membership ---------------------------------------------------------

    def register(self, client):
        with self._lock:
            if not any(c is client for c in self._clients):
                self._clients.append(client)
            self._subs.setdefault(id(client), {})

    def unregister(self, client):
        with self._lock:
            self._clients = [c for c in self._clients if c is not client]
            self._subs.pop(id(client), None)

    def is_registered(self, client):
        with self._lock:
            return any(c is client for c in self._clients)

    # -- subscriptions ------------------------------------------------------

    def subscribe(self, client, topic, group=None):
        with self._lock:
            self._subs.setdefault(id(client), {})[topic] = group

    def unsubscribe(self, client, topic):
        with self._lock:
            self._subs.get(id(client), {}).pop(topic, None)

    def unsubscribe_all(self, client):
        with self._lock:
            self._subs[id(client)] = {}

    def subscriptions(self, client):
        with self._lock:
            return set(self._subs.get(id(client), {}))

    def topic_history(self, topic):
        with self._lock:
            return list(self.history.get(topic, ()))

    # -- traffic ------------------------------------------------------------

    def publish(self, topic, payload):
        """Route ``payload`` (already encoded by the CLIENT) to subscribers."""
        with self._lock:
            self.history.setdefault(topic, []).append(payload)
            broadcast = []
            grouped = {}
            for client in self._clients:
                group = self._subs.get(id(client), {}).get(topic, '__absent__')
                if group == '__absent__':
                    continue
                if group is None:
                    broadcast.append(client)
                else:
                    grouped.setdefault(group, []).append(client)
            targets = list(broadcast)
            for group, members in grouped.items():
                index = self._rr.get((topic, group), 0)
                self._rr[(topic, group)] = index + 1
                targets.append(members[index % len(members)])
        for client in targets:
            client.enqueue(Message(topic, payload))

    def quiescent(self):
        with self._lock:
            clients = list(self._clients)
        return all(client.is_idle() for client in clients)


# ---------------------------------------------------------------------------
# A client with a real receive loop
# ---------------------------------------------------------------------------


class _BrokerClient:
    """Inbox + background dispatch thread.  Shared by the transport and the paho fake.

    ``buffers_until_callback`` is the one behavioural switch: ``True`` holds an
    inbound message that arrives while ``on_message`` is unset and dispatches it
    once a callback is assigned (what LIFECYCLE CONTRACT point 4 needs from a
    transport whose ``subscribe()`` blocks for a round trip); ``False`` drops it,
    which is what real paho does.
    """

    buffers_until_callback = True
    poll_interval = 0.002

    def _client_init(self, broker, label=''):
        self._broker = broker
        self._inbox = queue.Queue()
        self._held = []                 # arrived while on_message was unset
        self._dispatching = False
        self._loop_thread = None
        self._stop = threading.Event()
        self._label = label
        broker.register(self)

    # -- inbound ------------------------------------------------------------

    def enqueue(self, message):
        self._inbox.put(message)

    def is_idle(self):
        if self._dispatching or not self._inbox.empty():
            return False
        if self._has_pending_work():
            return False
        # A held message with a callback now assigned is work that has not
        # happened yet; pump() must not report quiescence before it lands.
        return not (self._held and self._current_callback() is not None)

    def _has_pending_work(self):
        """Work a mutant's :meth:`_loop_tick` still owes.  See ``_loop_tick``."""
        return False

    def _loop_tick(self):
        """Called once per receive-loop iteration; a seam for the mutants.

        Two mutants need work to happen ON THE LOOP THREAD rather than in the
        caller: :class:`DeferredSubscribeTransport` (activation deferred to a
        later poll -- Kafka's actual ``subscribe()`` semantics) and
        :class:`DroppingOutboxTransport` (a buffered producer).  Without this
        seam their mutation would have to be synchronous, which would stop
        modelling the thing that makes both bugs real.
        """

    def _current_callback(self):
        return self.on_message

    def _invoke(self, callback, message):
        callback(self, None, message)

    def _dispatch(self, message):
        callback = self._current_callback()
        if callback is None:
            if self.buffers_until_callback:
                self._held.append(message)
            return
        self._release_held(callback)
        self._invoke(callback, message)

    def _release_held(self, callback):
        if not self._held:
            return
        held, self._held = self._held, []
        for message in held:
            self._invoke(callback, message)

    def _run_loop(self):
        while not self._stop.is_set():
            self._loop_tick()
            try:
                message = self._inbox.get(timeout=self.poll_interval)
            except queue.Empty:
                callback = self._current_callback()
                if self._held and callback is not None:
                    self._dispatching = True
                    try:
                        self._release_held(callback)
                    finally:
                        self._dispatching = False
                continue
            self._dispatching = True
            try:
                self._dispatch(message)
            finally:
                self._dispatching = False

    # -- loop lifecycle -----------------------------------------------------

    def _start_loop(self):
        if self._loop_thread is not None and self._loop_thread.is_alive():
            return
        self._stop.clear()
        self._loop_thread = threading.Thread(
            target=self._run_loop, daemon=True,
            name=f'conformance-loop-{self._label or id(self)}')
        self._loop_thread.start()

    def _stop_loop(self, join=True):
        self._stop.set()
        thread = self._loop_thread
        self._loop_thread = None
        if join and thread is not None and thread is not threading.current_thread():
            thread.join(timeout=2.0)

    def loop_is_running(self):
        thread = self._loop_thread
        return bool(thread is not None and thread.is_alive())

    def hard_close(self):
        """Teardown that no mutant can decline -- used by the test fixture."""
        self._stop_loop(join=False)
        self._broker.unregister(self)


def _topics_of(topic):
    """Both live subscribe shapes -> a flat list of topic strings.

    ``subscribe('a/b', qos)`` and ``subscribe([('a/b', 0), ('c/d', 0)])`` are both
    in the codebase; paho handles both, so anything standing in for paho must.
    """
    if isinstance(topic, str):
        return [topic]
    out = []
    for item in topic:
        out.append(item[0] if isinstance(item, (tuple, list)) else item)
    return out


def _encode(payload):
    """PAYLOAD CONTRACT: publish() takes str, the wire carries bytes."""
    if isinstance(payload, str):
        return payload.encode('utf-8')
    if isinstance(payload, (bytes, bytearray)):
        return bytes(payload)
    return str(payload).encode('utf-8')


class _LoopbackOps:
    """The six mandatory members, implemented once so mutants can override one."""

    def disconnect(self, **kwargs):
        self._flush_outbox()
        self._stop_loop()
        self._broker.unregister(self)

    def loop_start(self, **kwargs):
        self._start_loop()

    def loop_stop(self, force=False, **kwargs):
        self._stop_loop()

    def publish(self, topic, payload=None, qos=0, retain=False, **kwargs):
        self._broker.publish(topic, _encode(payload))

    def subscribe(self, topic, qos=0, **kwargs):
        for name in _topics_of(topic):
            self._broker.subscribe(self, name)

    def unsubscribe(self, topic, **kwargs):
        for name in _topics_of(topic):
            self._broker.unsubscribe(self, name)

    def _flush_outbox(self):
        """No-op here; :class:`DroppingOutboxTransport` is what makes it a seam."""


class LoopbackTransport(_LoopbackOps, _BrokerClient, MessengerTransport):
    """A CONFORMING transport: the suite's positive control.

    Deliberately asynchronous -- delivery happens on a background thread, so the
    checks exercise the same "did it arrive yet or not at all" ambiguity a real
    broker creates, and the harness's ``pump()`` has real work to do.  A
    synchronous double would let a suite pass that could never pass against a
    network transport.
    """

    def __init__(self, broker, label=''):
        self._client_init(broker, label)


# ---------------------------------------------------------------------------
# A functional stand-in for paho.mqtt.client.Client
# ---------------------------------------------------------------------------


class FakePahoClient(_BrokerClient):
    """What ``PahoTransport`` forwards to, with paho's actual behaviours.

    Modelled deliberately, each because ``PahoTransport``'s conformance depends
    on it:

    * ``publish()`` encodes a ``str`` payload to UTF-8 bytes -- the asymmetry the
      PAYLOAD CONTRACT exists for, and the reason no ORSim call site has ever
      encoded anything itself.
    * Topics pass through verbatim, ``'/'`` included; MQTT has no topic-character
      restriction.
    * ``publish()`` reaches the broker before ``disconnect()`` returns, because
      paho puts PUBLISH and DISCONNECT on ONE ordered TCP connection.
    * ``disconnect()`` ends the network thread on its own -- paho's
      ``loop_forever`` sees the disconnecting state via ``should_exit()``.  This
      is what makes ``disconnect()`` sufficient teardown for an agent, which
      never calls ``loop_stop`` at all.
    * ``buffers_until_callback = False``: an inbound message arriving while
      ``on_message`` is unset is DROPPED.  That is real paho, and it is why
      ``PahoTransport`` fails the one STRICT check.  Making the fake forgiving
      here would hand ``PahoTransport`` a pass it has not earned and would hide
      the very hazard the strict check exists to state.
    """

    buffers_until_callback = False
    on_message = None

    def __init__(self, client_id=None, clean_session=True, broker=None):
        self.client_id = client_id
        self.clean_session = clean_session
        self._connected = False
        self._client_init(broker, label=str(client_id))

    # -- paho's surface -----------------------------------------------------

    def username_pw_set(self, username=None, password=None, **kwargs):
        self._credentials = (username, password)

    def connect(self, host, port=1883, keepalive=60, **kwargs):
        self._connected = True
        return 0

    def disconnect(self, **kwargs):
        self._connected = False
        self._stop_loop()
        self._broker.unregister(self)
        return 0

    def loop_start(self, **kwargs):
        self._start_loop()

    def loop_stop(self, force=False, **kwargs):
        self._stop_loop()

    def publish(self, topic, payload=None, qos=0, retain=False, **kwargs):
        self._broker.publish(topic, _encode(payload))
        return SimpleNamespace(rc=0, mid=0)

    def subscribe(self, topic, qos=0, **kwargs):
        for name in _topics_of(topic):
            self._broker.subscribe(self, name)
        return (0, 0)

    def unsubscribe(self, topic, **kwargs):
        for name in _topics_of(topic):
            self._broker.unsubscribe(self, name)
        return (0, 0)


@contextmanager
def functional_paho(broker_ref):
    """Patch ``paho`` and ``requests`` inside ``paho_transport`` with WORKING doubles.

    ``tests.fakes.patched_broker`` patches both with ``MagicMock``, which is right
    for asserting the four construction side effects happened in order and useless
    for asking whether messages move.  This one substitutes a paho module whose
    ``Client`` really delivers, so ``PahoTransport`` can be put through the
    conformance suite without a broker.

    ``requests`` stays a mock: RabbitMQ user registration is genuine HTTP that has
    nothing to do with the transport contract.  ``status_code = 200`` takes the
    "user already exists" branch, so construction issues two PUTs and no user
    creation.

    Args:
        broker_ref (callable): returns the broker that new clients should join.
            A callable rather than a broker so the harness can hand out a fresh
            substrate per check without re-entering the patch.
    """
    def make_client(client_id=None, clean_session=True, **kwargs):
        return FakePahoClient(client_id, clean_session, broker=broker_ref())

    paho_double = SimpleNamespace(Client=make_client)
    with mock.patch('orsim.messenger.paho_transport.paho', paho_double), \
         mock.patch('orsim.messenger.paho_transport.requests') as requests_mock:
        requests_mock.get.return_value.status_code = 200
        yield paho_double


# ---------------------------------------------------------------------------
# Harness plumbing
# ---------------------------------------------------------------------------


class World:
    """Owns the broker and every transport made from it, so threads get reaped.

    ``fresh_broker_per_check`` is what the conformance runner's ``begin_check``
    hook is for.  The checks reuse a handful of topic names, so a transport that
    REPLAYS history (:class:`ReplayOnSubscribeTransport`) would otherwise drag one
    check's traffic into the next and be reported against the wrong semantic.  A
    conforming transport does not need it -- forward-only delivery already makes
    each check hermetic -- which is exactly why the hook is optional on the
    harness.
    """

    def __init__(self, factory=None, expose_loop_probe=True, settle=None):
        self._factory = factory or (lambda broker: LoopbackTransport(broker))
        self.broker = InMemoryBroker()
        self.transports = []
        self.expose_loop_probe = expose_loop_probe
        self.settle = settle

    # -- harness hooks ------------------------------------------------------

    def new_transport(self):
        transport = self._factory(self.broker)
        self.transports.append(transport)
        return transport

    def begin_check(self, check):
        self.broker = InMemoryBroker()

    def pump(self, deadline):
        """Wait for the broker's queues to drain.  Drives delivery; never routes."""
        broker = self.broker
        end = time.time() + deadline
        stable = 0
        while time.time() < end:
            if broker.quiescent():
                stable += 1
                if stable >= 3:
                    return
            else:
                stable = 0
            time.sleep(0.002)

    def loop_is_running(self, transport):
        probe = getattr(transport, 'loop_is_running', None)
        if probe is None:
            return None
        return probe()

    def harness(self, name):
        kwargs = {}
        if self.settle is not None:
            kwargs['settle'] = self.settle
        return TransportHarness(
            new_transport=self.new_transport,
            pump=self.pump,
            loop_is_running=self.loop_is_running if self.expose_loop_probe else None,
            on_check_start=self.begin_check,
            name=name,
            **kwargs,
        )

    def close(self):
        for transport in self.transports:
            try:
                closer = getattr(transport, 'hard_close', None)
                if closer is not None:
                    closer()
            except Exception:
                pass
        self.transports = []


class PahoWorld(World):
    """A :class:`World` whose transports are real ``PahoTransport`` objects."""

    def __init__(self, settings, credentials, **kwargs):
        super().__init__(factory=None, **kwargs)
        self._settings = settings
        self._credentials = credentials
        self._transport_cls = None

    def bind(self, transport_cls):
        self._transport_cls = transport_cls

    def new_transport(self):
        transport = self._transport_cls(self._settings, self._credentials)
        self.transports.append(transport)
        return transport

    def loop_is_running(self, transport):
        return transport._client.loop_is_running()

    def close(self):
        for transport in self.transports:
            try:
                transport._client.hard_close()
            except Exception:
                pass
        self.transports = []


# ---------------------------------------------------------------------------
# The mutants
# ---------------------------------------------------------------------------
#
# One per contract requirement.  Each breaks exactly ONE rule and is otherwise
# the conforming LoopbackTransport, so ``test_conformance_suite.py`` can assert
# the EXACT set of checks that catches it.  Where a mutation unavoidably takes a
# second check down with it, the class docstring says which and why -- that
# overlap is a property of the contract, not slack in the suite.
#
# Every one of these is a transport that instantiates cleanly, satisfies the ABC,
# is accepted by Messenger, and returns normally from every call.  That is the
# point: this is the failure class prose cannot catch.


class NoPublishVirtualTransport(_LoopbackOps, _BrokerClient):
    """Registered as a VIRTUAL subclass, with ``publish`` not callable.

    ``MessengerTransport.register(cls)`` makes ``isinstance`` answer ``True``
    without computing ``__abstractmethods__``, so ``Messenger.__init__``'s guard
    -- whose own comment says it converts a phase-2 omission into a
    construction-time ``TypeError`` -- passes an object that cannot publish.  The
    first agent reply then raises ``AttributeError`` inside a receive callback,
    where ``except Exception: logging.warning; return`` swallows it, and every
    step times out with no traceback anywhere near the mistake.

    A missing ``publish`` breaks everything downstream, so this mutant fails many
    checks.  That is why ``interface_members`` runs FIRST: the report's first line
    names the cause, and the rest is its shadow.
    """

    on_message = None
    publish = None

    def __init__(self, broker, label=''):
        self._client_init(broker, label)


MessengerTransport.register(NoPublishVirtualTransport)


class CompetingConsumerTransport(LoopbackTransport):
    """Subscribes into a SHARED consumer group -- Kafka's default shape.

    Breaks: ``broadcast_fanout``.  One agent of ~1063 receives the step
    broadcast, ``confirm_responses`` never resolves, and every step burns the
    full ``STEP_TIMEOUT`` while the log fills with "agents still waiting".
    """

    GROUP = 'orsim-shared-group'

    def subscribe(self, topic, qos=0, **kwargs):
        for name in _topics_of(topic):
            self._broker.subscribe(self, name, group=self.GROUP)


class ReplacingSubscribeTransport(LoopbackTransport):
    """``subscribe`` REPLACES the subscription set instead of adding to it.

    Breaks: ``subscribe_is_additive_and_idempotent`` and, unavoidably,
    ``subscribe_shapes`` -- that check also subscribes twice, and a replacing
    transport loses the first call there too.  Both failures are the same defect.

    In the simulation this is the nap/wake kill: ``_resubscribe_broadcast``
    re-adds the broadcast topic ALONE, the agent's own app topic disappears, and
    since the wake path is driven by an app-topic event the agent can never be
    woken again -- for the rest of the run, with no error.
    """

    def subscribe(self, topic, qos=0, **kwargs):
        self._broker.unsubscribe_all(self)
        for name in _topics_of(topic):
            self._broker.subscribe(self, name)


class UnsubscribeAllTransport(LoopbackTransport):
    """``unsubscribe(topic)`` drops EVERY subscription, not just that one.

    Breaks: ``unsubscribe_removes_exactly_one_topic``.  Same end state as the
    replacing mutant, reached from the other side: the napping agent loses its
    app topic and never wakes.
    """

    def unsubscribe(self, topic, **kwargs):
        self._broker.unsubscribe_all(self)


class StrOnlySubscribeTransport(LoopbackTransport):
    """Handles ``subscribe(str)`` and stringifies anything else.

    Breaks: ``subscribe_shapes``.  ``ORSimAgent.start_listening`` passes a bare
    list of ``(topic, qos)`` tuples, so this transport subscribes to the literal
    ``"[('run/x/ORSimAgent', 0)]"`` -- a topic nothing publishes to -- and the
    agent is deaf from boot with every call having returned normally.
    """

    def subscribe(self, topic, qos=0, **kwargs):
        name = topic if isinstance(topic, str) else str(topic)
        self._broker.subscribe(self, name)


class DeferredSubscribeTransport(LoopbackTransport):
    """Activates a subscription on a later poll, not before ``subscribe()`` returns.

    Breaks: ``subscribe_in_effect_on_return``.  This is Kafka's REAL behaviour --
    ``consumer.subscribe()`` is a local declaration; the group join and partition
    assignment land on a subsequent ``poll()``, typically hundreds of
    milliseconds later.  ``ORSimAgent.start_listening`` subscribes and then
    immediately publishes its ``'ready'`` reply, so the agent announces itself,
    misses the next step broadcasts, is counted unresponsive and is pruned.

    It passes every other check because every other check pumps after
    subscribing, which is precisely why one check has to refuse to.
    """

    def __init__(self, broker, label=''):
        self._deferred = []
        super().__init__(broker, label)

    def subscribe(self, topic, qos=0, **kwargs):
        self._deferred.extend(_topics_of(topic))

    def _has_pending_work(self):
        return bool(self._deferred)

    def _loop_tick(self):
        while self._deferred:
            self._broker.subscribe(self, self._deferred.pop(0))


class SanitisingTopicTransport(LoopbackTransport):
    """Maps ``'/'`` to ``'.'`` on the wire and does not map it back.

    Breaks: ``topic_fidelity``.  This is the obvious remedy for a broker that
    forbids ``'/'``, and it is worse than the disease: every message arrives, is
    JSON-decoded and updates the payload cache, then
    ``self.message_handlers.get('run1.sched1.ORSimAgent')`` returns ``None`` and
    the message is dropped by a lookup with no else branch.  The scheduler's
    ``if message.topic == ...`` simply never matches, ``_response_event`` is never
    set, and every step burns ``STEP_TIMEOUT`` with the agents visibly working.
    """

    @staticmethod
    def _wire(name):
        return name.replace('/', '.')

    def publish(self, topic, payload=None, qos=0, retain=False, **kwargs):
        self._broker.publish(self._wire(topic), _encode(payload))

    def subscribe(self, topic, qos=0, **kwargs):
        for name in _topics_of(topic):
            self._broker.subscribe(self, self._wire(name))

    def unsubscribe(self, topic, **kwargs):
        for name in _topics_of(topic):
            self._broker.unsubscribe(self, self._wire(name))


class NoSlashTopicTransport(LoopbackTransport):
    """Silently ignores any topic containing ``'/'``.

    Breaks: ``slash_topics`` and, as a consequence, ``topic_fidelity`` -- nothing
    is delivered on a slash topic, so fidelity cannot be established either.  The
    pair is the signature: delivery gone AND fidelity unprovable means the
    character itself, whereas fidelity alone means a mapping that does not reverse.

    Models a Kafka transport built straight from the contract: ``produce()`` and
    ``subscribe()`` accept an illegal topic name locally and report the rejection
    only through an asynchronous delivery report -- which the contract says not to
    care about.  Every agent boots, every publish succeeds, nothing moves.
    """

    def publish(self, topic, payload=None, qos=0, retain=False, **kwargs):
        if '/' in topic:
            return
        self._broker.publish(topic, _encode(payload))

    def subscribe(self, topic, qos=0, **kwargs):
        for name in _topics_of(topic):
            if '/' not in name:
                self._broker.subscribe(self, name)


class StrPayloadTransport(LoopbackTransport):
    """Delivers ``message.payload`` as ``str`` -- "give back what you were given".

    Breaks: ``payload_str_in_bytes_out``.  Both ORSim handlers call
    ``.decode('utf-8')``, so this raises ``AttributeError: 'str' object has no
    attribute 'decode'``: it kills the agent's receive thread on its first
    message, and on the OpenRide scheduler side it is swallowed by
    ``except Exception`` so every reply vanishes and every step times out.
    """

    def publish(self, topic, payload=None, qos=0, retain=False, **kwargs):
        self._broker.publish(topic, payload)


class ReplayOnSubscribeTransport(LoopbackTransport):
    """Replays a topic's history when subscribing -- a committed-offset consumer.

    Breaks: ``forward_only_delivery``.  On a celery worker restart mid-run, a
    rejoining agent replays step broadcasts for time steps the scheduler has
    already closed and re-replies for them, corrupting
    ``agent_collection[...]['step_response']``.
    """

    def subscribe(self, topic, qos=0, **kwargs):
        for name in _topics_of(topic):
            self._broker.subscribe(self, name)
            for payload in self._broker.topic_history(name):
                self.enqueue(Message(name, payload))


class HiddenCallbackTransport(LoopbackTransport):
    """Stores ``on_message`` privately, so reading it back answers ``None``.

    Breaks: ``on_message_reads_back`` and nothing else -- deliberately, because
    this check is about the READ.  The realistic version of this bug (a wrapper
    that forwards the write through ``__setattr__`` to an inner client and has no
    read-through property) also drops every message, since ``MessengerTransport``'s
    ``on_message = None`` class attribute shadows ``__getattr__`` and the
    transport's own loop reads ``None``.  Isolating the read here is what lets the
    report name the CAUSE rather than list nine dead delivery checks.
    """

    _hidden = None

    def __setattr__(self, name, value):
        if name == 'on_message':
            object.__setattr__(self, '_hidden', value)
            return
        object.__setattr__(self, name, value)

    def _current_callback(self):
        return self._hidden


class CrashOnUnsetCallbackTransport(LoopbackTransport):
    """Calls ``self.on_message(...)`` unguarded, so an unset callback kills the loop.

    Breaks: ``on_message_unset_does_not_break_the_loop`` and, as a consequence,
    ``on_message_unset_buffers`` -- a dead loop delivers nothing later either.
    Both are the same window.

    ``ORSimAgent.start_listening`` subscribes BEFORE it assigns ``on_message``, and
    agents pass ``channel_id=None`` so the receive loop is already running by then.
    ``TypeError: 'NoneType' object is not callable`` on the poll thread leaves the
    agent silently deaf for the rest of the run.
    """

    def _dispatch(self, message):
        self.on_message(self, None, message)


class DropWhenCallbackUnsetTransport(LoopbackTransport):
    """Drops an inbound message that arrives while ``on_message`` is unset.

    Breaks: ``on_message_unset_buffers`` (STRICT) only -- the loop survives, so the
    REQUIRED half passes.  This is real paho's behaviour, which is exactly why the
    two halves are separate checks and why ``PahoTransport`` shows the same split.
    Under paho the window is microseconds; a transport that blocks ``subscribe()``
    for a group join, as LIFECYCLE CONTRACT point 4 requires, widens it to the join
    latency, and at boot the scheduler is broadcasting to ~1063 agents that are all
    joining at once.
    """

    buffers_until_callback = False


class LockedDispatchTransport(LoopbackTransport):
    """Holds a non-reentrant lock across dispatch and takes it in subscribe/publish.

    Breaks: ``reentrancy_from_on_message``.  ``OpenRideAgent`` re-enters the
    transport as ``unsubscribe`` / ``subscribe`` / ``publish`` from INSIDE
    ``on_receive_message`` on every nap and wake, so the loop thread blocks on a
    lock it is already holding and the agent processes nothing further.

    This models the deadlock shape a blocking Kafka ``subscribe()`` produces for
    free: it waits for an assignment only the poll loop can advance, and the poll
    loop is the thread inside ``subscribe()``.  ``disconnect`` deliberately takes no
    lock and does not join, so a deadlocked instance can still be torn down.
    """

    def __init__(self, broker, label=''):
        self._lock = threading.Lock()
        super().__init__(broker, label)

    def _dispatch(self, message):
        with self._lock:
            super()._dispatch(message)

    def subscribe(self, topic, qos=0, **kwargs):
        with self._lock:
            super().subscribe(topic, qos, **kwargs)

    def unsubscribe(self, topic, **kwargs):
        with self._lock:
            super().unsubscribe(topic, **kwargs)

    def publish(self, topic, payload=None, qos=0, retain=False, **kwargs):
        with self._lock:
            super().publish(topic, payload, qos, retain, **kwargs)

    def disconnect(self, **kwargs):
        self._stop_loop(join=False)
        self._broker.unregister(self)


class DisconnectKeepsLoopTransport(LoopbackTransport):
    """``disconnect()`` neither closes the connection nor stops the loop.

    Breaks: ``disconnect_stops_receive_loop``.  ``Messenger.disconnect`` calls
    ``loop_stop`` only when a ``channel_id`` was given and agents pass ``None``, so
    ``disconnect()`` is an agent's ENTIRE teardown; a transport that treats it as
    "close the socket, the loop is loop_stop's job" keeps consuming, keeps holding
    group membership, and leaks a poll thread per agent into celery workers that
    live across many runs.
    """

    def disconnect(self, **kwargs):
        return None


class LeakLoopThreadTransport(LoopbackTransport):
    """``disconnect()`` stops delivery but leaves the receive thread alive.

    Breaks: ``disconnect_stops_receive_loop`` -- but ONLY when the harness supplies
    ``loop_is_running``.  The delivery assertion cannot see this: the transport is
    unregistered, so nothing arrives, and the thread spins on forever.  It is the
    reason the probe hook exists, and
    ``test_the_loop_probe_is_what_catches_a_leaked_thread`` asserts both halves --
    caught with the probe, missed without it.
    """

    def disconnect(self, **kwargs):
        self._broker.unregister(self)


class DroppingOutboxTransport(LoopbackTransport):
    """Buffers publishes and PURGES the buffer on ``disconnect()``.

    Breaks: ``disconnect_flushes_pending_publishes``.  This is
    ``confluent_kafka``'s default destructor behaviour, and there is a live call
    site with zero delay between the two: ``OpenRideAgent.start_listening``
    publishes the agent's boot reply and then, for a failed agent, immediately
    calls ``stop_listening()`` -> ``Messenger.disconnect()``.  With librdkafka's 5 ms
    linger that reply is still local and is purged on destroy; the scheduler never
    records the agent and burns ``STEP_TIMEOUT`` waiting for it.  Same shape as the
    run that lost 3,247 events and still reported ``ok: true``.
    """

    def __init__(self, broker, label=''):
        self._outbox = []
        super().__init__(broker, label)

    def publish(self, topic, payload=None, qos=0, retain=False, **kwargs):
        self._outbox.append((topic, _encode(payload)))

    def _has_pending_work(self):
        return bool(self._outbox)

    def _loop_tick(self):
        while self._outbox:
            topic, payload = self._outbox.pop(0)
            self._broker.publish(topic, payload)

    def _flush_outbox(self):
        self._outbox.clear()      # the bug: purge instead of flush


class ResubscribeIsDeadTransport(LoopbackTransport):
    """``unsubscribe(topic)`` poisons that topic: a later ``subscribe`` is a no-op.

    Breaks: ``resubscribe_after_unsubscribe``.  It removes exactly one topic and
    leaves the others alone, so the end-state check passes; it even returns
    normally from the re-subscribe.  It scored a full 16/16 against the suite
    before that check existed, because no check subscribed to a topic TWICE.

    The real shape is a consumer closed or a tombstone recorded per topic on
    unsubscribe and never reopened.  ``OpenRideAgent._resubscribe_broadcast``
    takes the normal return as proof the subscription is live and sets
    ``_broadcast_subscribed = True`` without ever retrying, so every agent that
    returns ``sleep_steps_hint() == -1`` once is deaf to the step broadcast for
    the rest of the run and is pruned as unresponsive -- gradually, across the
    whole fleet, with nothing logged.
    """

    def __init__(self, broker, label=''):
        self._dead = set()
        super().__init__(broker, label)

    def subscribe(self, topic, qos=0, **kwargs):
        for name in _topics_of(topic):
            if name in self._dead:
                continue              # the bug: returns normally, activates nothing
            self._broker.subscribe(self, name)

    def unsubscribe(self, topic, **kwargs):
        for name in _topics_of(topic):
            self._dead.add(name)
            self._broker.unsubscribe(self, name)


class EagerRebalanceTransport(LoopbackTransport):
    """Re-declares its WHOLE subscription set on every change -- an eager assignor.

    Breaks: ``unsubscribe_does_not_drop_in_flight_traffic``.  Modelled on Kafka's
    eager assignment protocol, and modelled FAITHFULLY: the rebalance is
    synchronous on the calling thread (so it is in effect when the call returns),
    the set is additive and idempotent, delivery is broadcast and forward-only.
    Every one of those is a property the suite checks, and this transport scored
    16/16 including STRICT.

    What it does is drop every assignment, take ``REBALANCE_SECONDS`` to rejoin,
    and re-add what is still wanted.  The END STATE is correct, which is why
    ``unsubscribe_removes_exactly_one_topic`` -- which publishes before and after
    the change but never during it -- cannot see the defect.  A message published
    into the window is simply gone, and the window is exactly where the agent's
    wake notice lives: ``OpenRideAgent`` unsubscribes from the step broadcast the
    instant an agent naps, and the scheduler wakes it by publishing to that
    agent's own topic.  A wake notice lost there is never redelivered and the
    agent sleeps out the run.

    The idempotence short-circuit is deliberate: a no-change call triggers no
    rebalance, so the transport keeps passing the idempotency check honestly
    instead of failing for a second reason.
    """

    REBALANCE_SECONDS = 0.05

    def __init__(self, broker, label=''):
        self._wanted = set()
        super().__init__(broker, label)

    def _rebalance(self):
        self._broker.unsubscribe_all(self)
        time.sleep(self.REBALANCE_SECONDS)       # the join: nothing is assigned
        for name in sorted(self._wanted):
            self._broker.subscribe(self, name)

    def subscribe(self, topic, qos=0, **kwargs):
        before = set(self._wanted)
        self._wanted |= set(_topics_of(topic))
        if self._wanted != before:
            self._rebalance()

    def unsubscribe(self, topic, **kwargs):
        before = set(self._wanted)
        self._wanted -= set(_topics_of(topic))
        if self._wanted != before:
            self._rebalance()


class UnserialisedDispatchTransport(LoopbackTransport):
    """Dispatches ``on_message`` on a worker thread and gives up waiting for it.

    Breaks: ``dispatch_is_serialised``.  The motivation is reasonable -- "a slow
    handler must not stall my poll loop, so dispatch with a watchdog" -- and the
    consequence is that two callbacks run at once as soon as one handler takes
    longer than ``DISPATCH_WATCHDOG_SECONDS``.  Under light load the join always
    completes and the transport is indistinguishable from a conforming one, which
    is why it scored 16/16.

    ``OpenRideScheduler._handle_reply`` runs on the dispatch thread and does
    ``self._did_step_count += 1`` / ``self._outstanding -= 1`` with no lock, on
    the documented assumption that int operations are GIL-atomic -- true of a
    single operation, false of a read-modify-write.  Two concurrent replies lose
    an increment, the barrier never resolves, and the step burns the full
    ``STEP_TIMEOUT``: intermittent, load-dependent, silent.
    """

    DISPATCH_WATCHDOG_SECONDS = 0.05

    def _dispatch(self, message):
        worker = threading.Thread(
            target=super()._dispatch, args=(message,), daemon=True,
            name=f'unserialised-dispatch-{id(message)}')
        worker.start()
        worker.join(self.DISPATCH_WATCHDOG_SECONDS)   # the bug: gives up and moves on


class KeywordUnfriendlyTransport(LoopbackTransport):
    """Right behaviour, wrong parameter NAMES: rejects the framework's keyword calls.

    Breaks: ``messenger_drives_the_transport``.  It moves messages perfectly and
    accepts every positional call the rest of the suite makes, so it scored
    16/16.  What it cannot accept are the two keyword calls only ``Messenger``
    makes:

    * ``Messenger.__init__``'s ``subscribe(channel_id, qos=0)`` -- ``qos`` by
      keyword.  ``ORSimScheduler`` constructs with a str ``channel_id``, so this
      raises ``TypeError`` out of the scheduler's own constructor.
    * ``Messenger.disconnect``'s ``loop_stop(force=True)``, which is wrapped in
      ``except Exception: logging.exception`` -- so this one is SILENT and the
      receive loop is simply left running.

    Note the asymmetry of the two names: the suite's own calls are positional, so
    only a check that drives a real ``Messenger`` can see either of these.
    """

    def subscribe(self, topic, qos_level=0):          # the framework passes qos=
        for name in _topics_of(topic):
            self._broker.subscribe(self, name)

    def loop_stop(self, join=True):                   # the framework passes force=
        self._stop_loop()


#: Every mutant, with the EXACT set of check ids it must be caught by.  A check
#: that stops binding shows up here as a shrinking set, not as a green run.
MUTANTS = (
    (NoPublishVirtualTransport, None),                      # see the test: first-failure assertion
    (CompetingConsumerTransport, {'broadcast_fanout'}),
    # Three ids, one defect: a replacing subscribe loses the first of two
    # subscribe calls wherever a check makes two, and all three checks make two.
    (ReplacingSubscribeTransport, {'subscribe_is_additive_and_idempotent',
                                   'subscribe_shapes',
                                   'resubscribe_after_unsubscribe'}),
    # Likewise: dropping EVERY subscription shows up as a wrong end state, as a
    # topic that never comes back after the nap/wake cycle, and as a delivery gap
    # on traffic that was in flight.  The in-flight check sees it only because it
    # keeps publishing for IN_FLIGHT_COOLDOWN_SECONDS after the call returns --
    # without that the overlap would be a microsecond race and this set would be
    # nondeterministic.
    (UnsubscribeAllTransport, {'unsubscribe_removes_exactly_one_topic',
                               'resubscribe_after_unsubscribe',
                               'unsubscribe_does_not_drop_in_flight_traffic'}),
    (ResubscribeIsDeadTransport, {'resubscribe_after_unsubscribe'}),
    (EagerRebalanceTransport, {'unsubscribe_does_not_drop_in_flight_traffic'}),
    (StrOnlySubscribeTransport, {'subscribe_shapes'}),
    (DeferredSubscribeTransport, {'subscribe_in_effect_on_return'}),
    (SanitisingTopicTransport, {'topic_fidelity'}),
    (NoSlashTopicTransport, {'slash_topics', 'topic_fidelity'}),
    (StrPayloadTransport, {'payload_str_in_bytes_out'}),
    (ReplayOnSubscribeTransport, {'forward_only_delivery'}),
    (HiddenCallbackTransport, {'on_message_reads_back'}),
    (CrashOnUnsetCallbackTransport, {'on_message_unset_does_not_break_the_loop',
                                     'on_message_unset_buffers'}),
    (DropWhenCallbackUnsetTransport, {'on_message_unset_buffers'}),
    (LockedDispatchTransport, {'reentrancy_from_on_message'}),
    (UnserialisedDispatchTransport, {'dispatch_is_serialised'}),
    (KeywordUnfriendlyTransport, {'messenger_drives_the_transport'}),
    (DisconnectKeepsLoopTransport, {'disconnect_stops_receive_loop'}),
    (LeakLoopThreadTransport, {'disconnect_stops_receive_loop'}),
    (DroppingOutboxTransport, {'disconnect_flushes_pending_publishes'}),
)
