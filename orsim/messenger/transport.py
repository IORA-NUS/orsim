"""Transport abstraction for :class:`orsim.messenger.Messenger`.

WHY THIS MODULE EXISTS
----------------------
``Messenger`` was written against ``paho.mqtt.client.Client`` directly: it
constructed one, registered a RabbitMQ user through the management HTTP API,
and handed the raw paho object out as ``messenger.client``.  Every agent and
scheduler in the simulation then reached through that attribute
(``messenger.client.publish(...)``), so the broker choice was welded into
17 call sites spread across two repositories.

This module introduces the seam.  ``MessengerTransport`` is the *entire*
surface that messaging in ORSim actually uses -- nine members, no more -- so a
future transport (Kafka, in-memory, a test double) only has to satisfy them to
be a drop-in replacement.  ``Messenger.client`` holds a ``MessengerTransport``
from now on rather than a paho client, which is why none of those 17 call sites
has to change a single character.

Those nine members are NOT all mandatory, and the split is the important part:

* **SIX are abstract**, because the framework really calls them --
  ``disconnect``, ``loop_start``, ``loop_stop``, ``publish``, ``subscribe``,
  ``unsubscribe``.  Omit one and the class cannot be instantiated.
* **TWO are optional** -- ``connect`` and ``username_pw_set``.  Nothing in
  ORSim or in the OpenRide apps ever calls either one ON A TRANSPORT; they are
  part of paho's surface and are kept here for external callers that reach
  through ``messenger.client``.  See LIFECYCLE CONTRACT below for what replaces
  them.
* **ONE is an assignable attribute**, ``on_message``.

The abstract set is therefore exactly "what the framework invokes".  That
equivalence is deliberate and is pinned by
``tests/test_messenger_transport.py::test_the_abstract_set_is_exactly_the_framework_called_methods``:
making a method abstract that nobody calls forces busywork on implementors,
and leaving a called method non-abstract turns a phase-2 omission into a silent
runtime hang instead of a ``TypeError`` at class-definition time.

Two caveats on "omit one and the class cannot be instantiated".  It holds for
real SUBCLASSES only: ``MessengerTransport.register(SomeAdapter)`` -- the
escape hatch for a transport that wraps a class it cannot inherit from -- makes
``isinstance`` answer ``True`` with no ``__abstractmethods__`` check whatever.
``Messenger.__init__`` therefore re-checks that all six are present and
callable, so a registered adapter missing ``publish`` is refused at
construction rather than at the first agent reply.  And satisfying the six
SIGNATURES was never the bar in the first place -- see the sections below.

Satisfying the six signatures is NOT sufficient.  The six sections below --
DELIVERY SEMANTICS, TOPIC CONTRACT, CONCURRENCY CONTRACT, DURABILITY CONTRACT,
LIFECYCLE CONTRACT and PAYLOAD CONTRACT -- are the rest of the contract, and
each of them describes a way to build a transport that is type-correct,
instantiable, accepted by ``Messenger``, and silently broken at run time.  Read
all six before writing one.

PROSE IS NOT THE AUTHORITY -- RUN THE CONFORMANCE SUITE
-------------------------------------------------------
Everything below is English, and English cannot be machine-checked: three
adversarial review rounds of this file produced 5, then 10, then 16 gaps, a
count that GREW each time because a prose contract has no fixed point.  So the
normative form of this contract is the executable one.  ``orsim`` ships a
reusable transport conformance suite (``orsim.messenger.conformance``); run
your transport against it and treat a failure there as authoritative even where
this docstring is silent, ambiguous or -- since it is maintained by hand --
wrong.  These sections exist to explain WHY each rule is there and what breaks
when it is missed, which is the part a test cannot tell you.  Where a rule
below is checkable, the suite checks it; where it is not (the eventlet
constraint, topic legality against a real broker), the text says so explicitly
and names what you must verify yourself.

DELIVERY SEMANTICS -- BROADCAST, forward-only (read this first)
--------------------------------------------------------------
**Every subscriber of a topic must receive its OWN COPY of every message
published to that topic.**  This is pub/sub fan-out, NOT a work queue: two
transports subscribed to the same topic are not competing consumers, and a
message must never be handed to "one of" them.  MQTT gives this for free.
**Kafka inverts it by default**: ``consumer.subscribe(topics)`` with a shared
``group.id`` -- the natural choice, and the one every Kafka tutorial shows --
makes the consumer group split the partitions between members, so each message
reaches exactly ONE member.

Two call sites depend on the broadcast property, and both fail *silently*
under competing-consumer delivery:

1. ``orsim_scheduler.py::ORSimScheduler.step`` publishes ONE message to
   ``f'{run_id}/{scheduler_id}/ORSimAgent'`` and then awaits a reply from
   **every** agent (``orsim_scheduler.py::ORSimScheduler.confirm_responses``).
   Every agent subscribes to that same topic in
   ``orsim_agent.py::ORSimAgent.start_listening``.  Under group-consumer
   delivery 1 agent of ~1063 receives the step, the barrier never resolves, and
   each step burns the full ``STEP_TIMEOUT``.  Nothing crashes; the run simply
   proceeds at roughly one step per minute and the log fills with "agents still
   waiting" warnings.  A Kafka transport must therefore give each transport
   instance an identity nothing else shares (a unique ``group.id``, or manual
   partition assignment), so that a subscription is a private fan-out, not a
   share of a group's work.
2. ``orsim_scheduler.py::ORSimScheduler.on_receive_message`` -- the reply
   direction -- is the one topic with a single legitimate subscriber, so it
   works either way.  It is named here so the asymmetry is visible: the
   requirement is driven entirely by the step broadcast.

**Delivery is FORWARD-ONLY from the moment of subscribe.**  A subscriber
receives messages published AFTER its ``subscribe()`` returns and no others.
MQTT has no history to replay (retained messages are not used anywhere in
ORSim).  A Kafka consumer with a committed offset does the opposite: on a
celery worker restart mid-run, an agent rejoining would replay step broadcasts
for time steps already past and re-reply for them, corrupting
``agent_collection[...]['step_response']`` for steps the scheduler has closed.
A transport must start at the LATEST offset and must not commit or restore
offsets across a restart.

TOPIC CONTRACT -- the strings are NOT opaque, and Kafka forbids them
--------------------------------------------------------------------
The rest of this file treats a topic as a string that is passed through.  That
is true of MQTT and false of Kafka, and the difference produces the quietest
failure in the whole seam.

**THE EXACT GRAMMAR.**  Three shapes exist, and every one of them contains
``'/'``:

* ``f'{run_id}/{scheduler_id}/ORSimAgent'`` -- the step broadcast.  Published
  by ``orsim_scheduler.py::ORSimScheduler.step``; subscribed by EVERY agent
  (``orsim_agent.py::ORSimAgent.__init__`` registers the handler,
  ``::start_listening`` subscribes).
* ``f'{run_id}/{scheduler_id}/ORSimScheduler'`` -- the reply topic.  Subscribed
  by the scheduler's own Messenger (``ORSimScheduler.__init__`` passes it as
  ``channel_id``); published by every agent
  (``orsim_agent.py::ORSimAgent.handle_orsim_agent_message``).
* ``f'{run_id}/{manager_id}'`` -- ONE PER AGENT, its app topic, registered in
  ``orsim_app.py::ORSimApp.__init__``.  Publishers address these WITHOUT ever
  subscribing to them (e.g.
  ``apps/container_logistics/facility/app.py`` publishes to
  ``f'{run_id}/{truck_id}'``), so a transport cannot learn the topic set from
  its own subscriptions.

**CARDINALITY.**  Order 10**3 topics PER RUN, created implicitly, all of them
first touched at boot: a 1000-truck run needs ~1063 (one per agent plus the two
scheduler topics).  Do not design for a fixed, pre-declared topic list, and do
not assume topic creation is free.  The same number bounds anything else a
transport mints per instance -- note that the unique-identity fix DELIVERY
SEMANTICS asks for (a per-transport ``group.id``) would mint ~1063 consumer
groups per run, which is a failure mode this project has already paid for once
(consumer-group sprawl).  Whatever identity you mint must be cheap to create
AND cheap to reclaim.

**KAFKA REJECTS EVERY ONE OF THESE NAMES, AND NOT AT THE CALL.**  Kafka topic
names are restricted to ``[a-zA-Z0-9._-]``; ``'/'`` is illegal.  Neither
``confluent_kafka.Producer.produce()`` nor ``Consumer.subscribe()`` rejects the
name client-side -- both return normally (reproduced against confluent_kafka
with no broker).  The ONLY signal is an asynchronous delivery report carrying
``INVALID_TOPIC_EXCEPTION``, and PROVENANCE below tells you the publish return
value is not load-bearing.  Net effect if you take that literally: every agent
boots, ``Messenger.__init__`` succeeds, every ``subscribe()`` succeeds, every
``publish()`` succeeds, no message ever moves, the step barrier burns
``STEP_TIMEOUT`` forever and nothing is logged anywhere.  A transport whose
broker restricts topic characters MUST map these names -- and see the next
paragraph, which is the half that makes the obvious mapping fatal.

**TOPIC FIDELITY: what reaches ``on_message`` must be the string that was
published.**  ``message.topic`` must be BYTE-IDENTICAL to the topic passed to
:meth:`publish`, in the framework's own namespace -- not the broker's.  Both
inbound handlers dispatch on exact string equality and NEITHER has an ``else``
branch:

* ``orsim_agent.py::ORSimAgent.on_receive_message``:
  ``if self.message_handlers.get(message.topic) is not None:`` -- a
  non-matching topic falls off the end after the payload has already been
  decoded and ``payload_cache`` / ``start_time`` updated, so the agent looks
  busy and answers nothing.
* ``orsim_scheduler.py::ORSimScheduler.on_receive_message``:
  ``if message.topic == f"{self.run_id}/{self.scheduler_id}/ORSimScheduler":``
  -- a non-matching topic never writes ``step_response`` and never sets
  ``_response_event``, so every step burns the full ``STEP_TIMEOUT``.

So the natural remedy for the paragraph above -- ``topic.replace('/', '.')`` on
the way out, ``msg.topic()`` handed back on the way in -- is a transport in
which every message is received, decoded, and then silently discarded.  If you
mangle, the mapping must be INJECTIVE, TOTAL over the grammar above, applied
identically in :meth:`publish`, :meth:`subscribe` and :meth:`unsubscribe`, and
INVERTED before the message reaches :attr:`on_message`.

**EXISTENCE IS PART OF "IN EFFECT".**  LIFECYCLE CONTRACT point 4 requires a
subscription to be live when :meth:`subscribe` returns.  For a broker with
explicit topics that includes the topic EXISTING: agents subscribe to their own
app topic at boot, long before anything is published to it, and librdkafka
consumers default ``allow.auto.create.topics=false``, so there is nothing to be
assigned to and the subscription silently never becomes live.  Creating it
lazily on first publish does not help either -- a consumer that subscribed
before the topic existed does not see it until its metadata refresh
(``metadata.max.age.ms``, default 300 s).  A transport is responsible for
ensuring the topic exists before it claims the subscription is in effect.

CONCURRENCY CONTRACT -- who calls you, on which thread, in which runtime
------------------------------------------------------------------------
**1. :meth:`subscribe`, :meth:`unsubscribe` and :meth:`publish` are RE-ENTERED
from inside your own :attr:`on_message` callback.**  This is not a corner case;
it is the steady state of every run.  Delivering one step broadcast through
``ORSimAgent.on_receive_message`` re-enters the transport as
``unsubscribe(broadcast_topic)`` (nap), ``subscribe(broadcast_topic)`` (wake)
and ``publish(reply_topic)`` (the step reply), all with the callback still on
the stack -- the app-side call sites are
``openride_agent.py::OpenRideAgent._unsubscribe_broadcast`` and
``::_resubscribe_broadcast``, reached from handlers dispatched by
``on_receive_message``, and ``orsim_agent.py::ORSimAgent.handle_orsim_agent_message``'s
reply publish.

Combined with point 4's "``subscribe()`` must BLOCK until it is live", that is
a deadlock the implementor is otherwise instructed to build: a blocking
subscribe that waits for the consumer's assignment to advance is waiting for a
``poll()`` that only the receive loop makes, and the receive loop is the thread
now blocked inside ``subscribe()``.  Measured in a phase-2 draft: the
re-entrant subscribe blocked for its full 10 s timeout, returned UNSUBSCRIBED
with a log warning, and the agent processed nothing else in the meantime; with
~1063 agents napping and waking throughout a run that is a permanent stall.

**The rule:** blocking activation must be driven by the CALLING thread itself
(mutate the subscription set and drive the assignment on that thread, on a path
that does not depend on the receive loop making progress).  A transport must
never block, inside a call made from :attr:`on_message`, on work that only its
own receive loop can complete.

**2. Nothing may arrive before :attr:`on_message` is set -- and it IS set after
the subscribe.**  ``orsim_agent.py::ORSimAgent.start_listening`` calls
``subscribe(...)`` FIRST and assigns ``self.messenger.client.on_message`` AFTER
it, on a transport whose receive loop is already running
(``Messenger.__init__`` calls ``loop_start`` unconditionally, and agents pass
``channel_id=None``, so their real subscribe happens here).  Under paho that
window is microseconds; a blocking subscribe of the kind point 4 demands widens
it to the group-join latency, hundreds of milliseconds, exactly when the
scheduler is broadcasting to ~1063 agents that are all joining at once.

A transport must NOT consume-and-discard in that window: BUFFER inbound
messages until a callback is assigned (or do not begin consuming until then)
and dispatch them in order once it is.  Dropping them is not a tolerable
choice here, because a dropped message is not redelivered -- the agent never
replies for that step, is counted unresponsive, and is pruned.  (The ordering
in ``start_listening`` is app-visible behaviour and phase 1 does not change it;
the contract absorbs the hazard instead.)

**3. THE RUNTIME IS EVENTLET, AND THE HUB IS SHARED.**  Agents do not run in
processes of their own.  They run as long-lived celery tasks in workers started
with ``--pool eventlet`` (``scripts/run-celery.sh``), so ``eventlet.monkey_patch()``
is in effect and ``threading.Thread`` is a GREENLET: 20-40 co-resident agents
share ONE OS thread, and ``orsim_agent.py::ORSimAgent.start_listening`` spawns
each agent's heartbeat as another greenlet on that same hub.  Greenlets are
COOPERATIVE -- they run until they yield.

Today's transport is safe by accident: paho's ``loop_start`` thread does
eventlet-patched ``select``/``socket`` I/O, so it yields, which is the only
reason 40 agents can share a process at all.  A receive loop that blocks inside
a C extension does not yield, and the monkeypatch cannot make it: measured
under ``eventlet.monkey_patch()``, a loop calling
``confluent_kafka.Consumer.poll(1.0)`` starved a co-resident greenlet to 1
scheduling slice in 3 seconds where the paho loop gave 298 -- ~300x, inflicted
on every other agent in the worker.  The symptom is a slow run with no error:
agents miss the step barrier, the scheduler logs "still waiting after ... s
settle", and the transport is the last place anyone looks.

**The rule:** the receive loop must remain cooperative under eventlet
monkey-patching.  Pure-Python (patched) socket I/O qualifies -- that is what
paho does.  Short non-blocking polls (``poll(0)``) with an ``eventlet.sleep()``
between them qualify.  A genuine OS thread qualifies, but note that by the time
a transport is constructed the monkeypatch is long since applied, so
``threading.Thread`` is NOT one: you must reach the unpatched module
explicitly (``eventlet.patcher.original('threading')``) and then hand messages
back across a queue that the hub side can read without blocking -- and
:attr:`on_message` must still end up called on a greenlet, because the handlers
it invokes re-enter this transport and touch agent state.  A blocking C call on
the hub does not qualify.
**Acceptance bar:** with your receive loop running, a co-resident greenlet must
keep getting scheduled at roughly its unloaded rate.  This one cannot be
asserted from inside the conformance suite's own process without a
monkey-patched interpreter, so measure it explicitly before phase 2 ships.

DURABILITY CONTRACT -- the outbound side has rules too
-------------------------------------------------------
:meth:`publish` may be asynchronous; nothing calls it and then waits on its
return value.  That is a licence to buffer, NOT a licence to lose, and the
distinction is the whole of this section.  This project has already shipped a
run that lost 3,247 events and still reported ``ok: true``; a transport that
drops sends silently reproduces it at the messaging layer, where the effect is
a pruned agent rather than a missing metric.

* **:meth:`disconnect` MUST FLUSH every publish it has already accepted before
  it returns**, and must release send-side resources as well as stopping the
  receive loop.  paho gets this free -- one ordered TCP connection, so a
  QoS-0 packet is on the wire ahead of the DISCONNECT.  A transport with a
  separate producer does not: ``confluent_kafka.Producer``'s destructor PURGES
  its queue, and librdkafka lingers ~5 ms by default.  There is a live call
  site with no delay at all between the two:
  ``openride_agent.py::OpenRideAgent.start_listening`` calls
  ``super().start_listening()`` -- which publishes the agent's boot reply --
  and then, for a failed agent, immediately calls ``stop_listening()`` ->
  ``Messenger.disconnect()`` -> :meth:`disconnect`.  Purge that reply and the
  scheduler never records the agent at all.
* **:meth:`publish` must not raise on transient send-side backpressure.**  No
  caller handles exceptions from it, and the most important one is not even
  inside a ``try``: the agent's step reply in
  ``orsim_agent.py::ORSimAgent.handle_orsim_agent_message`` sits OUTSIDE the
  ``try`` that wraps the step, so an exception there escapes into
  ``on_receive_message`` and into your own receive loop.  A loop that does not
  catch it dies, and the agent is silently dead for the rest of the run.
  ``confluent_kafka.Producer.produce()`` raises ``BufferError`` synchronously
  when its local queue is full, which is precisely when the broker is
  struggling -- so absorb that inside the transport (block briefly, or drop
  with a loud counter), do not let it out.
* **A send that fails MUST be visible somewhere an operator will find it.**
  The return value is not that place; nothing reads it.  Log every delivery
  failure and keep a per-transport dropped-message counter.  ``Messenger.disconnect``
  additionally logs-and-swallows whatever :meth:`disconnect` raises, so a
  teardown-time flush failure that only raises is a failure nobody sees.

LIFECYCLE CONTRACT (read this before writing a transport)
---------------------------------------------------------
``Messenger.__init__`` does NOT connect a transport and does NOT hand it
credentials.  It receives an object it assumes is **already usable** and
immediately assigns ``on_message``, calls ``subscribe`` and calls
``loop_start``.  So:

1. **A transport must be fully connected and authenticated by the time its
   ``__init__`` returns.**  Take the broker address and the credentials through
   your own constructor -- that is what
   :class:`~orsim.messenger.paho_transport.PahoTransport` does
   (``PahoTransport(settings, credentials)`` sets credentials, registers the
   RabbitMQ user and connects, all inside ``__init__``).
2. **Connection failure must PROPAGATE out of ``__init__``.**  An agent whose
   broker is unreachable has to fail loudly at construction; that is today's
   behaviour (``ConnectionRefusedError`` / ``socket.gaierror`` out of
   ``Messenger.__init__``).  Do not defer, do not retry silently.
3. **Do not expect ``connect()`` or ``username_pw_set()`` to be called.**  They
   are optional precisely because the framework never invokes them.  A
   transport that relies on being connected later ends up subscribed, looped
   and published through while still disconnected -- silently, with no
   exception anywhere.
4. **``subscribe()`` arrives at ANY time -- usually AFTER ``loop_start()`` --
   and MUST be in effect by the time it returns.**  The
   ``on_message -> subscribe -> loop_start`` order in ``Messenger.__init__``
   is only what happens when a ``channel_id`` was given.  **Agents pass
   ``channel_id=None``** (``orsim_agent.py::ORSimAgent.__init__``), so at
   ``loop_start`` the subscription set is EMPTY and the real subscribe happens
   later, in ``orsim_agent.py::ORSimAgent.start_listening``, on a transport
   whose receive loop is already running.  It happens again during the run:
   ``openride_agent.py::OpenRideAgent._unsubscribe_broadcast`` and
   ``::_resubscribe_broadcast`` remove and re-add a topic on every nap and
   wake.  Two consequences, both fatal and both silent:

   * **Do not snapshot the subscription set at ``loop_start``.**  A transport
     that builds its consumer there over the then-current topics -- the natural
     Kafka shape -- receives NOTHING, for every agent, for the whole run.
   * **Do not defer activation to a later poll.**
     ``ORSimAgent.start_listening`` subscribes, attaches ``on_message``, and
     then immediately publishes its ``'ready'`` reply and takes its first step.
     paho makes that safe because SUBSCRIBE and PUBLISH share one ordered TCP
     connection.  Kafka's ``consumer.subscribe()`` is a purely LOCAL
     declaration: group join and partition assignment happen on a later
     ``poll()``, typically hundreds of milliseconds afterwards.  An agent that
     announces itself ready before its subscription is live misses the next
     step broadcasts, is counted unresponsive and is pruned.  If activation
     costs a round trip, ``subscribe()`` must BLOCK for it.
   * **...but that blocking must not depend on your receive loop, because
     subscribe() is called FROM it.**  ``subscribe``, ``unsubscribe`` and
     ``publish`` are all re-entered from inside your own ``on_message``
     callback, on the receive thread, every time an agent naps, wakes or
     replies.  Blocking there for something only the receive loop can advance
     is a deadlock.  See CONCURRENCY CONTRACT point 1 for the shape that works.
   * **...and a message that arrives before ``on_message`` is assigned must be
     BUFFERED, not consumed and dropped.**  ``ORSimAgent.start_listening``
     subscribes BEFORE it attaches the callback, and a blocking subscribe
     widens that window from microseconds to the join latency.  See
     CONCURRENCY CONTRACT point 2.
5. **Subscriptions are ADDITIVE and idempotent, ``unsubscribe`` removes exactly
   one topic, and neither may perturb delivery on the OTHER topics -- not even
   transiently.**  See :meth:`subscribe` and :meth:`unsubscribe`.
6. **``disconnect()`` must ALSO stop the receive loop, AND flush pending
   publishes.**  See :meth:`disconnect` and DURABILITY CONTRACT.
7. **ONE transport instance belongs to exactly ONE Messenger.**  A transport
   carries a single ``on_message`` callback and a single connection lifetime,
   so sharing one is not a resource optimisation, it is a data race: the
   second ``Messenger.__init__`` overwrites the first's callback, and a
   message arriving on the SCHEDULER's topic is then handed to the AGENT's
   handler.  Teardown is just as bad -- either owner's ``disconnect()`` closes
   the connection under the other.  ``Messenger.__init__`` refuses a transport
   it finds already bound (``ValueError``); build one transport per Messenger,
   and if pooling is ever wanted, pool the underlying CONNECTION inside the
   transport, not the transport object.

PAYLOAD CONTRACT
----------------
Outbound and inbound are ASYMMETRIC, and the asymmetry is load-bearing:

* **``publish()`` is handed a ``str``.**  Every one of the 12 call sites passes
  ``json.dumps(...)`` output.  A transport whose wire format is bytes must
  encode it (``payload.encode('utf-8')``); paho does this internally, which is
  why no call site has ever done it.
* **``message.payload`` delivered to ``on_message`` must be ``bytes``.**  Both
  handlers -- ``orsim_agent.py::ORSimAgent.on_receive_message`` and
  ``orsim_scheduler.py::ORSimScheduler.on_receive_message`` -- do
  ``json.loads(message.payload.decode('utf-8'))``.  ``.decode`` is the
  operative word: a ``str`` payload raises ``AttributeError: 'str' object has
  no attribute 'decode'``.

So "deliver back exactly what you were given" is WRONG, and it is wrong in the
most expensive way.  On the agent side the receive thread dies on its first
message.  On the OpenRide scheduler side
(``apps/simulation/openride_scheduler.py::OpenRideScheduler.on_receive_message``)
the parse sits inside ``except Exception: logging.warning(...); return``, so
EVERY agent reply is discarded with no traceback and every step times out.

PROVENANCE OF EVERY SIGNATURE
-----------------------------
Each signature below was taken from an actual call site, and the defaults match
paho-mqtt 1.6.1 (pinned in ``orsim/requirements.txt`` and in the app venv) so
that ``PahoTransport``'s forwards are byte-identical to the calls that were
being made before the abstraction existed.

Call sites are cited as ``<file>::<function>`` rather than by line number, on
purpose: line numbers drift the moment anyone edits a docstring in the cited
file -- which is exactly what happened to the first draft of this module, and a
provenance section whose every spot-check misses reads as fabricated.

``username_pw_set(username, password=None)``  -- OPTIONAL
    One caller BEFORE phase 1: ``messenger.py::Messenger.__init__``, in
    KEYWORD form ``username_pw_set(username=..., password=...)``.  After
    phase 1 that caller is
    ``paho_transport.py::PahoTransport.__init__``, which uses its OWN
    wrapped client (``self._client.username_pw_set(...)``), not the interface.
    So there are now ZERO framework calls to this method on a transport, and it
    is non-abstract: credentials arrive through the constructor instead (see
    LIFECYCLE CONTRACT).  It stays declared so external callers holding
    ``messenger.client`` keep a named place to reach.

``connect(host, port=1883, keepalive=60)``  -- OPTIONAL
    One caller BEFORE phase 1: ``messenger.py::Messenger.__init__``, a single
    positional host -- ``connect(settings['MQTT_BROKER'])``.  After phase 1 that
    caller is ``paho_transport.py::PahoTransport.__init__``, again via
    ``self._client``.  ZERO framework calls on a transport today, hence
    non-abstract: a transport is required to be connected by the end of its own
    ``__init__``.  Port and keepalive have never been passed; they are on the
    interface because paho's own default connect takes them and an external
    caller may.

``disconnect()``
    One caller: ``messenger.py::Messenger.disconnect``, zero arguments, return
    ignored.  It is the ONLY teardown an agent performs -- see the method's own
    docstring.

``loop_start()``
    One caller: ``messenger.py::Messenger.__init__``, zero arguments, called
    UNCONDITIONALLY (note the asymmetry with ``loop_stop`` below).

``loop_stop(force=False)``
    One caller: ``messenger.py::Messenger.disconnect``, ``loop_stop(force=True)``
    in KEYWORD form, and only ``if self.channel_id is not None``.  Agents build
    their Messenger with ``channel_id=None``, so agent teardown never calls this
    today -- that asymmetry is load-bearing and is documented here because it
    is invisible at the call site.  (paho 2.x removed ``force``; keeping it in
    the ABC records that coupling in exactly one place.)

``publish(topic, payload=None, qos=0, retain=False)``
    12 call sites across both repos, ALL of them exactly two positional
    arguments ``(topic, json_string)``: ``orsim_agent.py``,
    ``orsim_scheduler.py``, ``apps/simulation/openride_scheduler.py`` (x2),
    ``apps/container_logistics/{openride_agent, assignment/app, facility/app
    (x2), truck/trip_manager}``, ``apps/common/trip_manager_base.py`` and six
    legacy ride-hail modules.  No ``qos``, no ``retain``, no ``properties``
    anywhere.  No call site consumes the return value, so
    ``MQTTMessageInfo`` fidelity is not load-bearing -- ``PahoTransport``
    returns it regardless.  **That is not permission to ignore delivery
    failures**: it means the return value cannot be the place you report them.
    See DURABILITY CONTRACT.  The payload is always a ``str``; see PAYLOAD
    CONTRACT.  The ``topic`` string is also part of the contract -- see TOPIC
    CONTRACT, which is where a Kafka transport dies first.

``subscribe(topic, qos=0)``
    TWO distinct shapes are in use and BOTH must keep flowing unchanged:
    ``subscribe(topic_str, qos=0)`` (``messenger.py::Messenger.__init__``,
    ``apps/container_logistics/openride_agent.py::OpenRideAgent._resubscribe_broadcast``)
    and ``subscribe([(topic, 0), ...])`` -- a bare list of ``(topic, qos)``
    tuples with no ``qos`` keyword (``messenger.py::Messenger.__init__``,
    ``orsim_agent.py::ORSimAgent.start_listening``).  Hence the
    ``Union[str, List[Tuple[str, int]]]`` first parameter with a defaulted
    ``qos``.  A transport that normalises one shape into the other changes what
    the broker is told.

``unsubscribe(topic)``
    One caller:
    ``apps/container_logistics/openride_agent.py::OpenRideAgent._unsubscribe_broadcast``,
    a single positional topic string.

``on_message``
    An assignable ATTRIBUTE, not a method.  Written at two places:
    ``messenger.py::Messenger.__init__`` (``self.client.on_message =
    on_message``) and ``orsim_agent.py::ORSimAgent.start_listening``
    (``self.messenger.client.on_message = self.on_receive_message``).  Both are
    plain attribute writes on ``messenger.client``.  It is declared here as a
    class-level annotated attribute defaulting to ``None`` -- deliberately NOT
    an ``abstractmethod`` and NOT an ``abstractproperty``, because either would
    force every subclass into a property pair and would break the plain
    ``transport.on_message = cb`` assignment those two call sites perform.

This module is pure stdlib on purpose: ``abc`` and ``typing`` only, no paho, no
requests, no RabbitMQ knowledge.  Anything broker-specific belongs in a
concrete transport (see ``paho_transport.py``), which is what keeps a future
Kafka transport from inheriting RabbitMQ's management-API user registration.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, List, Optional, Tuple, Union


class MessengerTransport(ABC):
    """The nine-member contract every ORSim messaging transport must satisfy.

    Six abstract METHODS, two optional ones, and one assignable ATTRIBUTE
    (``on_message``).  That is the complete set of paho members touched
    anywhere in ORSim or in the OpenRide apps, which is what makes it a safe
    seam: satisfy these and ``Messenger`` -- and therefore every agent and
    scheduler -- works.

    MANDATORY (abstract; the framework calls each of them)
        :meth:`disconnect`, :meth:`loop_start`, :meth:`loop_stop`,
        :meth:`publish`, :meth:`subscribe`, :meth:`unsubscribe`.

    OPTIONAL (declared, never called by the framework)
        :meth:`connect` and :meth:`username_pw_set`.  Both default to raising
        ``NotImplementedError`` so that a transport which does not provide them
        fails loudly at an external caller rather than no-opping.

    DELIVERY -- the semantics the step barrier rests on
        **Publish/subscribe is BROADCAST.**  Every subscriber of a topic gets
        its own copy of every message published to it; subscribers are never
        competing consumers sharing a work queue.  ``ORSimScheduler.step``
        publishes ONE message to ``{run_id}/{scheduler_id}/ORSimAgent`` and
        waits for a reply from EVERY agent subscribed to it.  A Kafka consumer
        group -- the default shape -- delivers that message to exactly one
        member, so one agent of ~1063 steps, the barrier never resolves and the
        run stalls at ``STEP_TIMEOUT`` per step without raising anything.
        Delivery is also **forward-only**: a subscriber sees what is published
        after its ``subscribe()`` returns and nothing earlier, so a transport
        must not replay from a committed offset when a worker restarts
        mid-run.  See the module docstring's DELIVERY SEMANTICS section.

    LIFECYCLE -- the part that is easy to get wrong
        ``Messenger.__init__`` never connects a transport and never gives it
        credentials.  It takes the object it is handed, assigns
        ``on_message``, calls :meth:`subscribe`, and calls :meth:`loop_start`.
        Therefore:

        * **Be fully connected and authenticated by the end of your own
          ``__init__``.**  Take the broker address and credentials as
          constructor arguments, the way
          :class:`~orsim.messenger.paho_transport.PahoTransport` takes
          ``(settings, credentials)``.
        * **Let connection failure propagate out of ``__init__``.**  Agents are
          expected to fail loudly at construction when the broker is
          unreachable.
        * **Never wait for :meth:`connect` or :meth:`username_pw_set`.**  A
          transport that does will be subscribed, looped and published through
          while still disconnected, and nothing will raise.
        * **:meth:`subscribe` may be called at any time, including after
          :meth:`loop_start` and repeatedly during the run, and must be in
          effect when it returns.**  Agents construct with ``channel_id=None``,
          so at ``loop_start`` NOTHING is subscribed; the real subscription
          happens later in ``ORSimAgent.start_listening`` and changes again on
          every nap/wake.  Do not snapshot the topic set at ``loop_start`` and
          do not defer activation to a later poll.
        * **Subscriptions ADD; :meth:`unsubscribe` removes one topic; neither
          may disturb delivery on the others.**
        * **:meth:`disconnect` must also stop the receive loop** (agents never
          call :meth:`loop_stop`) **and flush pending publishes.**
        * **One transport instance per Messenger.**  A transport holds one
          ``on_message`` and one connection lifetime; ``Messenger.__init__``
          rejects one that is already bound.

    TOPICS -- not opaque strings
        Every topic in the system contains ``'/'``
        (``{run_id}/{scheduler_id}/ORSimAgent``, ``.../ORSimScheduler``, and
        one ``{run_id}/{agent_id}`` per agent -- order 10**3 per run, created
        implicitly).  Kafka forbids ``'/'`` and rejects them only through an
        asynchronous delivery report nobody reads, so a naive Kafka transport
        boots, subscribes, publishes and moves nothing.  And the obvious
        sanitisation is worse: ``message.topic`` must come back BYTE-IDENTICAL
        to what was published, because both inbound handlers dispatch on string
        equality with no ``else`` branch.  See the module docstring's TOPIC
        CONTRACT.

    CONCURRENCY -- re-entrant, on the receive thread, under eventlet
        :meth:`subscribe`, :meth:`unsubscribe` and :meth:`publish` are called
        from INSIDE your own :attr:`on_message`, so a blocking subscribe must
        not wait on your own receive loop.  A message arriving before
        :attr:`on_message` is assigned must be buffered, not dropped --
        ``ORSimAgent.start_listening`` subscribes first and attaches the
        callback second.  And the whole thing runs in an eventlet
        monkey-patched celery worker where 20-40 agents share one OS thread, so
        a receive loop that blocks in a C extension freezes all of them.  See
        the module docstring's CONCURRENCY CONTRACT.

    DURABILITY -- buffering is allowed, losing is not
        :meth:`publish` may be asynchronous, but :meth:`disconnect` must flush
        what it accepted (there is a call site that publishes and disconnects
        with nothing in between), :meth:`publish` must not raise on transient
        backpressure (its most important call site is outside any ``try``), and
        delivery failures must be logged and counted, because the return value
        is read by nobody.  See the module docstring's DURABILITY CONTRACT.

    Implementors: the receive side is a push model.  Your transport owns its
    own receive loop (started by :meth:`loop_start`, stopped by
    :meth:`loop_stop` OR by :meth:`disconnect`) and invokes
    ``self.on_message(client, userdata, message)`` from it, with paho's
    three-argument callback signature, where ``message`` exposes ``.topic``
    (``str``) and ``.payload`` (**``bytes``**).  ORSim's handlers do
    ``json.loads(message.payload.decode('utf-8'))`` and read ``message.topic``;
    nothing else on the message object is used.  Note that :meth:`publish` is
    handed a ``str`` while ``.payload`` must arrive as ``bytes`` -- see the
    module docstring's PAYLOAD CONTRACT.

    **Validate by RUNNING, not by reading.**  These docstrings are prose and
    have needed three adversarial review rounds already; the executable form of
    the contract is ``orsim.messenger.conformance``.  Run your transport
    against it, and treat its verdict as authoritative.
    """

    #: Callback invoked by the transport's own receive loop with paho's
    #: signature ``(client, userdata, message)``.  Declared as a class-level
    #: attribute defaulting to ``None`` so that ``transport.on_message = cb``
    #: is a plain assignment.
    #:
    #: THE ONE SUPPORTED CONTRACT: a read of ``transport.on_message`` must
    #: return the callback that was last assigned.  A transport that stores
    #: the callback anywhere other than its own
    #: ``self.__dict__['on_message']`` -- forwarding the write through
    #: ``__setattr__`` to a wrapped client, for instance -- MUST therefore also
    #: provide a read-through PROPERTY, as
    #: :class:`~orsim.messenger.paho_transport.PahoTransport` does.  Forwarding
    #: the write alone is not enough and fails silently: this class attribute
    #: (``None``) shadows ``__getattr__``, so every READ of ``self.on_message``
    #: inside your own receive loop answers ``None`` and every inbound message
    #: is dropped with nothing raised.  Pinned by
    #: ``tests/test_messenger_transport.py::test_a_setattr_only_transport_silently_drops_every_message``.
    #:
    #: IT IS ASSIGNED *AFTER* THE SUBSCRIBE, on a running loop.
    #: ``orsim_agent.py::ORSimAgent.start_listening`` calls ``subscribe(...)``
    #: and only then ``...client.on_message = self.on_receive_message``, and the
    #: receive loop has been running since ``Messenger.__init__``.  A message
    #: that arrives while this is still ``None`` must be BUFFERED and dispatched
    #: once a callback is assigned -- consuming and discarding it is not
    #: redeliverable, so the agent never replies for that step, is counted
    #: unresponsive and is pruned.  Under paho the window is microseconds; the
    #: blocking ``subscribe()`` that LIFECYCLE point 4 demands widens it to the
    #: broker's join latency, at boot, for ~1063 agents at once.  See the module
    #: docstring's CONCURRENCY CONTRACT, point 2.
    #:
    #: THE CALL IS RE-ENTRANT.  Whatever is assigned here calls back into
    #: :meth:`subscribe`, :meth:`unsubscribe` and :meth:`publish` before it
    #: returns; see CONCURRENCY CONTRACT, point 1.
    on_message: Optional[Callable[[Any, Any, Any], None]] = None

    # ------------------------------------------------------------------
    # OPTIONAL members.  NOT abstract, because nothing in the framework
    # calls either one on a transport -- see the class docstring's LIFECYCLE
    # section and the module docstring's PROVENANCE entries.
    # ------------------------------------------------------------------

    def username_pw_set(self, username: str, password: Optional[str] = None) -> None:
        """OPTIONAL.  Set credentials for a LATER :meth:`connect`.

        **The framework never calls this.**  ``Messenger`` does not hand a
        transport credentials at all; a transport takes them through its own
        constructor (see the class docstring's LIFECYCLE section).  The method
        survives on the interface only because ``messenger.client`` is public
        and an external caller may reach for the paho-shaped name.

        The default implementation raises ``NotImplementedError``, so a
        transport without credential support fails at the caller instead of
        silently no-opping.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement username_pw_set(); "
            "credentials are supplied to a transport through its constructor."
        )

    def connect(self, host: str, port: int = 1883, keepalive: int = 60) -> Any:
        """OPTIONAL.  Establish a connection to the broker at ``host``.

        **The framework never calls this.**  A transport must already be
        connected when ``Messenger`` receives it -- ``Messenger.__init__``
        goes straight to ``on_message`` / :meth:`subscribe` /
        :meth:`loop_start`.  Connect inside your own ``__init__`` and let
        failure propagate from there (that is what produces today's
        ``ConnectionRefusedError`` / ``socket.gaierror`` out of
        ``Messenger.__init__``).

        The default implementation raises ``NotImplementedError``, so a
        transport that has no separate connect step fails at the caller rather
        than returning as if it had connected.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement connect(); a transport "
            "must be connected by the end of its own __init__."
        )

    # ------------------------------------------------------------------
    # MANDATORY members.  Each of the six is invoked by Messenger, by an
    # ORSim agent/scheduler, or by the OpenRide apps.
    # ------------------------------------------------------------------

    @abstractmethod
    def disconnect(self) -> Any:
        """Close the broker connection, stop the receive loop, FLUSH the sends.

        Return value is ignored by callers.

        **It must flush the outbound side too, and release its resources.**
        The rest of this docstring is about the receive loop, which is the half
        paho makes obvious; the send half is the half that loses data.  Every
        publish this transport has already accepted must be on the wire before
        :meth:`disconnect` returns, and any send-side handle must be closed
        rather than left to a destructor.  ``confluent_kafka.Producer`` PURGES
        its queue on destroy and librdkafka lingers ~5 ms by default, so "close
        the consumer, stop the loop" loses the agent's last messages -- and
        there is a call site with nothing in between:
        ``openride_agent.py::OpenRideAgent.start_listening`` publishes the boot
        reply through ``super().start_listening()`` and then, for a failed
        agent, immediately calls ``stop_listening()`` ->
        ``Messenger.disconnect()`` -> here.  Note also that
        ``messenger.py::Messenger.disconnect`` logs-and-swallows whatever this
        raises, so a flush failure that only raises is a flush failure nobody
        sees: log and count it here.  See the module docstring's DURABILITY
        CONTRACT.

        **This is the whole of an agent's teardown, so it must release the
        receive loop too.**  ``messenger.py::Messenger.disconnect`` calls
        :meth:`loop_stop` only ``if self.channel_id is not None``, and agents
        build their Messenger with ``channel_id=None``
        (``orsim_agent.py::ORSimAgent.__init__``) -- so for every agent in
        every run, ``disconnect()`` is the ONLY teardown call that ever
        arrives.  paho satisfies this by accident: ``loop_forever``'s
        ``should_exit()`` observes the disconnecting state and the network
        thread ends on its own.

        An implementor who reads the two method names literally -- consumer
        close in ``disconnect()``, thread join in ``loop_stop()`` -- leaks a
        poll thread per agent, spinning on a closed consumer or holding group
        membership forever.  Celery workers here are long-lived (64 processes,
        ~1063 agents per run, agents entering and leaving throughout), so that
        leak accumulates across runs until a worker restart.  Make
        :meth:`disconnect` idempotent with :meth:`loop_stop`: either may be
        called first, either may be called alone, and neither may raise if the
        other already ran.
        """

    @abstractmethod
    def loop_start(self) -> Any:
        """Start the background receive loop that drives :attr:`on_message`.

        Called UNCONDITIONALLY by ``messenger.py::Messenger.__init__``, and --
        for agents -- while NOTHING is yet subscribed AND while
        :attr:`on_message` is still ``None``.  Do not treat this as the moment
        the topic set is known, and do not consume-and-discard before a
        callback exists: see :meth:`subscribe`, the module docstring's
        LIFECYCLE CONTRACT point 4, and CONCURRENCY CONTRACT point 2.

        **The loop you start here shares an OS thread with 20-40 other
        agents.**  Agents run in celery workers launched with
        ``--pool eventlet``, so ``eventlet.monkey_patch()`` is in effect and a
        ``threading.Thread`` here is a greenlet on a hub that every co-resident
        agent -- and every agent's heartbeat greenlet -- depends on.  A loop
        that blocks inside a C extension never yields and freezes all of them,
        with no exception and no log line.  See CONCURRENCY CONTRACT, point 3,
        for the acceptance bar and the three loop shapes that meet it.
        """

    @abstractmethod
    def loop_stop(self, force: bool = False) -> Any:
        """Stop the background receive loop.

        Called as ``loop_stop(force=True)``, and only when the Messenger was
        built with a ``channel_id``.  See the module docstring for why that
        asymmetry with :meth:`loop_start` must not be "corrected", and
        :meth:`disconnect` for what an agent -- which never reaches here --
        relies on instead.
        """

    @abstractmethod
    def publish(self, topic: str, payload: Any = None, qos: int = 0, retain: bool = False) -> Any:
        """Publish ``payload`` to ``topic``, BROADCAST to every subscriber.

        Every call site in both repositories passes exactly two positional
        arguments and ignores the return value; ``qos`` and ``retain`` exist
        for parity with paho, not because anything uses them.

        ``payload`` is always a ``str`` -- ``json.dumps(...)`` output at all 12
        call sites.  **The round trip is asymmetric**: a transport whose wire
        format is bytes must encode it here, because the same body has to
        arrive at :attr:`on_message` as ``message.payload`` of type ``bytes``
        (both handlers call ``.decode('utf-8')`` on it).  Handing back the
        ``str`` it was given raises ``AttributeError: 'str' object has no
        attribute 'decode'`` -- which kills the agent's receive thread on its
        first message, and on the OpenRide scheduler side is swallowed by an
        ``except Exception`` so that every reply vanishes and every step times
        out.  See the module docstring's PAYLOAD CONTRACT.

        Delivery must reach EVERY subscriber of ``topic``, not one of them;
        the step barrier depends on it.  See DELIVERY SEMANTICS.

        ``topic`` is a framework string, and every one of them contains ``'/'``
        -- illegal in Kafka, and rejected only by an asynchronous delivery
        report.  If your broker needs the name mangled, mangle it injectively
        and invert it before the message reaches :attr:`on_message`, because
        ``message.topic`` must arrive byte-identical to what was published.
        See TOPIC CONTRACT.

        **May be called RE-ENTRANTLY, from inside your own**
        :attr:`on_message` -- the agent's step reply is published from the
        handler dispatched by ``ORSimAgent.on_receive_message``.  Do not block
        it on your receive loop.  See CONCURRENCY CONTRACT, point 1.

        **Failure semantics.**  ``publish`` may be asynchronous, and the return
        value is read by nobody -- but it must NOT raise on transient send-side
        backpressure.  Its most important call site, the step reply in
        ``orsim_agent.py::ORSimAgent.handle_orsim_agent_message``, sits OUTSIDE
        the ``try`` that wraps the step, so an exception escapes into
        ``on_receive_message`` and into your own receive loop; a loop that does
        not catch it dies and the agent goes silently dead for the rest of the
        run.  (``confluent_kafka.Producer.produce()`` raises ``BufferError``
        synchronously on a full local queue -- absorb it.)  Conversely, a send
        that never arrives must be logged and counted, because the return value
        cannot report it.  See DURABILITY CONTRACT.
        """

    @abstractmethod
    def subscribe(self, topic: Union[str, List[Tuple[str, int]]], qos: int = 0) -> Any:
        """Subscribe to a single topic string, or to a list of ``(topic, qos)``.

        Both shapes are live in the codebase (see the module docstring).  An
        implementation must accept both and must not rewrite one into the
        other.

        THREE semantics beyond the signature, each of which fails silently:

        * **ADDITIVE.**  ``subscribe`` ADDS to the current subscription set; it
          never replaces it.  Kafka's ``consumer.subscribe(topics)`` does the
          opposite, and the difference is fatal here.  An agent subscribes to
          its whole handler table in one call
          (``orsim_agent.py::ORSimAgent.start_listening``); during a nap
          ``openride_agent.py::OpenRideAgent._unsubscribe_broadcast`` drops the
          step-broadcast topic, and on wake ``::_resubscribe_broadcast`` re-adds
          it alone.  Against a REPLACING transport the set ends as just
          ``{...ORSimAgent}`` -- **the agent's own app topic is gone**.  The
          wake path is itself driven by an app-topic event, so the agent can
          never be woken again and every assignment/dispatch message addressed
          to it is dropped for the rest of the run, with no error anywhere.
          Re-subscribing to a topic already held is a no-op, not a duplicate
          delivery: it must be IDEMPOTENT.

          **"Additive" is necessary but NOT sufficient: adding or removing one
          topic must not perturb delivery on the others, not even
          transiently.**  A transport can satisfy the letter of the rule --
          keep your own set, re-declare the WHOLE set on every change, which is
          what "additive" invites -- and still lose messages.  With Kafka's
          default eager assignor, changing the subscription set revokes ALL
          partitions and rejoins; with no committed offsets (which the
          FORWARD-ONLY rule mandates) the re-assigned partitions resume at
          ``auto.offset.reset=latest``, so anything published to the agent's
          APP topic during the rebalance window is skipped -- including the
          wake event.  Every nap does this, ~1063 agents nap and wake
          repeatedly per run.  Retain your read position across a subscription
          change: that rules IN incremental assignment
          (``incremental_assign`` / ``incremental_unassign``) and rules OUT
          re-declaring a subscription set.
        * **THE TOPIC MUST EXIST** before you may claim the subscription is in
          effect.  Agents subscribe to their own app topic long before anything
          publishes to it, and a consumer that subscribes to a not-yet-existing
          topic gets an empty assignment, no error, and no visibility until its
          metadata refresh.  See TOPIC CONTRACT.
        * **RE-ENTRANT.**  Called from inside your own :attr:`on_message`
          (``openride_agent.py::OpenRideAgent._resubscribe_broadcast``, on the
          wake path), so whatever blocking this method does must be driven by
          the calling thread and must not wait on the receive loop.  See
          CONCURRENCY CONTRACT, point 1 -- this is the deadlock the "IMMEDIATE"
          rule below would otherwise create.
        * **``message.topic`` must round-trip.**  If you mangle topic names for
          the broker, invert the mangling before dispatch; both inbound
          handlers dispatch on exact string equality with no ``else`` branch.
          See TOPIC CONTRACT.
        * **BROADCAST.**  The subscription gives THIS transport its own
          independent copy of every message published to the topic -- pub/sub
          fan-out, not a competing-consumer share of a work queue.  See
          DELIVERY SEMANTICS.
        * **IMMEDIATE, at any time.**  It may be called after
          :meth:`loop_start` and repeatedly during the run, and it must be in
          effect by the time it returns -- callers publish and expect replies
          within microseconds of it.  See LIFECYCLE CONTRACT, point 4.  If it
          blocks for activation, note that a message may land before
          :attr:`on_message` has been assigned (``start_listening`` subscribes
          FIRST): buffer it, do not consume and drop it.  See CONCURRENCY
          CONTRACT, point 2.
        """

    @abstractmethod
    def unsubscribe(self, topic: str) -> Any:
        """Unsubscribe from a single topic string.

        Removes EXACTLY that one topic and leaves every other subscription
        untouched; the receive loop keeps running and keeps delivering on the
        rest.  Its one caller
        (``apps/container_logistics/openride_agent.py::OpenRideAgent._unsubscribe_broadcast``)
        drops only the step-broadcast topic when an agent naps, and depends on
        the agent's app topics still delivering -- that is what wakes it.  See
        :meth:`subscribe` for what a replacing implementation destroys here.

        **Removing one topic must not disturb the others even MOMENTARILY.**
        An implementation that re-declares its whole subscription set here
        satisfies "removes exactly one topic" and still drops the wake event,
        because the rebalance it triggers revokes and re-seeks every other
        partition.  See :meth:`subscribe`'s ADDITIVE bullet.

        **RE-ENTRANT**: its one caller runs inside your own :attr:`on_message`
        (``_unsubscribe_broadcast`` is reached from the handler dispatched by
        ``ORSimAgent.on_receive_message``), so it must not block on work only
        the receive loop can complete.  See CONCURRENCY CONTRACT, point 1.

        ``topic`` is the framework's string; if you mangle names for the
        broker, mangle it here identically to :meth:`subscribe` and
        :meth:`publish`, or the removal silently misses.  See TOPIC CONTRACT.
        """
