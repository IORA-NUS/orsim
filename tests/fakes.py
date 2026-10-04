"""Test doubles for the ``MessengerTransport`` abstraction.

WHY THIS MODULE EXISTS
----------------------
Every test in this suite must pass on a machine with **no RabbitMQ, no MQTT
broker and no network**.  The pre-existing ``tests/test_orsim.py`` needed a live
broker, which is precisely why it was never run and why its collection error sat
unnoticed until the transport work.  A transport abstraction whose tests need a
broker would be self-defeating: the point of the seam is that the nine-member
contract can be satisfied by something that does no I/O at all.

:class:`FakeTransport` is that something.  It implements the six abstract
methods and the two optional ones, plus the ``on_message`` attribute, records
every interaction on a single ordered timeline, and never opens a socket.

WHAT IS DELIBERATE HERE
-----------------------
1. **Every method takes ``*args, **kwargs``.**  The recorded tuple is what the
   CALLER actually passed, not what a normalising signature would have made of
   it.  That is the whole point: assertions like
   ``('subscribe', ('chan',), {'qos': 0})`` vs ``('subscribe', ([('a', 0), ('b', 0)],), {})``
   can only distinguish the two live subscribe shapes if the fake preserves them
   verbatim.  Giving the fake paho-shaped defaults would silently normalise the
   difference away and the tests would stop being able to see it.

2. **``on_message`` assignment lands on the SAME timeline as the calls.**
   ``Messenger.__init__`` assigns the callback before it subscribes, and that
   order matters (a callback attached after the subscription can miss messages
   that a real broker delivered in between).  Ordering between an attribute
   write and a method call cannot be expressed with two separate lists, so
   ``__setattr__`` appends ``('on_message', (value,), {})`` to ``calls``.

3. **``on_message`` is NOT pre-seeded in ``__init__``.**  It resolves to the
   ``MessengerTransport`` class attribute (``None``) until something assigns it.
   ``'on_message' not in fake.__dict__`` is therefore the machine-checkable form
   of "the messenger never touched the callback", which is what the
   ``on_message=None`` case has to prove.

4. **``raise_on`` records BEFORE it raises.**  ``Messenger.disconnect`` swallows
   exceptions from both ``loop_stop`` and ``disconnect``; proving it *attempted*
   both requires the call to appear on the timeline even though it blew up.

5. **ONE FakeTransport PER MESSENGER.**  A transport carries a single
   ``on_message`` and a single connection lifetime, so sharing one across
   Messengers is a data race, and ``Messenger.__init__`` now refuses it
   (``ValueError``).  This suite used to hand one instance to three Messengers,
   which modelled the hazard rather than the contract.  Build a fresh one.
"""

import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest import mock

from orsim.messenger.transport import MessengerTransport


# --- Fixture data -----------------------------------------------------------
# Shaped exactly like the real thing: the four keys ORSimEnv's
# messenger_backend_settings_schema requires, and the {'email', 'password'}
# credentials pair ORSimAgent/ORSimScheduler build for themselves.  None of
# these hosts is ever contacted -- paho and requests are always patched out.

SETTINGS = {
    'RABBITMQ_MANAGEMENT_SERVER': 'http://broker.invalid:15672/api',
    'RABBITMQ_ADMIN_USER': 'admin_user',
    'RABBITMQ_ADMIN_PASSWORD': 'admin_pw',
    'MQTT_BROKER': 'broker.invalid',
}

CREDENTIALS = {
    'email': 'agent@example.invalid',
    'password': 'secret_password',
}


class FakeMessage:
    """The two attributes the two ``on_receive_message`` callbacks actually read.

    ``ORSimAgent.on_receive_message`` reads ``message.payload`` (and dispatches on
    ``message.topic`` via the handler table); ``ORSimScheduler.on_receive_message``
    compares ``message.topic`` and decodes ``message.payload``.  Nothing else on
    paho's ``MQTTMessage`` is touched anywhere in either repository, so nothing
    else is modelled.

    Args:
        topic (str): the topic the message arrived on.
        payload (bytes): the raw body; both callbacks call ``.decode('utf-8')``
            on it, so it must be ``bytes``, not ``str``.
    """

    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload

    def __repr__(self):
        return f"FakeMessage(topic={self.topic!r}, payload={self.payload!r})"


class FakeTransport(MessengerTransport):
    """A zero-I/O :class:`MessengerTransport` that records what was asked of it.

    Attributes:
        calls (list): ordered timeline of ``(name, args, kwargs)``.  Includes
            ``('on_message', (callback,), {})`` entries for callback assignment
            -- see this module's docstring, point 2.
        published (list): ``(topic, payload)`` for each :meth:`publish`.
        subscriptions (list): ``(args, kwargs)`` for each :meth:`subscribe`.
        raise_on (set): names that should raise ``RuntimeError`` when called,
            after being recorded.
    """

    def __init__(self, raise_on=None):
        # 'calls' first: __setattr__ consults it, so nothing may be assigned
        # before it exists.
        self.calls = []
        self.published = []
        self.subscriptions = []
        self.raise_on = set(raise_on or ())

    # -- recording ----------------------------------------------------------

    def _record(self, name, args, kwargs):
        self.calls.append((name, args, kwargs))
        if name in self.raise_on:
            raise RuntimeError(f"FakeTransport: {name} configured to fail")

    def __setattr__(self, name, value):
        if name == 'on_message' and 'calls' in self.__dict__:
            self.calls.append(('on_message', (value,), {}))
        object.__setattr__(self, name, value)

    @property
    def call_names(self):
        """The timeline reduced to names -- for order-only assertions."""
        return [name for name, _, _ in self.calls]

    # -- the nine members ---------------------------------------------------
    # on_message is inherited from MessengerTransport as a plain class
    # attribute defaulting to None; it is an ATTRIBUTE, not a method, and is
    # deliberately not redeclared here.

    def username_pw_set(self, *args, **kwargs):
        self._record('username_pw_set', args, kwargs)

    def connect(self, *args, **kwargs):
        self._record('connect', args, kwargs)

    def disconnect(self, *args, **kwargs):
        self._record('disconnect', args, kwargs)

    def loop_start(self, *args, **kwargs):
        self._record('loop_start', args, kwargs)

    def loop_stop(self, *args, **kwargs):
        self._record('loop_stop', args, kwargs)

    def publish(self, *args, **kwargs):
        topic = args[0] if args else kwargs.get('topic')
        payload = args[1] if len(args) > 1 else kwargs.get('payload')
        self.published.append((topic, payload))
        self._record('publish', args, kwargs)

    def subscribe(self, *args, **kwargs):
        self.subscriptions.append((args, kwargs))
        self._record('subscribe', args, kwargs)

    def unsubscribe(self, *args, **kwargs):
        self._record('unsubscribe', args, kwargs)

    # -- inbound simulation -------------------------------------------------

    def deliver(self, topic, payload):
        """Invoke the registered callback as a broker's receive loop would.

        Mirrors paho's ``on_message(client, userdata, message)`` signature with
        the transport standing in for the client and ``None`` for userdata --
        neither callback in either repository reads those two arguments.

        Args:
            topic (str): topic to deliver on.
            payload (dict): JSON-serialised into the message body, because both
                callbacks do ``json.loads(message.payload.decode('utf-8'))``.
        """
        if self.on_message is None:
            raise AssertionError("deliver() called before on_message was assigned")
        message = FakeMessage(topic, json.dumps(payload).encode('utf-8'))
        return self.on_message(self, None, message)


@contextmanager
def patched_broker(status_code=404, manager=None):
    """Patch out paho and requests inside ``orsim.messenger.paho_transport``.

    ``PahoTransport.__init__`` constructs a paho client, sets credentials,
    performs the RabbitMQ management-API registration and connects -- four real
    side effects.  Patching the two modules it imports neutralises all four
    while leaving the *sequence* observable, which is what the ordering test
    asserts.

    ``status_code`` must be a real integer: ``register_rabbitmq_user`` evaluates
    ``response.status_code >= 200``, and a bare ``MagicMock`` there raises
    ``TypeError`` rather than taking either branch.

    Args:
        status_code (int): what the users GET reports. 404 -> the user-creation
            PUT fires (three PUTs total); 200 -> it does not (two PUTs).
        manager (mock.Mock, optional): if given, both mocks are attached to it
            so that ``manager.mock_calls`` carries ONE cross-object ordering.
    """
    with mock.patch('orsim.messenger.paho_transport.paho') as paho_mock, \
         mock.patch('orsim.messenger.paho_transport.requests') as requests_mock:
        if manager is not None:
            manager.attach_mock(paho_mock, 'paho')
            manager.attach_mock(requests_mock, 'requests')
        requests_mock.get.return_value.status_code = status_code
        yield SimpleNamespace(paho=paho_mock, requests=requests_mock)
