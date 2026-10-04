"""``Messenger``'s two paths: the injected transport, and the legacy four-arg form.

The four-argument test at the bottom (:func:`test_legacy_four_arg_constructor_builds_a_paho_transport`)
is the one to point at in review.  ``orsim`` has been on PyPI since 0.1.2 and the
ride-hail ecosystem still imports it, so ``Messenger(settings, credentials,
channel_id, on_message)`` must behave exactly as it did before the abstraction
existed.  Everything else here tests the new capability; that one tests that the
old one did not move.
"""

import json
from unittest import mock

import pytest

from orsim.messenger import Messenger
from orsim.messenger.paho_transport import PahoTransport
from orsim.messenger.transport import MessengerTransport

from .fakes import CREDENTIALS, SETTINGS, FakeTransport, patched_broker


# ---------------------------------------------------------------------------
# The ABC is a real contract
# ---------------------------------------------------------------------------
# A "transport abstraction" that any object satisfies is not an abstraction.
# These guard the three properties the seam depends on: SIX methods are
# MANDATORY, TWO are optional, and on_message is an ATTRIBUTE rather than a
# ninth method.
#
# Round 1 of this work tested exactly ONE mandatory name (unsubscribe), which
# was enough to look convincing and not enough to enforce anything: deleting
# the @abstractmethod line above ``def publish(`` -- and nothing else -- left
# the whole suite green at 51 passed, while a transport omitting publish would
# instantiate happily and drop every agent step reply, hanging the run at the
# scheduler barrier with no exception anywhere.  The set is now pinned whole,
# and every member of it is exercised individually.

#: The methods the framework ACTUALLY invokes on a transport, and therefore
#: exactly the set the ABC must refuse to let an implementor omit.  Keep this
#: in step with the "MANDATORY" list in MessengerTransport's docstring.
MANDATORY_MEMBERS = frozenset({
    'disconnect',      # Messenger.disconnect
    'loop_start',      # Messenger.__init__
    'loop_stop',       # Messenger.disconnect, force=True
    'publish',         # 12 call sites across both repos
    'subscribe',       # Messenger.__init__ (both shapes), ORSimAgent
    'unsubscribe',     # apps/container_logistics/openride_agent.py
})

#: Declared on the interface, never called by the framework.  Non-abstract on
#: purpose -- see MessengerTransport's LIFECYCLE section: a transport is
#: required to be connected and authenticated by the end of its own __init__,
#: because Messenger goes straight to on_message / subscribe / loop_start.
OPTIONAL_MEMBERS = frozenset({'connect', 'username_pw_set'})


def _stub_transport(members):
    """Build a MessengerTransport subclass defining exactly ``members``."""
    body = {name: (lambda self, *a, **k: None) for name in members}
    return type('StubTransport', (MessengerTransport,), body)


def test_messenger_transport_cannot_be_instantiated():
    with pytest.raises(TypeError):
        MessengerTransport()


def test_the_abstract_set_is_exactly_the_framework_called_methods():
    """THE pin.  Not a sample of the contract -- the whole of it.

    Two directions of failure, both caught here.  Removing an @abstractmethod
    (the round-1 defect) shrinks the set and a phase-2 transport omitting that
    method becomes a silent runtime hang instead of a TypeError at class
    definition.  Adding one for a method nobody calls (``connect``,
    ``username_pw_set``) grows the set and forces every implementor to write a
    method the framework will never invoke -- which is how round 1 came to
    document a connect() lifecycle that does not exist.
    """
    assert MessengerTransport.__abstractmethods__ == MANDATORY_MEMBERS


@pytest.mark.parametrize('omitted', sorted(MANDATORY_MEMBERS))
def test_a_transport_omitting_any_mandatory_method_cannot_be_instantiated(omitted):
    """Every mandatory name, one at a time -- not just ``unsubscribe``."""
    incomplete = _stub_transport(MANDATORY_MEMBERS - {omitted})

    assert incomplete.__abstractmethods__ == frozenset({omitted})
    with pytest.raises(TypeError):
        incomplete()


def test_a_transport_defining_only_the_mandatory_six_is_instantiable():
    """The complement of the test above: six is ENOUGH.

    This is what makes the optional pair genuinely optional rather than
    merely undocumented -- a Kafka transport with no connect() and no
    username_pw_set() is a legal transport.
    """
    minimal = _stub_transport(MANDATORY_MEMBERS)

    assert minimal.__abstractmethods__ == frozenset()
    assert isinstance(minimal(), MessengerTransport)


@pytest.mark.parametrize('optional', sorted(OPTIONAL_MEMBERS))
def test_the_optional_members_are_declared_but_not_abstract(optional):
    assert optional not in MessengerTransport.__abstractmethods__
    assert callable(getattr(MessengerTransport, optional))


@pytest.mark.parametrize('optional', sorted(OPTIONAL_MEMBERS))
def test_an_unimplemented_optional_member_raises_rather_than_no_opping(optional):
    """A default ``pass`` here would be the very bug F1 described.

    ``Messenger`` never calls these, so an external caller is the only one who
    can reach them.  Returning None as if the work had been done is how a
    transport ends up "connected" while disconnected; NotImplementedError names
    the class instead.
    """
    minimal = _stub_transport(MANDATORY_MEMBERS)()

    with pytest.raises(NotImplementedError) as excinfo:
        getattr(minimal, optional)('arg')

    assert 'StubTransport' in str(excinfo.value)


class HalfTransport:
    """Duck-typed, NOT a MessengerTransport: only the two members F4 named.

    Deliberately missing ``publish`` -- the member whose absence is invisible
    until the first agent reply is due and then hangs the step barrier.
    """

    on_message = None

    def loop_start(self, *args, **kwargs):
        return None


def test_a_duck_typed_transport_is_rejected_at_construction():
    """M4: the ABC is enforced WHERE IT IS CONSUMED, not merely declared.

    Before this check, ``HalfTransport()`` constructed a Messenger happily --
    "Messenger built OK, isinstance: False, has publish? False" -- because
    nothing ever asked. The failure then surfaced as an ``AttributeError``
    inside a receive callback on the first publish: the agent never replies, the
    scheduler burns STEP_TIMEOUT, and no traceback points at the transport.
    ``disconnect()`` hid it further by swallowing the same AttributeError.
    """
    with patched_broker():
        with pytest.raises(TypeError) as excinfo:
            Messenger(SETTINGS, CREDENTIALS, 'chan', transport=HalfTransport())

    message = str(excinfo.value)
    assert 'MessengerTransport' in message
    assert 'HalfTransport' in message


def test_disconnect_no_longer_absorbs_a_missing_transport_method():
    """The other half of M4: teardown must not report success on a broken transport.

    ``Messenger.disconnect`` swallows exceptions from ``loop_stop`` and
    ``disconnect`` so agent teardown cannot raise -- but it used to swallow the
    ``AttributeError`` from the method being ABSENT too, which is a broken
    transport rather than a broker hiccup. The lookups now happen outside the
    ``try``; failures raised BY the calls are still swallowed
    (``test_disconnect_swallows_failures_from_both_calls_and_attempts_both``).
    """
    class VanishingTransport(FakeTransport):
        """A throwaway subclass so the deletion cannot leak into other tests."""

    transport = VanishingTransport()
    with patched_broker():
        messenger = Messenger(SETTINGS, CREDENTIALS, 'chan', transport=transport)

    # Shadow the inherited method with something that is not there. The
    # constructor guard makes this unreachable in practice, which is the point:
    # the narrowing is belt-and-braces, and it must still not lie.
    VanishingTransport.loop_stop = property(
        lambda self: (_ for _ in ()).throw(AttributeError('loop_stop')))

    with pytest.raises(AttributeError):
        messenger.disconnect()


def test_a_registered_virtual_subclass_missing_a_mandatory_method_is_rejected():
    """isinstance() alone is NOT enforcement: ABCMeta.register bypasses it.

    A phase-2 adapter that wraps an existing client class cannot inherit, so the
    documented escape hatch is ``MessengerTransport.register(Adapter)``. Python
    performs NO ``__abstractmethods__`` check on a virtual subclass, so an
    adapter that forgot ``publish`` answered ``isinstance(...) is True``,
    constructed a Messenger cleanly, and raised ``AttributeError`` on the first
    agent reply -- inside a receive callback, where
    ``OpenRideScheduler.on_receive_message``'s ``except Exception`` swallows it.
    Every step then times out with no traceback at the mistake, which is exactly
    what the isinstance guard's own comment claims to prevent.
    """
    class RegisteredAdapter:
        """Everything except publish -- the invisible-until-the-barrier omission."""

        on_message = None

        def disconnect(self, *a, **k): return None
        def loop_start(self, *a, **k): return None
        def loop_stop(self, *a, **k): return None
        def subscribe(self, *a, **k): return None
        def unsubscribe(self, *a, **k): return None

    MessengerTransport.register(RegisteredAdapter)
    adapter = RegisteredAdapter()
    # The hole, stated: the ABC says yes.
    assert isinstance(adapter, MessengerTransport)

    with patched_broker():
        with pytest.raises(TypeError) as excinfo:
            Messenger(SETTINGS, CREDENTIALS, 'chan', transport=adapter)

    message = str(excinfo.value)
    assert 'RegisteredAdapter' in message
    assert 'publish' in message


def test_a_registered_virtual_subclass_with_all_six_is_accepted():
    """The complement: the escape hatch still works when it is used correctly.

    The presence check must not turn ``register()`` into a dead end -- a wrapper
    around a third-party client is a legitimate way to build a transport.
    """
    class CompleteAdapter:
        on_message = None

        def disconnect(self, *a, **k): return None
        def loop_start(self, *a, **k): return None
        def loop_stop(self, *a, **k): return None
        def publish(self, *a, **k): return None
        def subscribe(self, *a, **k): return None
        def unsubscribe(self, *a, **k): return None

    MessengerTransport.register(CompleteAdapter)

    with patched_broker():
        messenger = Messenger(SETTINGS, CREDENTIALS, 'chan', transport=CompleteAdapter())

    assert isinstance(messenger.client, CompleteAdapter)


def test_a_transport_cannot_be_shared_between_messengers():
    """m1: single ownership, enforced rather than hoped for.

    Two Messengers over one transport is a data race, not a saving. The second
    ``__init__`` overwrites the first's ``on_message``, so a message delivered on
    the SCHEDULER's topic invokes the AGENT's callback; and either owner's
    ``disconnect()`` closes the connection under the other. The suite itself used
    to hand one FakeTransport to three Messengers as if that were fine.
    """
    fake = FakeTransport()
    with patched_broker():
        Messenger(SETTINGS, CREDENTIALS, 'chan', transport=fake)

        with pytest.raises(ValueError) as excinfo:
            Messenger(SETTINGS, CREDENTIALS, 'other', transport=fake)

    assert 'FakeTransport' in str(excinfo.value)
    assert 'already bound' in str(excinfo.value)


def test_a_failed_construction_does_not_claim_ownership_of_the_transport():
    """m2(a): the mark must not survive a constructor that blew up.

    ``loop_start()`` raising is not hypothetical -- it is the broker-unreachable
    case the ABC's LIFECYCLE point 2 requires to PROPAGATE. When the mark was
    written before ``loop_start``, a retry with the same instance was refused
    with "already bound to a Messenger": a message about a data race, reported
    for a sequence containing no concurrency at all, hiding the real cause.
    """
    fake = FakeTransport(raise_on={'loop_start'})

    with patched_broker():
        with pytest.raises(RuntimeError):
            Messenger(SETTINGS, CREDENTIALS, 'chan', transport=fake)

        # Not owned: nothing completed.
        assert '_orsim_messenger_bound' not in fake.__dict__

        # ...so the retry reports the REAL problem, whatever it is, rather than
        # a spurious ValueError. Here the transport is repaired first.
        fake.raise_on.clear()
        messenger = Messenger(SETTINGS, CREDENTIALS, 'chan', transport=fake)

    assert messenger.client is fake


def test_a_transport_can_be_rebound_after_a_clean_teardown():
    """m2(b): ownership is a LIFETIME, not a permanent brand.

    Tearing a Messenger down and building a new one over the same live transport
    is a legitimate reconnect shape. It was refused, because the mark was never
    cleared -- so the single-ownership guard, whose whole purpose is to stop two
    LIVE Messengers sharing one callback, also blocked a sequence with exactly
    one live Messenger at every instant.
    """
    fake = FakeTransport()
    with patched_broker():
        first = Messenger(SETTINGS, CREDENTIALS, 'chan', transport=fake)
        assert '_orsim_messenger_bound' in fake.__dict__

        first.disconnect()
        assert '_orsim_messenger_bound' not in fake.__dict__

        second = Messenger(SETTINGS, CREDENTIALS, 'chan', transport=fake)

    assert second.client is fake

    # And the guard still bites while the SECOND one is live.
    with patched_broker():
        with pytest.raises(ValueError):
            Messenger(SETTINGS, CREDENTIALS, 'chan', transport=fake)


def test_the_default_path_binds_a_fresh_transport_every_time():
    """The ownership check must never fire on the default (no-transport) path.

    Each ``Messenger(settings, credentials, ...)`` builds its OWN PahoTransport,
    so 1063 agents booting in one worker must not collide. This is the guard on
    the guard.
    """
    with patched_broker():
        first = Messenger(SETTINGS, CREDENTIALS, 'chan')
        second = Messenger(SETTINGS, CREDENTIALS, 'chan')

    assert isinstance(first.client, PahoTransport)
    assert first.client is not second.client


def test_a_setattr_only_transport_silently_drops_every_message():
    """M5: the shape the ABC used to sanction, shown failing.

    The ``on_message`` comment offered "or via ``__setattr__``" as an
    alternative to a write-through property. A transport taking that advice is
    ABC-compliant, instantiable and accepted by ``Messenger`` -- and drops every
    inbound message, because ``MessengerTransport.on_message = None`` is a CLASS
    attribute that shadows any other read path, so the transport's own receive
    loop reads ``None`` and has nothing to call. Nothing raises, anywhere.
    """
    base = _stub_transport(MANDATORY_MEMBERS)

    class SetattrOnlyTransport(base):
        """Forwards the WRITE and provides no read-through property."""

        def __setattr__(self, name, value):
            if name == 'on_message':
                object.__setattr__(self, 'forwarded', value)
            else:
                object.__setattr__(self, name, value)

    transport = SetattrOnlyTransport()
    callback = mock.Mock()

    with patched_broker():
        Messenger(SETTINGS, CREDENTIALS, 'chan', callback, transport=transport)

    # The write was captured...
    assert transport.forwarded is callback
    # ...and every READ still answers None, so its receive loop calls nothing.
    assert transport.on_message is None


def test_a_read_through_property_is_what_makes_the_forwarding_shape_work():
    """The complement: the single supported contract, stated positively.

    Store the callback where you like -- but a read of ``transport.on_message``
    must return it. That is what PahoTransport's property does, and it is the
    only reason its ``__setattr__`` forwarding is safe.
    """
    base = _stub_transport(MANDATORY_MEMBERS)

    class WriteThroughTransport(base):
        @property
        def on_message(self):
            return self.__dict__.get('forwarded')

        @on_message.setter
        def on_message(self, value):
            self.__dict__['forwarded'] = value

    transport = WriteThroughTransport()
    callback = mock.Mock()

    with patched_broker():
        Messenger(SETTINGS, CREDENTIALS, 'chan', callback, transport=transport)

    assert transport.on_message is callback


def test_messenger_never_connects_or_credentials_an_injected_transport():
    """The lifecycle the ABC now states, asserted rather than assumed.

    F1: the round-1 docstrings claimed connect() and username_pw_set() ARE
    called on a transport.  They are not -- by anyone, ever -- which is why a
    transport written to that description would be subscribed, looped and
    published through while still disconnected.  The contract is now "be
    connected by the end of your own __init__", and this is its enforcement.
    """
    # A FRESH transport per Messenger. Sharing one is the hazard
    # test_a_transport_cannot_be_shared_between_messengers pins, and a suite that
    # models it invites the next reader to copy it.
    fakes = [FakeTransport() for _ in range(3)]
    with patched_broker():
        Messenger(SETTINGS, CREDENTIALS, 'chan', transport=fakes[0])
        Messenger(SETTINGS, CREDENTIALS, ['a'], transport=fakes[1])
        Messenger(SETTINGS, CREDENTIALS, transport=fakes[2]).disconnect()

    for fake in fakes:
        assert 'connect' not in fake.call_names
        assert 'username_pw_set' not in fake.call_names


def test_fake_transport_satisfies_the_contract():
    fake = FakeTransport()
    assert isinstance(fake, MessengerTransport)
    assert FakeTransport.__abstractmethods__ == frozenset()


def test_on_message_is_an_attribute_not_a_method():
    """The machine-checkable form of 'assignable callback, not an override point'.

    Two live call sites do ``transport.on_message = cb``.  Declaring on_message
    as an abstractmethod (or an abstractproperty) would force every subclass to
    define it and would break plain assignment on transports that do not care.
    """
    assert MessengerTransport.on_message is None
    assert 'on_message' not in MessengerTransport.__abstractmethods__


# ---------------------------------------------------------------------------
# Injected transport: the whole point of the seam
# ---------------------------------------------------------------------------
# patched_broker() is active on every one of these so that a regression which
# reintroduced a paho/requests call would be caught rather than executed.


def test_injected_transport_is_used_verbatim():
    fake = FakeTransport()
    with patched_broker():
        messenger = Messenger(SETTINGS, CREDENTIALS, transport=fake)
    assert messenger.client is fake


def test_injected_transport_never_touches_rabbitmq():
    """The leak this abstraction exists to make fixable.

    ``register_user`` is an unauthenticated GET plus three authenticated PUTs
    against the RabbitMQ management API, run once per agent construction -- the
    mechanism behind this box's ~699,625 user rows.  Phase 1 does not fix the
    leak, but it does establish that a non-paho transport never inherits it.
    """
    fake = FakeTransport()
    with patched_broker() as broker:
        Messenger(SETTINGS, CREDENTIALS, 'chan', transport=fake)

    broker.paho.Client.assert_not_called()
    broker.requests.get.assert_not_called()
    broker.requests.put.assert_not_called()


def test_messenger_module_no_longer_imports_the_broker_libraries():
    """The imports really moved; they are not merely unused where they sit.

    Without this, ``test_injected_transport_never_touches_rabbitmq`` could pass
    against a Messenger that still held its own ``paho``/``requests`` module
    references, since it only patches the ones in ``paho_transport``.
    """
    import orsim.messenger.messenger as messenger_module

    assert not hasattr(messenger_module, 'paho')
    assert not hasattr(messenger_module, 'requests')


def test_channel_id_none_subscribes_to_nothing_but_still_starts_the_loop():
    fake = FakeTransport()
    with patched_broker():
        Messenger(SETTINGS, CREDENTIALS, transport=fake)

    assert fake.calls == [('loop_start', (), {})]
    assert fake.subscriptions == []


def test_channel_id_str_subscribes_with_explicit_qos_then_starts_the_loop():
    fake = FakeTransport()
    with patched_broker():
        Messenger(SETTINGS, CREDENTIALS, 'chan', transport=fake)

    assert fake.calls == [
        ('subscribe', ('chan',), {'qos': 0}),
        ('loop_start', (), {}),
    ]


def test_channel_id_list_subscribes_with_tuples_and_no_qos_kwarg():
    """The list shape passes a bare list of ``(topic, qos)`` pairs, no ``qos=``.

    Both shapes are handled by paho itself.  Normalising them into one here
    would change what the broker is actually asked for, so the difference is
    pinned rather than tidied.
    """
    fake = FakeTransport()
    with patched_broker():
        Messenger(SETTINGS, CREDENTIALS, ['a', 'b'], transport=fake)

    assert fake.calls == [
        ('subscribe', ([('a', 0), ('b', 0)],), {}),
        ('loop_start', (), {}),
    ]


def test_channel_id_tuple_is_silently_not_subscribed():
    """PINS A QUIRK, deliberately.

    ``Messenger.__init__`` tests ``isinstance(channel_id, str)`` then
    ``isinstance(channel_id, list)``.  A TUPLE of channel names matches neither,
    so it is accepted, stored on ``self.channel_id``, never subscribed -- and,
    because ``disconnect`` keys off ``channel_id is not None``, it still takes
    the loop_stop branch on teardown.  That is today's behaviour.  This test
    exists so that a later "cleanup" which widens the check has to break a test
    on purpose rather than change agent behaviour by accident.
    """
    fake = FakeTransport()
    with patched_broker():
        messenger = Messenger(SETTINGS, CREDENTIALS, ('a', 'b'), transport=fake)

    assert fake.calls == [('loop_start', (), {})]
    assert messenger.channel_id == ('a', 'b')


def test_on_message_is_assigned_before_the_subscribe():
    """Order matters: a callback attached after subscribing can miss deliveries."""
    callback = mock.Mock()
    fake = FakeTransport()
    with patched_broker():
        Messenger(SETTINGS, CREDENTIALS, 'chan', callback, transport=fake)

    assert fake.on_message is callback
    assert fake.call_names == ['on_message', 'subscribe', 'loop_start']


def test_on_message_none_is_never_assigned_at_all():
    """Not "assigned None" -- never written.

    ``if on_message is not None`` guards the assignment, so the transport's own
    default survives.  ``'on_message' not in __dict__`` is what distinguishes
    "untouched" from "explicitly set to None"; the two are indistinguishable by
    reading the value.
    """
    fake = FakeTransport()
    with patched_broker():
        Messenger(SETTINGS, CREDENTIALS, 'chan', None, transport=fake)

    assert fake.on_message is None
    assert 'on_message' not in fake.__dict__
    assert 'on_message' not in fake.call_names


def test_messenger_still_exposes_settings_credentials_and_channel_id():
    fake = FakeTransport()
    with patched_broker():
        messenger = Messenger(SETTINGS, CREDENTIALS, 'chan', transport=fake)

    assert messenger.settings is SETTINGS
    assert messenger.credentials is CREDENTIALS
    assert messenger.channel_id == 'chan'


def test_transport_property_aliases_client():
    fake = FakeTransport()
    with patched_broker():
        messenger = Messenger(SETTINGS, CREDENTIALS, transport=fake)

    assert messenger.transport is messenger.client is fake


def test_deliver_round_trips_through_the_assigned_callback():
    """The fake can drive the inbound direction too, with no broker."""
    received = []
    fake = FakeTransport()
    with patched_broker():
        Messenger(SETTINGS, CREDENTIALS, 'chan',
                  lambda client, userdata, message: received.append(message),
                  transport=fake)

    fake.deliver('chan', {'action': 'step', 'time_step': 7})

    assert len(received) == 1
    assert received[0].topic == 'chan'
    assert json.loads(received[0].payload.decode('utf-8')) == {'action': 'step', 'time_step': 7}


# ---------------------------------------------------------------------------
# disconnect(): the asymmetry and the swallowing
# ---------------------------------------------------------------------------


def test_disconnect_with_a_channel_stops_the_loop_then_disconnects():
    fake = FakeTransport()
    with patched_broker():
        messenger = Messenger(SETTINGS, CREDENTIALS, 'chan', transport=fake)
    fake.calls.clear()

    messenger.disconnect()

    assert fake.calls == [
        ('loop_stop', (), {'force': True}),
        ('disconnect', (), {}),
    ]


def test_disconnect_without_a_channel_skips_loop_stop():
    """PINS AN ASYMMETRY.

    ``loop_stop`` is guarded by ``channel_id is not None`` while ``disconnect``
    is not.  Agents construct their Messenger with channel_id=None, so agent
    teardown reaches ``disconnect`` but never ``loop_stop`` -- the background
    loop that ``__init__`` unconditionally started is left running.  Whether
    that is a defect is a question for a later phase; it is behaviour today.
    """
    fake = FakeTransport()
    with patched_broker():
        messenger = Messenger(SETTINGS, CREDENTIALS, transport=fake)
    fake.calls.clear()

    messenger.disconnect()

    assert fake.calls == [('disconnect', (), {})]


def test_disconnect_swallows_failures_from_both_calls_and_attempts_both():
    """Teardown must not raise, and must not stop at the first failure."""
    fake = FakeTransport(raise_on={'loop_stop', 'disconnect'})
    with patched_broker():
        messenger = Messenger(SETTINGS, CREDENTIALS, 'chan', transport=fake)
    fake.calls.clear()

    messenger.disconnect()  # must not raise

    assert fake.call_names == ['loop_stop', 'disconnect']


# ---------------------------------------------------------------------------
# register_user: pure delegation, no RabbitMQ fallback
# ---------------------------------------------------------------------------


def test_register_user_delegates_to_a_transport_that_offers_one():
    fake = FakeTransport()
    fake.register_user = mock.Mock(name='register_user')

    with patched_broker() as broker:
        messenger = Messenger(SETTINGS, CREDENTIALS, transport=fake)
        messenger.register_user('u', 'p')

    fake.register_user.assert_called_once_with('u', 'p')
    broker.requests.get.assert_not_called()
    broker.requests.put.assert_not_called()


def test_register_user_raises_for_a_transport_that_offers_none():
    """F3: NO HTTP FALLBACK.  This is the inverse of the round-1 assertion.

    Round 1 fell back to the module-level ``register_rabbitmq_user`` here, on a
    back-compat argument that does not survive contact with the code: nothing
    in either repository calls ``Messenger.register_user`` at all -- its one
    caller is ``PahoTransport.__init__``, on its own instance.  What the
    fallback DID do was re-run the RabbitMQ management API for a transport that
    is not RabbitMQ-backed: one GET plus three PUTs, creating exactly the user
    rows (~699,625 on this box) that moving the call into PahoTransport exists
    to stop.  And on a deployment with no ``RABBITMQ_MANAGEMENT_SERVER`` key it
    raised ``KeyError`` -- more confusing than the ``AttributeError`` the
    fallback was added to avoid.

    So: delegate or raise, and raise with the transport's name in the message.
    """
    fake = FakeTransport()
    assert not hasattr(fake, 'register_user')

    with patched_broker(status_code=404) as broker:
        messenger = Messenger(SETTINGS, CREDENTIALS, transport=fake)

        with pytest.raises(AttributeError) as excinfo:
            messenger.register_user('u', 'p')

    assert 'FakeTransport' in str(excinfo.value)

    # The whole point: not one byte went to the RabbitMQ management API.
    broker.requests.get.assert_not_called()
    broker.requests.put.assert_not_called()


def test_register_user_does_not_need_the_rabbitmq_settings_keys():
    """The fallback's other defect: it raised KeyError, not AttributeError.

    A non-RabbitMQ deployment has no reason to carry
    ``RABBITMQ_MANAGEMENT_SERVER``.  Round 1's fallback indexed it directly, so
    the caller got a ``KeyError`` naming a setting they had deliberately not
    configured.  Delegation-only means the settings dict is never touched.
    """
    fake = FakeTransport()
    kafka_only_settings = {'MQTT_BROKER': 'unused.invalid'}

    with patched_broker():
        messenger = Messenger(kafka_only_settings, CREDENTIALS, transport=fake)

        with pytest.raises(AttributeError):
            messenger.register_user('u', 'p')


# ---------------------------------------------------------------------------
# THE BACKWARD-COMPATIBILITY NET
# ---------------------------------------------------------------------------


def test_legacy_four_arg_constructor_builds_a_paho_transport():
    """Existing callers get exactly what they got before -- via the new seam.

    ``Messenger(settings, credentials, channel_id, on_message)`` is the form
    every pre-abstraction caller uses.  It must still construct a paho client,
    set its credentials, register the RabbitMQ user, connect, attach the
    callback, subscribe and start the loop.  The only thing that changed is
    that ``messenger.client`` now holds the ``PahoTransport`` wrapper rather
    than the raw paho object -- and the 17 ``messenger.client.<method>(...)``
    call sites cannot tell the difference, which is why none of them moved.
    """
    callback = mock.Mock()

    with patched_broker(status_code=404) as broker:
        messenger = Messenger(SETTINGS, CREDENTIALS, 'chan', callback)

        assert isinstance(messenger.client, PahoTransport)

        # The broker-side side effects still happened, in full.
        broker.paho.Client.assert_called_once_with(CREDENTIALS['email'], clean_session=True)
        client = broker.paho.Client.return_value
        client.username_pw_set.assert_called_once_with(
            username=CREDENTIALS['email'], password=CREDENTIALS['password'])
        client.connect.assert_called_once_with(SETTINGS['MQTT_BROKER'])
        broker.requests.get.assert_called_once()
        assert broker.requests.put.call_count == 3

        # And the messenger-side wiring still happened, in full.
        assert messenger.client.on_message is callback
        client.subscribe.assert_called_once_with('chan', 0)
        client.loop_start.assert_called_once_with()


def test_legacy_positional_call_still_reaches_the_same_place_as_the_keyword_form():
    """``transport`` was the fifth positional parameter and stays the fifth."""
    fake = FakeTransport()
    with patched_broker():
        messenger = Messenger(SETTINGS, CREDENTIALS, 'chan', None, fake)
    assert messenger.client is fake
