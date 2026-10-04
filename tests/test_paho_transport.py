"""``PahoTransport`` is a TRANSCRIPTION, and these tests hold it to that.

The file it was moved from was not tidy: no request timeouts, no auth on the
users GET, the permissions PUTs outside the try, a typo'd key in the
topic-permissions body, and a side-effect order (register the RabbitMQ user
BEFORE connecting) that is load-bearing for a fresh run's first boot.  Every one
of those looks like something to fix, and fixing any of them in phase 1 would
change observable behaviour for every agent boot.

So these tests pin the original's shape rather than its intent.  The ordering
test in particular is the evidence for "same behaviour, same order": a set of
per-mock assertions can prove that each call happened but says nothing about how
the calls on TWO DIFFERENT objects (the paho client and the requests module)
interleave, and the interleaving is exactly what was at risk in the move.
"""

import json
from unittest import mock

import pytest

from orsim.messenger.paho_transport import PahoTransport, register_rabbitmq_user

from .fakes import CREDENTIALS, SETTINGS, patched_broker


MGMT = SETTINGS['RABBITMQ_MANAGEMENT_SERVER']
AUTH = (SETTINGS['RABBITMQ_ADMIN_USER'], SETTINGS['RABBITMQ_ADMIN_PASSWORD'])


class RegistrationBoom(Exception):
    """A distinctive exception type, so ``raise e`` can be shown to re-raise it."""


# ---------------------------------------------------------------------------
# Construction order -- the single most important assertion in this file
# ---------------------------------------------------------------------------


def test_construction_performs_the_four_side_effects_in_the_original_order():
    """paho.Client -> username_pw_set -> register_user (GET + PUTs) -> connect.

    Attaching both patched modules to one manager mock gives a single ordered
    timeline across objects.  Registering the user before connecting is what
    lets an agent whose RabbitMQ user does not yet exist connect at all; it is
    also why a refused connection still leaves a user row behind, i.e. the
    mechanism of the leak.  Connecting first would change fresh-run boot
    behaviour, so the order is asserted, not assumed.
    """
    manager = mock.Mock()

    with patched_broker(status_code=404, manager=manager):
        PahoTransport(SETTINGS, CREDENTIALS)

    assert [call[0] for call in manager.mock_calls] == [
        'paho.Client',
        'paho.Client().username_pw_set',
        'requests.get',
        'requests.put',
        'requests.put',
        'requests.put',
        'paho.Client().connect',
    ]


def test_construction_uses_the_original_paho_call_shapes():
    """Positional client_id + keyword clean_session; keyword creds; bare host.

    paho's own defaults fill in the rest.  Changing any of these argument forms
    changes what the library receives even though the call "looks the same".
    """
    with patched_broker() as broker:
        PahoTransport(SETTINGS, CREDENTIALS)

    broker.paho.Client.assert_called_once_with(CREDENTIALS['email'], clean_session=True)
    client = broker.paho.Client.return_value
    client.username_pw_set.assert_called_once_with(
        username=CREDENTIALS['email'], password=CREDENTIALS['password'])
    client.connect.assert_called_once_with(SETTINGS['MQTT_BROKER'])


# ---------------------------------------------------------------------------
# register_user branches
# ---------------------------------------------------------------------------


def test_existing_user_skips_creation_but_still_reapplies_both_permissions():
    """PINS A BRANCH THAT IS EASY TO LOSE.

    Only the user-creation PUT is inside the else; the two permissions PUTs sit
    outside it and fire unconditionally.  Wrapping all three in the else -- the
    obvious "tidy" reading -- would stop re-applying permissions for every
    already-registered user, which is a live behaviour change on every rerun.
    """
    with patched_broker(status_code=200) as broker:
        PahoTransport(SETTINGS, CREDENTIALS)

    user = CREDENTIALS['email']
    put_urls = [call.args[0] for call in broker.requests.put.call_args_list]
    assert put_urls == [
        f"{MGMT}/permissions/%2F/{user}",
        f"{MGMT}/topic-permissions/%2F/{user}",
    ]
    assert f"{MGMT}/users/{user}" not in put_urls


def test_absent_user_is_created_with_the_original_body():
    with patched_broker(status_code=404) as broker:
        PahoTransport(SETTINGS, CREDENTIALS)

    user = CREDENTIALS['email']
    users_put = broker.requests.put.call_args_list[0]
    assert users_put.args[0] == f"{MGMT}/users/{user}"
    assert json.loads(users_put.kwargs['data']) == {
        'password': CREDENTIALS['password'],
        'tags': '',
    }
    assert users_put.kwargs['auth'] == AUTH
    assert users_put.kwargs['headers'] == {'content-type': 'application/json'}


def test_users_get_is_unauthenticated_and_untimed():
    """PINS TWO LATENT ODDITIES.

    The GET carries no auth and no timeout, unlike the three PUTs.  Both are
    real findings; both are behaviour changes to fix (an authenticated GET can
    return a different status, and a timeout turns a hang into an exception at
    agent boot).  Recorded here so a later phase changes them knowingly.
    """
    with patched_broker() as broker:
        PahoTransport(SETTINGS, CREDENTIALS)

    get_call = broker.requests.get.call_args
    assert get_call.args == (f"{MGMT}/users/{CREDENTIALS['email']}",)
    assert get_call.kwargs == {}


def test_topic_permissions_body_keeps_its_typo():
    """PINS A TYPO ON PURPOSE.

    The body's first key is ``{username: username}`` -- the VALUE of the
    variable used as the key -- almost certainly meant to be the literal
    ``"username"``, as the permissions PUT immediately above it has.  Asserting
    the typo is how the move is proved verbatim: had it been silently corrected,
    RabbitMQ would receive a different document and this test would fail.
    """
    user = CREDENTIALS['email']
    with patched_broker() as broker:
        PahoTransport(SETTINGS, CREDENTIALS)

    topic_put = broker.requests.put.call_args_list[-1]
    assert topic_put.args[0] == f"{MGMT}/topic-permissions/%2F/{user}"
    assert json.loads(topic_put.kwargs['data']) == {
        user: user,          # <- the typo, verbatim
        'vhost': '/',
        'exchange': '',
        'write': '.*',
        'read': '.*',
    }

    permissions_put = broker.requests.put.call_args_list[-2]
    assert json.loads(permissions_put.kwargs['data']) == {
        'username': user,    # <- the correct form, for contrast
        'vhost': '/',
        'configure': '.*',
        'write': '.*',
        'read': '.*',
    }


def test_a_failed_user_creation_propagates_out_of_construction_unchanged():
    """``raise e`` re-raises the original object; no new try/except was added.

    Exception identity and timing at agent boot must not shift.  A failure here
    has always killed the constructor, and it still does -- one extra traceback
    frame is the only difference.
    """
    with patched_broker(status_code=404) as broker:
        broker.requests.put.side_effect = RegistrationBoom('management API rejected the user')
        with pytest.raises(RegistrationBoom):
            PahoTransport(SETTINGS, CREDENTIALS)

        # It died before connecting -- the order in the ordering test, confirmed
        # from the failure side.
        broker.paho.Client.return_value.connect.assert_not_called()


def test_register_rabbitmq_user_is_callable_without_a_transport():
    """It is a plain function, and callable as one.

    NOT because anything falls back to it: ``Messenger.register_user``
    delegates to the transport or raises, and a fallback would re-run the
    management API for a transport that is not RabbitMQ-backed. It is
    module-level because it is the single implementation behind
    ``PahoTransport.register_user``, and because an operator holding only a
    settings dict -- no transport, no messenger -- can then invoke it.
    """
    with patched_broker() as broker:
        register_rabbitmq_user(SETTINGS, 'someone', 'pw')

    broker.requests.get.assert_called_once_with(f"{MGMT}/users/someone")
    assert broker.requests.put.call_count == 3


# ---------------------------------------------------------------------------
# Forwarding fidelity -- what paho actually receives
# ---------------------------------------------------------------------------


@pytest.fixture
def transport_and_client():
    """A constructed PahoTransport with a FRESH mock client.

    Construction already calls username_pw_set and connect, so a test of those
    two forwards would otherwise be reading __init__'s calls rather than its
    own.  Replacing ``_client`` afterwards gives every forward a clean slate.
    """
    with patched_broker():
        transport = PahoTransport(SETTINGS, CREDENTIALS)
    client = mock.Mock(name='paho_client')
    transport._client = client
    return transport, client


def test_publish_forwards_the_full_paho_signature(transport_and_client):
    """Defaults are made positional -- and they are paho's OWN defaults.

    ``publish('t', 'payload')`` reaches paho as ``('t', 'payload', 0, False)``,
    which is byte-identical to what paho would have filled in itself.  The
    return value is passed back too, so a caller that inspects the
    ``MQTTMessageInfo`` sees no difference (no call site in either repo does).
    """
    transport, client = transport_and_client
    result = transport.publish('t', 'payload')
    client.publish.assert_called_once_with('t', 'payload', 0, False)
    assert result is client.publish.return_value


def test_subscribe_forwards_both_live_shapes_unchanged(transport_and_client):
    transport, client = transport_and_client

    transport.subscribe('t', qos=0)
    transport.subscribe([('a', 0), ('b', 0)])

    assert client.subscribe.call_args_list == [
        mock.call('t', 0),
        mock.call([('a', 0), ('b', 0)], 0),
    ]


def test_the_remaining_forwards_reach_paho_intact(transport_and_client):
    transport, client = transport_and_client

    transport.unsubscribe('t')
    transport.loop_start()
    transport.loop_stop(force=True)
    transport.disconnect()
    transport.connect('host')
    transport.username_pw_set(username='u', password='p')

    client.unsubscribe.assert_called_once_with('t')
    client.loop_start.assert_called_once_with()
    client.loop_stop.assert_called_once_with(True)
    client.disconnect.assert_called_once_with()
    client.connect.assert_called_once_with('host', 1883, 60)
    client.username_pw_set.assert_called_once_with('u', 'p')


def test_on_message_writes_through_to_the_client_and_reads_back(transport_and_client):
    """The wrapper must not swallow the callback.

    ``MessengerTransport`` declares ``on_message = None`` as a class attribute so
    that plain assignment works on any transport -- and that class attribute
    would shadow ``__getattr__``, making reads answer ``None`` forever.  The
    property is what makes the read go through to paho's dispatch loop.
    """
    transport, client = transport_and_client
    callback = mock.Mock()

    transport.on_message = callback

    assert client.on_message is callback
    assert transport.on_message is callback


def test_the_whole_on_prefix_family_reaches_the_client(transport_and_client):
    """on_connect / on_disconnect / on_publish / ... keep working for the package's
    external consumers, who were handed a raw paho client before this change."""
    transport, client = transport_and_client
    callback = mock.Mock()

    transport.on_connect = callback

    assert client.on_connect is callback


def test_undeclared_paho_methods_still_reach_the_client(transport_and_client):
    """``is_connected`` is not one of the nine, and must still work."""
    transport, client = transport_and_client

    result = transport.is_connected()

    client.is_connected.assert_called_once_with()
    assert result is client.is_connected.return_value


def test_getattr_does_not_recurse_on_a_partially_constructed_object():
    """The ``_client`` guard in ``__getattr__``.

    Without it, looking up ``_client`` on an object whose ``__init__`` has not
    run misses normal lookup, enters ``__getattr__``, looks up ``_client``
    again, and recurses until the stack dies -- during construction and in every
    debugger repr.
    """
    bare = PahoTransport.__new__(PahoTransport)
    assert getattr(bare, '_client', None) is None
