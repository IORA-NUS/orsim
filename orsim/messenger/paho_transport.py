
"""The RabbitMQ/MQTT transport -- today's behaviour, moved, not changed.

WHY THIS MODULE EXISTS
----------------------
Everything broker-specific that used to live inside ``Messenger`` lives here
instead: the ``paho.mqtt.client`` import, the ``paho.Client(...)``
construction, the credential set, the RabbitMQ **management-API user
registration**, and the connect.  ``Messenger`` keeps only the parts that are
genuinely transport-neutral (channel subscription, the loop start, the
disconnect asymmetry), so a future Kafka transport can satisfy the same
nine-member contract without inheriting a RabbitMQ HTTP call it has no use for.

``register_user`` in particular had no business in a class documented as
"MQTT and RabbitMQ-based messaging": it is an unauthenticated GET plus three
authenticated PUTs against the RabbitMQ management port, performed once per
agent construction.  That is also why this box currently holds ~699,625
RabbitMQ users / 3.92 GB -- every agent boot in every run has been creating a
user row.  **Phase 1 does not fix that leak.**  Fixing it changes when (or
whether) a user row appears, which is externally observable behaviour; it
belongs in a later phase with its own evidence.  The move below is byte-for-byte.

WHAT MUST NOT BE "TIDIED" HERE
------------------------------
This file is a transcription.  Four properties of the original are
load-bearing and are preserved deliberately, each of which looks like a defect.
(Call sites are cited below as ``<file>::<function>``, never by line number:
line numbers drift the next time anyone edits a docstring in the cited file,
and a citation that lands in the wrong place makes the evidence read as
fabricated.)

1. **Side-effect ORDER.**  ``username_pw_set`` -> ``register_user`` -> ``connect``.
   Registering the user before connecting is what lets an agent whose RabbitMQ
   user does not yet exist connect at all on a fresh run.  It is also why a
   broker that *refuses* the connection still leaves the user row behind --
   the mechanism of the leak above.  Connecting first would change boot
   behaviour for every fresh run; deferring ``register_user`` to a lazy call
   would move when the row appears.  Neither is acceptable in a phase whose
   acceptance criterion is that nothing is observable.

2. **No new ``try``/``except`` anywhere.**  Exception identity and timing must
   not shift.  ``register_rabbitmq_user``'s first ``requests.get`` is
   unguarded, so a down management API raises ``requests.exceptions.
   ConnectionError`` out of construction today; ``connect`` raises
   ``ConnectionRefusedError`` / ``socket.gaierror``.  After the move these
   propagate out of ``PahoTransport.__init__`` called from
   ``Messenger.__init__`` -- same type, same point in the sequence, one extra
   traceback frame.  Wrapping any of it would silently change failure
   behaviour for every agent boot.  The ``raise e`` after a failed users-PUT
   survives verbatim.

3. **The latent oddities in ``register_rabbitmq_user``.**  No request timeouts;
   no auth on the GET; the two permissions PUTs outside any ``try``; and the
   topic-permissions body whose first key is ``{username: username}`` -- the
   *value of the variable* used as the key, almost certainly a typo for the
   literal ``"username"``.  All four are reproduced exactly.  Each is a real
   finding, and each is a behaviour change to fix.

4. **The paho call shapes.**  Positional ``client_id`` with keyword
   ``clean_session``; keyword ``username``/``password``; single positional host
   to ``connect``.  The eight method forwards below are one-line delegations
   that keep the caller's names and parameter order, so what paho receives is
   unchanged.  Note ``connect`` and ``username_pw_set`` are OPTIONAL on the
   interface (nothing in the framework calls them on a transport) but are
   implemented here anyway, because an external caller reaching through
   ``messenger.client`` expects the full paho surface.

BACK-COMPAT NOTE (accepted residual risk)
-----------------------------------------
``messenger.client`` stops being a ``paho.mqtt.client.Client`` instance.
``isinstance(messenger.client, paho.Client)`` becomes ``False`` and
``type(...).__name__`` changes in logs.  Both repositories were grepped: zero
``isinstance`` checks and zero uses outside the nine members (the app tests
replace the whole messenger with a ``Mock``).  For unseen consumers of the
published PyPI package, :meth:`PahoTransport.__getattr__` delegates any other
attribute READ to the wrapped client, and :meth:`__setattr__` routes every
``on_*`` assignment through to it, so ``on_connect`` / ``on_disconnect`` /
``on_publish`` / ``on_subscribe`` / ``on_log`` keep working.  A write of an
unknown NON-``on_`` name (e.g. ``client.max_inflight_messages = 5``) would land
on the wrapper and silently no-op; there are no such writes in either repo.
"""

import json
import logging

import paho.mqtt.client as paho
import requests

from .transport import MessengerTransport


def register_rabbitmq_user(settings, username, password):
    """Create the RabbitMQ user (if absent) and (re)apply its permissions.

    Moved VERBATIM from ``Messenger.register_user``; the only edit is
    ``self.settings`` -> ``settings``, which makes it callable without a
    transport instance.  It is module-level so that the RabbitMQ knowledge has
    exactly ONE home and an operator holding only ``settings`` can still invoke
    it directly.

    ``Messenger.register_user`` deliberately does NOT fall back to this
    function.  Doing so would re-run the management API for a transport that is
    not RabbitMQ-backed -- creating the very user rows the abstraction exists to
    stop -- so that method delegates to the transport or raises.  Only
    :meth:`PahoTransport.register_user` calls this.

    See this module's docstring, point 3, for the oddities preserved here on
    purpose.  Do not add timeouts, do not add auth to the GET, do not wrap the
    permissions PUTs, and do not "fix" the ``{username: username}`` key.

    Args:
        settings (dict): must carry ``RABBITMQ_MANAGEMENT_SERVER``,
            ``RABBITMQ_ADMIN_USER`` and ``RABBITMQ_ADMIN_PASSWORD``.
        username (str): the user to register (an agent's e-mail-shaped id).
        password (str): that user's password.

    Raises:
        Exception: re-raised unchanged if the user-creation PUT fails.
        requests.exceptions.*: propagated from any of the four calls; the GET
            in particular is unguarded by design.
    """

    response = requests.get(f"{settings['RABBITMQ_MANAGEMENT_SERVER']}/users/{username}")
    if (response.status_code >= 200) and (response.status_code <= 299):
        logging.warning('User is already registered')
    else:
        try:
            response = requests.put(f"{settings['RABBITMQ_MANAGEMENT_SERVER']}/users/{username}",
                                    data=json.dumps({'password': password, 'tags': ''}),
                                    headers={"content-type": "application/json"},
                                    auth=(settings['RABBITMQ_ADMIN_USER'], settings['RABBITMQ_ADMIN_PASSWORD'])
                                )
        except Exception as e:
            logging.exception(str(e))
            raise e

    # reset the user and set appropriate permissions as needed
    quoted_slash = '%2F'
    response = requests.put(f"{settings['RABBITMQ_MANAGEMENT_SERVER']}/permissions/{quoted_slash}/{username}",
                    data=json.dumps({"username":username, "vhost":"/", "configure":".*", "write":".*", "read":".*"}),
                    headers={"content-type": "application/json"},
                    auth=(settings['RABBITMQ_ADMIN_USER'], settings['RABBITMQ_ADMIN_PASSWORD'])
                )

    response = requests.put(f"{settings['RABBITMQ_MANAGEMENT_SERVER']}/topic-permissions/{quoted_slash}/{username}",
                    data=json.dumps({username: username, "vhost": "/", "exchange": "", "write": ".*", "read": ".*"}),
                    headers={"content-type": "application/json"},
                    auth=(settings['RABBITMQ_ADMIN_USER'], settings['RABBITMQ_ADMIN_PASSWORD'])
                )


class PahoTransport(MessengerTransport):
    """MQTT-over-RabbitMQ transport wrapping a ``paho.mqtt.client.Client``.

    This is the default path: ``Messenger(settings, credentials, ...)`` with no
    ``transport=`` builds one of these, and ``messenger.client`` then holds it.
    Constructing it performs the same four side effects, in the same order,
    that ``Messenger.__init__`` used to perform inline.

    Args:
        settings (dict): needs ``MQTT_BROKER`` plus the three
            ``RABBITMQ_*`` keys used by :func:`register_rabbitmq_user`.
        credentials (dict): ``{'email': ..., 'password': ...}``.  The e-mail is
            both the MQTT client id and the RabbitMQ username -- that identity
            is why the user table grows one row per agent.
    """

    def __init__(self, settings, credentials):
        # NOTE: this reproduces the pre-phase-1 Messenger.__init__ lines 64-69
        # exactly, in order, with no error handling added.  See the module
        # docstring, points 1 and 2, before changing anything in this method.
        self._settings = settings
        self._credentials = credentials

        self._client = paho.Client(credentials['email'], clean_session=True)
        self._client.username_pw_set(username=credentials['email'], password=credentials['password'])
        self.register_user(credentials['email'], credentials['password'])

        self._client.connect(settings['MQTT_BROKER'])

    # ------------------------------------------------------------------
    # RabbitMQ-specific (NOT part of the nine-member contract)
    # ------------------------------------------------------------------

    def register_user(self, username, password):
        """Register ``username`` with the RabbitMQ management API.

        Thin instance-bound wrapper over :func:`register_rabbitmq_user`, kept as
        a method so ``Messenger.register_user`` -- which is pure delegation to
        ``self.client.register_user(...)`` -- keeps working unchanged on the
        default path.  Deliberately NOT on
        :class:`~orsim.messenger.transport.MessengerTransport`: a Kafka
        transport must never inherit it, and because it is absent from the
        interface, ``Messenger.register_user`` raises ``AttributeError`` on such
        a transport rather than quietly registering a RabbitMQ user.
        """
        register_rabbitmq_user(self._settings, username, password)

    # ------------------------------------------------------------------
    # The nine-member contract: explicit one-line forwards.
    #
    # Each keeps the caller's parameter names and order and also accepts
    # **kwargs, so an exotic external caller passing paho extras
    # (properties=, keepalive=, options=) still reaches the real client.
    # ------------------------------------------------------------------

    def username_pw_set(self, username, password=None, **kwargs):
        """Forward to paho; called in keyword form at the one live call site."""
        return self._client.username_pw_set(username, password, **kwargs)

    def connect(self, host, port=1883, keepalive=60, **kwargs):
        """Forward to paho.  Failure propagates -- see module docstring point 2."""
        return self._client.connect(host, port, keepalive, **kwargs)

    def disconnect(self, **kwargs):
        """Forward to paho.  Return value is ignored by ``Messenger``.

        Satisfies the ABC's DURABILITY rule (flush pending publishes before
        returning) BY ACCIDENT, and it is worth saying so once: paho puts
        SUBSCRIBE, PUBLISH and DISCONNECT on one ordered TCP connection, so a
        QoS-0 packet is already ahead of the DISCONNECT and there is no
        separate producer to purge.  A transport with an independent send path
        has to do this deliberately -- see
        :meth:`~orsim.messenger.transport.MessengerTransport.disconnect`.
        """
        return self._client.disconnect(**kwargs)

    def loop_start(self, **kwargs):
        """Forward to paho.  Called unconditionally by ``Messenger.__init__``.

        This is also the reason 20-40 agents can share one eventlet hub in a
        celery worker: paho's network thread does patched ``select``/``socket``
        I/O, so it yields cooperatively.  The property is paho's, not this
        wrapper's -- see the ABC's CONCURRENCY CONTRACT point 3 before
        replacing it with a loop that blocks in a C extension.
        """
        return self._client.loop_start(**kwargs)

    def loop_stop(self, force=False, **kwargs):
        """Forward to paho.

        ``Messenger.disconnect`` calls this as ``loop_stop(force=True)`` and
        ONLY when it holds a ``channel_id`` -- agents pass ``channel_id=None``,
        so agent teardown never reaches here.  ``force`` is retained because
        the live call passes it; paho 2.x drops the parameter.
        """
        return self._client.loop_stop(force, **kwargs)

    def publish(self, topic, payload=None, qos=0, retain=False, **kwargs):
        """Forward to paho.  Returns paho's ``MQTTMessageInfo``.

        No call site in either repo consumes the return value, so its fidelity
        is not load-bearing -- it is returned anyway so that behaviour is
        indistinguishable from calling the client directly.
        """
        return self._client.publish(topic, payload, qos, retain, **kwargs)

    def subscribe(self, topic, qos=0, **kwargs):
        """Forward to paho, passing ``topic`` through UNCHANGED.

        Both live shapes -- a topic string with ``qos=0``, and a bare list of
        ``(topic, qos)`` tuples with no ``qos`` -- are handled by paho itself.
        Normalising them here would change what the broker is asked for.

        MQTT accepts ORSim's ``'/'``-separated topic names verbatim, which is
        why this forward can be a pass-through and why the TOPIC CONTRACT on
        the ABC reads as a Kafka problem rather than a general one.  It also
        means ``message.topic`` round-trips byte-identically for free here.
        """
        return self._client.subscribe(topic, qos, **kwargs)

    def unsubscribe(self, topic, **kwargs):
        """Forward to paho."""
        return self._client.unsubscribe(topic, **kwargs)

    # ------------------------------------------------------------------
    # Attribute plumbing: what makes the wrapper indistinguishable from the
    # paho client for the callback family and for unseen external readers.
    # ------------------------------------------------------------------

    @property
    def on_message(self):
        """Read/write the paho client's message callback.

        A property is REQUIRED on the read side, and the ABC now says so too
        (see ``MessengerTransport.on_message``): ``MessengerTransport``
        declares ``on_message = None`` as a class attribute (so that plain
        assignment works on any transport), and that class attribute would
        shadow :meth:`__getattr__`, making ``transport.on_message`` answer
        ``None`` no matter what had been assigned.  A transport that forwards
        only the WRITE -- through ``__setattr__``, as this class also does --
        is instantiable, ABC-compliant and accepted by ``Messenger``, yet drops
        every inbound message, because its own receive loop reads ``None``.
        Reading the callback back is also something external code plausibly
        does, so it reads THROUGH.

        Writes normally arrive via :meth:`__setattr__`, which routes the whole
        ``on_*`` family to the client; this setter is the same destination by a
        different road, so either path leaves the client holding ``value``.
        """
        return self._client.on_message

    @on_message.setter
    def on_message(self, value):
        self._client.on_message = value

    def __setattr__(self, name, value):
        """Route ``on_*`` assignments through to the wrapped paho client.

        ``self.messenger.client.on_message = cb``
        (``orsim_agent.py::ORSimAgent.start_listening``) and
        ``self.client.on_message = on_message``
        (``messenger.py::Messenger.__init__``) must reach paho's dispatch loop,
        not stop at the wrapper.  Handling the whole
        ``on_*`` prefix covers ``on_connect`` / ``on_disconnect`` /
        ``on_publish`` / ``on_subscribe`` / ``on_unsubscribe`` / ``on_log`` for
        free.

        The ``'_client' in self.__dict__`` guard is not optional: without it,
        ``__init__``'s own assignments run before the client exists and the
        lookup of ``self._client`` re-enters here -- infinite recursion on a
        partially constructed object.  (Underscore names short-circuit first,
        so the guard is belt-and-braces for that path and load-bearing for any
        ``on_*`` write attempted before construction finishes.)
        """
        if name.startswith('_'):
            object.__setattr__(self, name, value)
        elif name.startswith('on_') and '_client' in self.__dict__:
            setattr(self._client, name, value)
        else:
            object.__setattr__(self, name, value)

    def __getattr__(self, name):
        """Delegate any attribute this wrapper does not define to paho.

        Miss-only -- Python calls it solely when normal lookup fails -- so it
        costs nothing on the nine forwards.  It exists for external consumers
        of the published package that may touch members the greps could not
        see (``is_connected()``, ``will_set(...)``, ``_client_id``, ...).

        The ``_client`` guard prevents unbounded recursion if this is reached
        on a partially constructed object (``self._client`` missing -> lookup
        fails -> ``__getattr__('_client')`` -> ...).
        """
        if name == '_client':
            raise AttributeError(name)
        return getattr(self._client, name)
