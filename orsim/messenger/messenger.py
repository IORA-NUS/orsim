
import logging

from .transport import MessengerTransport
from .paho_transport import PahoTransport


#: Marker written onto a transport while a Messenger owns it.
#:
#: Its LIFETIME is the point of it: written at the END of a SUCCESSFUL
#: ``Messenger.__init__`` and removed by ``Messenger.disconnect()``. Set any
#: earlier and a construction that fails partway (an unreachable broker out of
#: ``loop_start``) leaves the instance permanently unusable and reports the
#: wrong reason on the retry; never removed and a rebuild-the-Messenger-over-a-
#: live-transport sequence is refused for a data race that is not happening.
#:
#: Read out of the instance ``__dict__`` directly, never with ``getattr``:
#: ``PahoTransport.__getattr__`` forwards unknown names to the wrapped paho
#: client, which would answer with a client attribute (or a Mock) instead of
#: reporting "not bound".
_TRANSPORT_OWNER_FLAG = '_orsim_messenger_bound'


class Messenger:
    """
    Messenger class for managing transport-backed messaging.

    This class handles subscribing to channels and starting the transport's receive
    loop, and allows for custom message handling via a callback. It supports both
    single and multiple channel subscriptions.

    As of the transport abstraction, everything broker-specific -- the paho client,
    the MQTT connect, and the RabbitMQ management-API user registration -- lives in
    :class:`~orsim.messenger.paho_transport.PahoTransport`. ``register_user`` moved
    there and remains here only as a PURE DELEGATION, so a future non-RabbitMQ
    transport never inherits -- and can never be made to perform -- a RabbitMQ
    HTTP call.

    Attributes:
        settings (dict): Configuration settings for the messaging backend.
        credentials (dict): User credentials containing 'email' and 'password'.
        channel_id (str or list, optional): Channel(s) to subscribe to.
        client (MessengerTransport): The transport instance -- a
            :class:`~orsim.messenger.paho_transport.PahoTransport` by default. Kept
            under the name ``client`` because every call site in ORSim and in the
            OpenRide apps reaches through ``messenger.client.publish(...)``.

    Args:
        settings (dict): Configuration settings for the messaging system.
        credentials (dict): User credentials for authentication.
        channel_id (str or list, optional): Channel(s) to subscribe to.
        on_message (callable, optional): Callback function for incoming messages.
        transport (MessengerTransport, optional): Transport to use. Previously
            inert, this parameter now has meaning: supply an instance satisfying
            the :class:`~orsim.messenger.transport.MessengerTransport` contract
            and it is used as-is -- ALREADY CONNECTED, since this constructor
            neither connects it nor gives it credentials (see that class's
            LIFECYCLE section). ``None`` (the default) builds a
            ``PahoTransport``, which is exactly the pre-existing behaviour.
            It must be a real ``MessengerTransport`` SUBCLASS (``TypeError``
            otherwise) and must not already belong to another Messenger
            (``ValueError`` otherwise) -- see :meth:`__init__`.

    Methods:
        disconnect():
            Disconnects the transport and stops its loop.

        register_user(username, password):
            Pure delegation to the transport; raises AttributeError if the
            transport has no user registration of its own.
    """

    def __init__(self, settings, credentials, channel_id=None, on_message=None, transport=None):
        """
        Initializes the Messenger instance with the provided settings, credentials, and optional parameters.

        Args:
            settings (dict): Configuration settings, including MQTT broker information.
            credentials (dict): User credentials containing 'email' and 'password'.
            channel_id (str or list, optional): Channel(s) to subscribe to. Can be a single channel (str) or multiple channels (list of str).
            on_message (callable, optional): Callback function to handle incoming messages.
            transport (MessengerTransport, optional): Transport instance, expected
                to be connected already -- this method calls neither ``connect()``
                nor ``username_pw_set()`` on it. If None, a default
                :class:`PahoTransport` is constructed -- which registers the
                RabbitMQ user and connects inside its own ``__init__``, exactly as
                this method used to do inline.

        Notes:
            - Subscribes to the specified channel(s) and starts the transport loop.
            - ``loop_start()`` is UNCONDITIONAL: agents construct with
              ``channel_id=None`` and still expect a running receive loop, since they
              subscribe later in ``start_listening``.
            - The ``isinstance`` checks are exactly ``str`` then ``list``; any other
              channel_id type subscribes to nothing. That is existing behaviour and is
              deliberately not generalised.
            - Designed for inter-agent communication using RabbitMQ PubSub queues.

        Raises:
            TypeError: If ``transport`` is not a
                :class:`~orsim.messenger.transport.MessengerTransport` subclass,
                OR is one only by ``ABCMeta.register()`` and does not actually
                provide all six mandatory methods. This is the ONLY place the
                ABC is enforced -- declaring it does nothing on its own, and a
                duck-typed object missing ``publish`` would otherwise construct
                fine and hang the run at the first message. The second half
                matters because a VIRTUAL subclass gets no
                ``__abstractmethods__`` check from Python at all.
            ValueError: If ``transport`` is already bound to a LIVE Messenger.
                A transport carries exactly one ``on_message`` callback and one
                connection lifetime; the second Messenger would silently clobber
                the first's callback, and either one's ``disconnect()`` would
                tear the shared connection down under the other. The mark is
                claimed only once this constructor has fully succeeded, and is
                released by :meth:`disconnect`, so neither a failed construction
                nor a clean teardown leaves a transport unusable.
        """

        self.settings = settings
        self.credentials = credentials
        self.channel_id = channel_id

        if transport is None:
            transport = PahoTransport(settings, credentials)

        # Enforce the ABC HERE, at the one place a transport is consumed.
        # Declaring MessengerTransport is not enforcement: without this check a
        # duck-typed object missing publish() constructs fine, every agent boots,
        # the run starts, and the first publish raises AttributeError inside the
        # receive callback -- so the agent never replies and the step barrier
        # hangs with no traceback at the point of the mistake. disconnect()'s
        # except-blocks used to absorb the evidence too. Same guard shape as
        # ORSimApp.__init__ applies to its `messenger` argument.
        if not isinstance(transport, MessengerTransport):
            raise TypeError(
                "transport must be an instance of "
                "orsim.messenger.transport.MessengerTransport; got "
                f"{type(transport).__name__}. Subclass MessengerTransport so the "
                "six mandatory methods (disconnect, loop_start, loop_stop, "
                "publish, subscribe, unsubscribe) are checked at instantiation "
                "rather than at the first message."
            )

        # isinstance() is necessary but NOT sufficient: ABCMeta.register(cls)
        # makes an unrelated class a VIRTUAL subclass, and virtual subclasses
        # get no __abstractmethods__ check at all. A phase-2 adapter that wraps
        # an existing client class (so it cannot inherit) and calls
        # MessengerTransport.register(...) while forgetting publish() would pass
        # the check above and fail exactly the way the check exists to prevent:
        # AttributeError on the first agent reply, raised inside a receive
        # callback where OpenRideScheduler.on_receive_message's
        # `except Exception: logging.warning; return` swallows it. Six getattrs
        # per Messenger closes that hole; the names come from the ABC itself, so
        # this cannot drift out of step with it.
        missing = sorted(
            name for name in MessengerTransport.__abstractmethods__
            if not callable(getattr(transport, name, None))
        )
        if missing:
            raise TypeError(
                f"{type(transport).__name__} is a MessengerTransport but does "
                f"not provide callable {', '.join(missing)}. A virtual subclass "
                "registered with MessengerTransport.register() is not checked "
                "for the mandatory methods, so they are checked here instead; "
                "subclass MessengerTransport directly to get the check at "
                "class-definition time."
            )

        # A transport belongs to exactly ONE Messenger. Sharing one silently
        # clobbers the first Messenger's on_message with the second's -- so a
        # message delivered on the scheduler's topic invokes the agent's callback
        # -- and either party's disconnect() tears down the connection under the
        # other.
        #
        # The CHECK is here, before anything is mutated; the MARK is written at
        # the very END of __init__ (see below). Marking here instead would leave
        # the flag set on a transport whose loop_start() raised -- the broker-
        # unreachable case LIFECYCLE point 2 requires to propagate -- so a retry
        # with the same instance would be refused with "already bound to a
        # Messenger", a message that names the wrong problem entirely.
        self._raise_if_transport_already_bound(transport)

        self.client = transport

        if on_message is not None:
            self.client.on_message = on_message


        # RabbitMQ PubSub queue is used for processing requests in sequence
        # This is a deliberate design choice to enable:
        #   - Inter-Agent communication as core part of system design

        # if channel_id is not None:
        if isinstance(channel_id, str):
            # self.client.loop_start()
            self.client.subscribe(channel_id, qos=0)
            logging.debug(f"Channel: {channel_id}")
        elif isinstance(channel_id, list):
            # self.client.loop_start()
            self.client.subscribe([(cid, 0) for cid in channel_id])
            logging.debug(f"Channel: {channel_id}")

        self.client.loop_start()

        # Ownership is claimed only now that construction has fully succeeded.
        # Everything above can raise -- loop_start() on an unreachable broker is
        # the documented case -- and an aborted construction owns nothing.
        self._mark_transport_bound(transport)


    @staticmethod
    def _raise_if_transport_already_bound(transport):
        """Refuse a transport another Messenger still owns.

        Read out of the instance ``__dict__`` directly, never with ``getattr``:
        ``PahoTransport.__getattr__`` forwards unknown names to the wrapped paho
        client, which would answer with a client attribute (or, under a Mock,
        with a Mock) instead of reporting "not bound".
        """
        if _TRANSPORT_OWNER_FLAG in getattr(transport, '__dict__', {}):
            raise ValueError(
                f"{type(transport).__name__} instance is already bound to a "
                "live Messenger. A transport carries exactly one on_message "
                "callback and one connection lifetime, so it cannot be shared; "
                "build one transport per Messenger. (The mark is cleared by "
                "Messenger.disconnect(), so rebinding after a clean teardown is "
                "allowed.)"
            )

    @staticmethod
    def _mark_transport_bound(transport):
        try:
            object.__setattr__(transport, _TRANSPORT_OWNER_FLAG, True)
        except (AttributeError, TypeError):
            # A transport that forbids attribute assignment (``__slots__``)
            # cannot be marked; single ownership then rests on the contract
            # alone, which is no worse than before this check existed.
            pass

    @staticmethod
    def _clear_transport_bound(transport):
        """Release ownership so the instance can be handed to a new Messenger.

        Silent when the transport was never marked (``__slots__``) or has
        already been released -- releasing twice is not an error, and
        ``disconnect()`` is documented as never raising for that class of
        problem.
        """
        getattr(transport, '__dict__', {}).pop(_TRANSPORT_OWNER_FLAG, None)


    def disconnect(self):
        """
        Disconnects the client from the messaging service.

        Stops the client's event loop if a channel is subscribed, and then disconnects the client.
        Exceptions raised BY those two calls are logged and swallowed.

        Note the asymmetry that transport implementors have to know about:
        ``loop_stop`` is guarded by ``channel_id is not None`` while
        ``disconnect`` is not, and agents build their Messenger with
        ``channel_id=None``. So for every agent in every run this method calls
        ``disconnect()`` and NOTHING else -- which is why
        :meth:`~orsim.messenger.transport.MessengerTransport.disconnect` is
        specified to stop the receive loop as well -- and to FLUSH pending
        publishes, because this method logs-and-swallows anything the transport
        raises, so a teardown-time send failure that only raises is invisible.

        On completion the transport's single-ownership mark is released, so the
        same instance may be handed to a new Messenger.

        Raises:
            AttributeError: If the transport does not provide ``loop_stop`` or
                ``disconnect`` at all. The lookups are deliberately outside the
                ``try`` so a broken transport is not tidied away as a successful
                teardown; failures raised by the calls themselves are still
                logged and swallowed as before.
        """

        # try:
        #     self.client.unsubscribe(self.channel_id)
        # except Exception as e:
        #     logging.exception(str(e))

        # The method is RESOLVED outside the try on purpose. A missing method is
        # a broken transport and must surface as AttributeError; an error raised
        # BY the call is a broker-teardown failure and is swallowed exactly as
        # before, so exception identity and timing on the paho path are
        # unchanged. Before this split, `except Exception` absorbed both and a
        # transport with no loop_stop/disconnect tore down "successfully".
        if self.channel_id is not None:
            loop_stop = self.client.loop_stop
            try:
                loop_stop(force=True)
            except Exception as e:
                logging.exception(str(e))

        disconnect = self.client.disconnect
        try:
            disconnect()
        except Exception as e:
            logging.exception(str(e))

        # Teardown has been attempted on both calls, so this Messenger no longer
        # owns the transport: release the single-ownership mark. Without this,
        # the flag set in __init__ was never cleared and a legitimate rebuild --
        # tear the Messenger down, build a new one over the same live transport
        # -- was refused with "already bound", i.e. the guard against a data
        # race fired on a sequence with no concurrency in it at all.
        #
        # Deliberately AFTER both calls and NOT in a finally: if a lookup above
        # raised AttributeError (a transport missing loop_stop/disconnect, which
        # the constructor guard now makes unreachable), the transport is broken
        # rather than torn down and stays bound.
        self._clear_transport_bound(self.client)




    @property
    def transport(self):
        """Read-only alias for :attr:`client`.

        ``client`` is the contract -- 17 call sites reach through it -- so this is a
        convenience for new code that wants the clearer name. It is a property, not
        an instance attribute, so ``Messenger.__dict__`` shape is unchanged.
        """
        return self.client


    # @classmethod
    def register_user(self, username, password):
        """
        Registers a new user with the transport's own user-management facility.

        The implementation moved to
        :class:`~orsim.messenger.paho_transport.PahoTransport`, where a
        RabbitMQ-specific management-API call belongs. This method remains as
        published API (``Messenger`` has been on PyPI since 0.1.2) but is now a
        PURE DELEGATION: it forwards to ``self.client.register_user`` and does
        nothing else.

        There is deliberately NO HTTP fallback. A fallback would re-run the
        RabbitMQ management-API call for a transport that is not RabbitMQ-backed
        -- creating exactly the user rows this abstraction exists to stop, and
        raising ``KeyError`` on ``settings['RABBITMQ_MANAGEMENT_SERVER']``
        whenever a non-RabbitMQ deployment has (correctly) not configured one.
        The default four-argument path is unaffected: ``PahoTransport`` provides
        ``register_user``, so it behaves byte-identically to before.

        Args:
            username (str): The username to register.
            password (str): The password for the new user.

        Raises:
            AttributeError: If the transport in use provides no
                ``register_user``. The message names the transport class, so the
                caller can see which one declined.
            Exception: Whatever the transport raises -- for ``PahoTransport``,
                an error during user creation or permission assignment.

        Note:
            For the default ``PahoTransport`` this requires RabbitMQ management
            API credentials and server URL in ``self.settings``.
        """

        delegate = getattr(self.client, 'register_user', None)
        if not callable(delegate):
            raise AttributeError(
                f"{type(self.client).__name__} provides no register_user(); "
                "Messenger.register_user delegates to the transport and has no "
                "RabbitMQ fallback."
            )
        return delegate(username, password)
