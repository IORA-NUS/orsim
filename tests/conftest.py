"""Suite-wide guarantee: these tests cannot touch the network.

The pre-existing ``tests/test_orsim.py`` needed a live RabbitMQ/MQTT broker.
That is why it was never run, and why its collection error (importing
``ORSimAgent`` from ``orsim.core``, where it does not live) sat undetected --
the suite was assumed broken-because-no-broker rather than broken-because-broken.

A transport abstraction whose own tests need a broker would repeat that mistake,
so this fixture makes the requirement structural rather than a convention: any
attempt to open a socket fails the test that made it, with a message pointing at
the fix.  Every broker-touching path in the suite is either driven by
:class:`tests.fakes.FakeTransport` or has ``paho``/``requests`` patched out; this
is what proves that claim instead of asserting it.

Note the guard is installed per-test, not at import time -- ``orsim`` pulls in
paho, requests and celery at collection, and those imports are allowed (they
open nothing).  What is forbidden is a test actually reaching for a connection.

It blocks CONNECTING and NAME RESOLUTION, not socket construction: ``asyncio``
builds a local self-pipe out of a socket pair for its own event loop, and
``asyncio.run`` is how the scheduler wiring test drives a real ``step()``.
Blocking the constructor would break the loop rather than the test's networking,
which is the wrong thing to catch.
"""

import socket

import pytest


class NetworkAccessAttempted(RuntimeError):
    """Raised instead of opening a socket."""


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    def blocked(*args, **kwargs):
        raise NetworkAccessAttempted(
            "This suite must pass with no RabbitMQ, no MQTT broker and no network. "
            "Use tests.fakes.FakeTransport, or patch the broker with "
            "tests.fakes.patched_broker()."
        )

    monkeypatch.setattr(socket, 'create_connection', blocked)
    monkeypatch.setattr(socket, 'getaddrinfo', blocked)
    monkeypatch.setattr(socket.socket, 'connect', blocked)
    monkeypatch.setattr(socket.socket, 'connect_ex', blocked)
