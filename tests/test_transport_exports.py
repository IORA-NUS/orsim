"""Public surface and version guards.

``orsim`` is published on PyPI and consumed by an ecosystem outside this repo,
so the names it exports are part of the contract, not an implementation detail.
And phase 1 is explicitly a no-release change: the version guard is here so that
an accidental ``bumpversion`` (which rewrites ``orsim/__init__.py`` AND commits
AND tags, per setup.cfg) fails a test instead of quietly shipping.
"""

import orsim


def test_top_level_package_exports_the_transport_names():
    from orsim import Messenger, MessengerTransport, PahoTransport

    assert Messenger is orsim.Messenger
    assert issubclass(PahoTransport, MessengerTransport)


def test_messenger_subpackage_exports_the_transport_names():
    from orsim.messenger import Messenger, MessengerTransport, PahoTransport

    assert Messenger is orsim.Messenger
    assert issubclass(PahoTransport, MessengerTransport)


def test_register_rabbitmq_user_is_importable_from_the_paho_module():
    """The RabbitMQ knowledge moved, and is still reachable under its own name.

    NOT a fallback target: ``Messenger.register_user`` delegates to the
    transport or raises, deliberately, and re-adding a fallback here would
    re-create the RabbitMQ user rows this abstraction exists to stop (see
    ``test_messenger_transport.py::test_register_user_raises_for_a_transport_that_offers_none``).
    It is module-level because it is the single implementation behind
    ``PahoTransport.register_user`` and an operator holding only a settings
    dict can call it directly.
    """
    from orsim.messenger.paho_transport import register_rabbitmq_user

    assert callable(register_rabbitmq_user)


def test_version_was_not_bumped():
    """Phase 1 does not release. The tag and the release are human-gated."""
    assert orsim.__version__ == '1.3.0'
