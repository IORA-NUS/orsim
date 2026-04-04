from typing import Any, Dict
from orsim.messenger import Messenger
import logging

from abc import ABC, abstractmethod
from typing import Optional, List
from cerberus import Validator

class HandlerValidationException(Exception):
    pass


class ORSimApp(ABC):
    """
    Robust base class for all app modules. Designed for library use.
    Subclasses must implement create_user and create_manager.
    """
    run_id: str
    sim_clock: str
    credentials: Dict[str, Any]
    messenger: Optional[Messenger]
    behavior: Dict[str, Any]
    user: Any
    manager: Any
    topic_params: dict
    message_queue: List[Any]
    exited_market: bool
    latest_sim_clock: Optional[str]
    latest_loc: Optional[Any]


    def __init__(self, run_id: str, sim_clock: str, behavior: Dict[str, Any], messenger: Optional[Messenger]=None, **kwargs):
        """
        Initialize the base app.
        Args:
            run_id: Unique run identifier
            sim_clock: Simulation clock time
            behavior: Behavior configuration dict
            messenger: Messaging interface (optional)
            kwargs: Additional fields for subclass customization
        """
        self.run_id = run_id
        self.sim_clock = sim_clock
        self.behavior = behavior
        self.credentials = self.extract_credentials(behavior)
        if messenger is not None:
            if Messenger is None or not isinstance(messenger, Messenger):
                raise TypeError("messenger must be an instance of orsim.messenger.Messenger")
        self.messenger = messenger

        for k, v in kwargs.items():
            setattr(self, k, v)

        self.user = self._create_user()
        self.manager = self._create_manager()

        self.topic_params = {}
        if self.manager and hasattr(self.manager, "get_id"):
            try:
                self.topic_params = {
                    f"{self.run_id}/{self.manager.get_id()}": self.handle_app_topic_messages
                }
            except Exception as e:
                logging.warning(f"Failed to set topic_params: {str(e)}")

        self.message_queue: List[Any] = []
        self.exited_market: bool = False
        self.latest_sim_clock: Optional[str] = None
        self.latest_loc: Optional[Any] = None

        if not hasattr(self, 'managed_statemachine'):
            raise NotImplementedError("Subclasses must define managed_statemachine")
        if not hasattr(self, 'interaction_ground_truth_list'):
            raise NotImplementedError("Subclasses must define interaction_ground_truth_list")

        self.validate_message_handlers()
        self.validate_behavior()

    def extract_credentials(self, behavior: Dict[str, Any]) -> Dict[str, Any]:
        """Extract credentials from the behavior dict."""
        creds = {}
        for key in ['email', 'password']:
            if key in behavior:
                creds[key] = behavior[key]
        return creds


    def validate_behavior(self, schema=None):
        """
        Validate self.behavior against the provided schema (defaults to self.behavior_schema).
        Uses allow_unknown=True so extra fields are permitted (for subclass/runtime extension).
        Raises ValueError if validation fails for required/typed fields in the schema.
        Subclasses can call this with an expanded schema.
        """
        if schema is None:
            schema = self.runtime_behavior_schema
        validator = Validator(schema, allow_unknown=True)
        if not validator.validate(self.behavior):
            raise ValueError(f"Invalid agent behavior: {validator.errors}")

    @property
    @abstractmethod
    def runtime_behavior_schema(self) -> Dict[str, Any]:
        """Subclasses must provide the runtime behavior schema for validation."""
        pass

    def validate_message_handlers(self):
        """
        Validates that all required @message_handler methods for this app's statemachine are implemented.
        Raises HandlerValidationException if any handler is missing.
        More robust: handles missing/empty ground truth, missing attributes, and type errors.
        """
        if not getattr(self, 'managed_statemachine', None):
            return

        managed_name = getattr(self.managed_statemachine, '__name__', None)
        if not managed_name:
            raise HandlerValidationException("managed_statemachine must have a __name__ attribute.")

        ground_truth_list = getattr(self, 'interaction_ground_truth_list', None)
        if not ground_truth_list:
            return

        missing = []
        for ground_truth in ground_truth_list:
            if not isinstance(ground_truth, list):
                continue
            for entry in ground_truth:
                if not isinstance(entry, dict):
                    continue
                if entry.get('target_statemachine') == managed_name:
                    action = entry.get('action')
                    event = entry.get('event')
                    if not action or not event:
                        continue
                    found = False
                    for attr in dir(self):
                        fn = getattr(self, attr, None)
                        if hasattr(fn, '_agentcore_message_handler'):
                            try:
                                handler_action, handler_event = fn._agentcore_message_handler
                            except Exception:
                                continue
                            if handler_action == action and handler_event == event:
                                found = True
                                break
                    if not found:
                        missing.append((action, event, entry.get('description', '')))

        if missing:
            msg = "Missing message handlers:\n" + "\n".join(
                f"  action={a}, event={e}, desc={d}" for a, e, d in missing
            )
            raise HandlerValidationException(msg)

    @property
    @abstractmethod
    def managed_statemachine(self):
        """Subclasses must declare the managed statemachine (or None)."""
        pass

    @property
    @abstractmethod
    def interaction_ground_truth_list(self):
        """Subclasses must declare the ground truth list (can be empty)."""
        pass

    @abstractmethod
    def _create_user(self) -> Any:
        """Create and return the user object. Must be implemented by subclass."""
        raise NotImplementedError

    @abstractmethod
    def _create_manager(self) -> Any:
        """Create and return the manager object. Must be implemented by subclass."""
        raise NotImplementedError


    def launch(self, sim_clock: str, **kwargs) -> None:
        """Launch the app and login the manager."""
        if self.manager and hasattr(self.manager, "login"):
            try:
                self.manager.login(sim_clock)
            except Exception as e:
                logging.warning(f"Failed to login manager: {str(e)}")

    def close(self, sim_clock: str) -> None:
        """Close the app and logout the manager."""
        self.exited_market = True
        if self.manager and hasattr(self.manager, "logout"):
            try:
                self.manager.logout(sim_clock)
            except Exception as e:
                logging.warning(f"Failed to logout {self.get_manager()}: {str(e)}")

    def get_transition_probability(self, condition, default):
        try:
            for rule in self.behavior.get('profile', {}).get('transition_prob'):
                if rule[0] == condition:
                    return rule[1]
        except: pass

        return default

    # def update_current(self, sim_clock: str, current_loc: Any) -> None:
    #     """Update the latest simulation clock and location."""
    #     self.latest_sim_clock = sim_clock
    #     self.latest_loc = current_loc
    def update_current(self, sim_clock: str) -> None:
        """Update the latest simulation clock and location."""
        self.latest_sim_clock = sim_clock
        if hasattr(self, 'current_loc'):
            self.latest_loc = self.current_loc

    @abstractmethod
    def handle_app_topic_messages(self, *args, **kwargs) -> None:
        """Default message handler for app topic messages. Subclasses should override."""
        raise NotImplementedError

    def enqueue_message(self, payload: Any) -> None:
        """Add a message to the end of the queue."""
        self.message_queue.append(payload)

    def dequeue_message(self) -> Optional[Any]:
        """Remove and return the first message from the queue, or None if empty."""
        if not self.message_queue:
            logging.debug("Message queue is empty on dequeue.")
            return None
        return self.message_queue.pop(0)

    def enfront_message(self, payload: Any) -> None:
        """Add a message to the front of the queue."""
        self.message_queue.insert(0, payload)

    def get_manager(self) -> Optional[Any]:
        """Return the manager as a dict, if available."""
        if self.manager and hasattr(self.manager, "as_dict"):
            return self.manager.as_dict()
        return None

    @property
    def is_exited(self) -> bool:
        """Check if the app has exited the market."""
        return self.exited_market
