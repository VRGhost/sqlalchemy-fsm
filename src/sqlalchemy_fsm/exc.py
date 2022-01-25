"""FSM exceptions."""


class FSMException(Exception):
    """Generic Finite State Machine Exception."""


class PreconditionError(FSMException):
    """Raised when transition conditions are not satisfied."""


class SetupError(FSMException):
    """Raised when FSM is configured incorrectly."""


class InvalidSourceStateError(FSMException, NotImplementedError):
    """Can not switch from current state to the requested state."""
