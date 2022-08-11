""" Transition decorator. """
import abc
import typing
import inspect as py_inspect
import warnings

from sqlalchemy.ext.hybrid import HYBRID_METHOD
from sqlalchemy.orm.interfaces import InspectionAttrInfo

from . import bound, cache, exc, state_machine
from .meta import FSMMeta


@cache.dict_cache
def sql_equality_cache(key):
    """It takes a bit of time for sqlalchemy to generate these.

    So I'm caching them.
    """
    (column, target) = key
    assert target, "Target must be defined."
    return column == target


class BoundTransitionABC(abc.ABC):
    """Abstract class controlling interfaces provided by bound transition objects."""

    @abc.abstractproperty
    def transition(self) -> state_machine.FSMStateTransition:
        """Particular state this transition targets"""
        raise NotImplementedError

    @abc.abstractproperty
    def state_machine(self) -> state_machine.FiniteStateMachine:
        """Top-level state machine"""
        raise NotImplementedError

    @abc.abstractmethod
    def __call__(self, *args, **kwargs):
        raise NotImplementedError


class ClassBoundFsmTransition(BoundTransitionABC):

    __slots__ = (
        "_sa_fsm_owner_cls",
        "state_machine",
        "transition",
    )

    def __init__(
        self,
        state_machine: state_machine.FiniteStateMachine,
        transition: state_machine.FSMStateTransition,
        owner_cls,
    ):
        self._sa_fsm_owner_cls = owner_cls
        self.state_machine = state_machine
        self.transition = transition

    def __call__(self):
        """Return a SQLAlchemy filter for this particular state."""
        column = self._sa_fsm_sqla_handle.fsm_column
        target = self._sa_fsm_meta.target
        return sql_equality_cache.get_value((column, target))

    def is_(self, value):
        if isinstance(value, bool):
            out = self().is_(value)
        else:
            warnings.warn("Unexpected is_ argument: {!r}".format(value))
            # Can be used as sqlalchemy filer. Won't match anything
            out = False
        return out


class InstanceBoundFsmTransition(BoundTransitionABC):

    __slots__ = ClassBoundFsmTransition.__slots__ + ("_sa_fsm_self",)

    def __init__(
        self,
        cls_state_machine: state_machine.FiniteStateMachine,
        transition: state_machine.FSMStateTransition,
        owner_cls,
        instance,
    ):
        self.state_machine = state_machine.InstanceBoundStateMachine(
            cls_state_machine, instance
        )
        self.transition = transition
        self._sa_fsm_owner_cls = owner_cls
        self._sa_fsm_self = instance

    def __call__(self):
        """Check if this is the current state of the object."""
        bound_meta = self._sa_fsm_bound_meta
        return bound_meta.target_state == bound_meta.current_state

    def set(self, *args, **kwargs):
        """Transition the FSM to this new state."""
        bound_meta = self._sa_fsm_bound_meta
        func = self._sa_fsm_transition_fn

        if not bound_meta.transition_possible():
            raise exc.InvalidSourceStateError(
                "Unable to switch from {} using method {}".format(
                    bound_meta.current_state, func.__name__
                )
            )
        if not bound_meta.conditions_met(args, kwargs):
            raise exc.PreconditionError("Preconditions are not satisfied.")
        return bound_meta.to_next_state(args, kwargs)

    def can_proceed(self, *args, **kwargs):
        bound_meta = self._sa_fsm_bound_meta
        return bound_meta.transition_possible() and bound_meta.conditions_met(
            args, kwargs
        )


class FsmTransition(InspectionAttrInfo):

    is_attribute = True
    extension_type = HYBRID_METHOD
    _sa_fsm_is_transition = True

    meta: FSMMeta

    def __init__(self, meta):
        self.meta = meta

    def __get__(self, instance, owner) -> BoundTransitionABC:
        try:
            fsm_machine_root = owner._sa_fsm_sqlalchemy_handle
        except AttributeError:
            # Owner class is not bound to sqlalchemy handle object
            fsm_machine_root = state_machine.FiniteStateMachine(owner)
            owner._sa_fsm_sqlalchemy_handle = fsm_machine_root

        fsm_transition = fsm_machine_root.register_transition(self.meta)
        
        if instance is None:
            return ClassBoundFsmTransition(fsm_machine_root, fsm_transition, owner)
        else:
            return InstanceBoundFsmTransition(
                fsm_machine_root, fsm_transition, owner, instance
            )


def transition(source="*", target=None, conditions=()):
    def inner_transition(subject):
        print(globals())
        if py_inspect.isfunction(subject):
            meta = FSMMeta(
                source, target, conditions, (), bound.BoundFSMFunction, subject
            )
        elif py_inspect.isclass(subject):
            # Assume a class with multiple handles for various source states
            meta = FSMMeta(source, target, conditions, (), bound.BoundFSMClass, subject)
        else:
            raise NotImplementedError(f"Do not know how to {subject!r}")

        return FsmTransition(meta)

    return inner_transition
