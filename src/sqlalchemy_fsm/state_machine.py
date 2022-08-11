"""This module implements abstract state machine for the particular table"""

import threading

from sqlalchemy import inspect as sqla_inspect

from . import exc, sqltypes, meta, util


def find_fsm_column(table_class):
    fsm_fields = [
        col
        for col in sqla_inspect(table_class).columns
        if isinstance(col.type, sqltypes.FSMField)
    ]

    if len(fsm_fields) == 0:
        raise exc.SetupError("No FSMField found in model")
    elif len(fsm_fields) > 1:
        raise exc.SetupError(
            "More than one FSMField found in model ({})".format(fsm_fields)
        )
    return fsm_fields[0]


class FiniteStateMachine:
    """This is class-bound state machine."""

    __slots__ = (
        "table_class",
        "record",
        "fsm_column",
        "dispatch",
        "column_name",
        "access_lock",
        "states",
        "transitions",
    )

    def __init__(self, table_class):
        self.table_class = table_class
        # self.record = table_record_instance
        self.fsm_column = find_fsm_column(table_class)
        self.column_name = self.fsm_column.name
        self.access_lock = threading.Lock()
        self.states = {}  # state label -> FSMState
        self.transitions = []

        # if table_record_instance:
        #     self.dispatch = events.BoundFSMDispatcher(table_record_instance)

    def get_state(self, label: str):
        """Returns state if it exists, raises exception otherwise."""
        return self.states[label]

    def register_state(self, label: str):
        """Ensure that the state object exists"""
        with self.access_lock:
            return self.states.setdefault(label, FSMState(self, label))


    def register_transition(self, meta: meta.FSMMeta):
        transition = FSMStateTransition(self, meta)
        for a_state in tuple(meta.sources) + (meta.target, ):
            if util.is_valid_literal_state(a_state):
                self.register_state(a_state)
        print(self.states)

        self.transitions.append(transition)
        return transition

class FSMState:
    """A particular FSM state"""

    __slots__ = ("parent", "value", "states")

    value: str

    def __init__(self, parent: FiniteStateMachine, value: str):
        self.parent = parent
        self.value = value

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.parent=} {self.value=!r}>"

class FSMStateTransition:
    """Transition between two or more FSM states"""

    __slots__ = ( 'parent', 'meta', )

    def __init__(self, parent: FiniteStateMachine, meta: meta.FSMMeta):
        self.parent = parent
        self.meta = meta

class InstanceBoundStateMachine:
    """A proxy to the class-bound `FiniteStateMachine` that provides few extra API handles."""

    __slots__ = ("cls_fsm", "instance")

    def __init__(self, cls_fsm: FiniteStateMachine, instance):
        self.cls_fsm = cls_fsm
        self.instance = instance

    @property
    def current_raw_value(self):
        return getattr(self.instance, self.cls_fsm.column_name)

    @property
    def current_state(self) -> FSMState:
        return self.cls_fsm.get_state(self.current_raw_value)
