import pytest
import sqlalchemy

from sqlalchemy_fsm import FSMField, transition, exc


from tests.conftest import Base


class NotFsm(Base):
    __tablename__ = "NotFsm"
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)

    @transition(source="*", target="blah")
    def change_state(self):
        pass

    def not_transition(self):
        pass


def test_not_fsm():
    with pytest.raises(exc.SetupError) as err:
        NotFsm().change_state()
    assert "No FSMField found in model" in str(err)


def test_not_transition():
    with pytest.raises(AttributeError):
        NotFsm.not_transition.can_proceed()


class TooMuchFsm(Base):
    __tablename__ = "TooMuchFsm"
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    state1 = sqlalchemy.Column(FSMField)
    state2 = sqlalchemy.Column(FSMField)

    @transition(source="*", target="blah")
    def change_state(self):
        pass


def test_too_much_fsm():
    with pytest.raises(exc.SetupError) as err:
        TooMuchFsm().change_state()
    assert "More than one FSMField found in model" in str(err)


def test_transition_raises_on_unknown():
    class MyCallable(object):
        def __call__(*args):
            pass

    with pytest.raises(NotImplementedError) as err:

        wrapper = transition(source="*", target="blah")
        wrapper(MyCallable())

    assert "Do not know how to" in str(err)


def test_transition_raises_on_invalid_state():
    with pytest.raises(NotImplementedError) as err:

        @transition(source=42, target="blah")
        def func():
            pass

    assert "42" in str(err)

    with pytest.raises(NotImplementedError) as err:

        @transition(source="*", target=42)
        def func():
            pass

    assert "42" in str(err)

    with pytest.raises(NotImplementedError) as err:

        @transition(source=["str", 42], target="blah")
        def func():
            pass

    assert "42" in str(err)


def one_arg_condition():
    def one_arg_condition(instance, arg1):
        return True

    return one_arg_condition


class MisconfiguredTransitions(Base):
    __tablename__ = "MisconfiguredTransitions"
    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True)
    state = sqlalchemy.Column(FSMField)

    @transition(source="*", target="blah", conditions=[one_arg_condition()])
    def change_state(self):
        """Condition accepts one arg, state handler doesn't -> exception."""
        pass

    @transition(source="*", target="blah")
    class multi_handler_transition(object):
        """The system won't know which transition{1,2} handler to chose."""

        @transition()
        def transition1(self, instance):
            pass

        @transition()
        def transition2(self, instance):
            pass

    @transition(source="*", target="blah")
    class incompatible_targets(object):
        """The system won't know which transition{1,2} handler to chose."""

        @transition(target="not-blah")
        def transition1(self, instance):
            pass

    @transition(source=["src1", "src2"], target="blah")
    class incompatible_sources(object):
        """The system won't know which transition{1,2} handler to chose."""

        @transition(source=["src3", "src4"])
        def transition1(self, instance):
            pass

    @transition(source="*", target="blah")
    class no_conflict_due_to_precondition_arg_count(object):
        @transition(conditions=[lambda self, instance, arg1: True])
        def change_state(self, instance, arg1):
            pass

        @transition()
        def no_arg_condition(self, instance):
            pass


class TestMisconfiguredTransitions(object):
    @pytest.fixture
    def model(self):
        return MisconfiguredTransitions()

    def test_misconfigured_transitions(self, model):
        with pytest.raises(exc.SetupError) as err:
            with pytest.warns(UserWarning):
                model.change_state.set(42)
        assert "Mismatch beteen args accepted" in str(err)

    def test_multi_transition_handlers(self, model):
        with pytest.raises(exc.SetupError) as err:
            model.multi_handler_transition.set()
        assert "Can transition with multiple handlers" in str(err)

    def test_incompatible_targets(self, model):
        with pytest.raises(exc.SetupError) as err:
            model.incompatible_targets.set()
        assert "are not compatable" in str(err)

    def test_incompatable_sources(self, model):
        with pytest.raises(exc.SetupError) as err:
            model.incompatible_sources.set()
        assert "are not compatable" in str(err)

    def test_no_conflict_due_to_precondition_arg_count(self, model):
        assert model.no_conflict_due_to_precondition_arg_count.can_proceed()


def test_unexpected_is__type(session):
    model = MisconfiguredTransitions()
    session.add(model)
    session.commit()
    with pytest.warns(UserWarning) as warn:
        result = (
            session.query(MisconfiguredTransitions)
            .filter(MisconfiguredTransitions.change_state.is_("hello world"))
            .all()
        )
    assert not result
    assert "Unexpected is_ argument: 'hello world'" in str(warn.list[0].message)
