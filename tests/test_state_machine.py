import pytest
from wzcore_sandbox.state_machine import DeterministicStateMachine, State, TransitionError


def test_valid_transitions_happy_path():
    sm = DeterministicStateMachine()
    sm.step(State.PROCESSING)
    sm.step(State.SUCCESS)
    assert sm.state == State.SUCCESS
    assert [(t.frm, t.to) for t in sm.transitions] == [
        (State.INIT, State.PROCESSING),
        (State.PROCESSING, State.SUCCESS),
    ]


def test_invalid_transition_raises():
    sm = DeterministicStateMachine()
    with pytest.raises(TransitionError):
        sm.step(State.SUCCESS)


def test_retry_path():
    sm = DeterministicStateMachine()
    sm.step(State.PROCESSING)
    sm.step(State.FAILED)
    sm.step(State.PROCESSING)
    sm.step(State.SUCCESS)
    assert sm.state == State.SUCCESS
