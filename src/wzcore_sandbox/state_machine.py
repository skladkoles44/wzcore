from __future__ import annotations
from dataclasses import dataclass
from enum import Enum
from typing import Dict, Set


class State(str, Enum):
    INIT = "INIT"
    PROCESSING = "PROCESSING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


ALLOWED: Dict[State, Set[State]] = {
    State.INIT: {State.PROCESSING},
    State.PROCESSING: {State.SUCCESS, State.FAILED},
    State.FAILED: {State.PROCESSING},  # retry path
    State.SUCCESS: set(),
}


class TransitionError(RuntimeError):
    pass


@dataclass(frozen=True)
class Transition:
    frm: State
    to: State


class DeterministicStateMachine:
    def __init__(self) -> None:
        self.state: State = State.INIT
        self.transitions: list[Transition] = []

    def step(self, to: State) -> None:
        allowed = ALLOWED.get(self.state, set())
        if to not in allowed:
            raise TransitionError(f"Invalid transition: {self.state} -> {to}")
        self.transitions.append(Transition(self.state, to))
        self.state = to
