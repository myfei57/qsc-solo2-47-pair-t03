"""Run states, the action-side context and the final run report."""

from dataclasses import dataclass
from typing import Any, Callable

from ..errors import PlanCancelledError

# A step waits until every predecessor succeeded.
PENDING = "pending"
# Dependencies are done; the scheduler is trying to seat it on its resources.
READY = "ready"
# Ready but at least one declared resource is still held by another step.
WAITING = "waiting"
# Resources in hand, action running on a worker.
RUNNING = "running"
# Action returned.
SUCCEEDED = "succeeded"
# Action raised, or raised after the run was aborted.
FAILED = "failed"
# Never started because another step failed or the run was cancelled.
CANCELLED = "cancelled"

TERMINAL = frozenset({SUCCEEDED, FAILED, CANCELLED})

RUNNING_STATES = frozenset({PENDING, READY, WAITING, RUNNING})


@dataclass(frozen=True)
class StepResult:
    """How one step ended."""

    name: str
    state: str
    value: Any = None
    error: str | None = None
    resources: tuple[str, ...] = ()


@dataclass(frozen=True)
class RunReport:
    """The whole procedure once it settles."""

    run_id: str
    plan: str
    state: str
    steps: tuple[StepResult, ...]
    failed_step: str | None = None

    def result(self, name: str) -> StepResult:
        for step in self.steps:
            if step.name == name:
                return step
        raise KeyError(name)

    def succeeded(self) -> tuple[str, ...]:
        return tuple(step.name for step in self.steps if step.state == SUCCEEDED)

    def cancelled(self) -> tuple[str, ...]:
        return tuple(step.name for step in self.steps if step.state == CANCELLED)

    def as_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "plan": self.plan,
            "state": self.state,
            "failed_step": self.failed_step,
            "steps": [
                {
                    "name": step.name,
                    "state": step.state,
                    "error": step.error,
                    "resources": list(step.resources),
                }
                for step in self.steps
            ],
        }


@dataclass
class StepContext:
    """What an action may see and do while it runs."""

    run_id: str
    name: str
    resources: tuple[str, ...]
    is_cancelled: Callable[[], bool]

    def check_cancelled(self) -> None:
        """Raise if the run was aborted; lets long actions stop cooperatively."""
        if self.is_cancelled():
            raise PlanCancelledError(f"{self.name} stopped because the run was aborted")

    def state(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "step": self.name,
            "resources": list(self.resources),
            "cancelled": self.is_cancelled(),
        }
