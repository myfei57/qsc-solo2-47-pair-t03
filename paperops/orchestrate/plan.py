"""Declarative start/stop procedures: steps, branches and shared resources.

A :class:`Plan` is a DAG of :class:`Step` declarations. ``after`` names the
steps that must finish first (the branches), and ``resources`` names the
exclusive resources the step holds while its action runs (the mutex). The
graph is checked once up front so a malformed declaration can never reach the
machinery that moves valves.
"""

from dataclasses import dataclass
from typing import Any, Callable, Iterable, Sequence

from ..errors import (
    DependencyCycleError,
    DuplicateStepError,
    EmptyPlanError,
    PlanError,
    UnknownResourceError,
    UnknownStepError,
)

Action = Callable[..., Any]


@dataclass(frozen=True)
class Step:
    """One preparation action in a procedure.

    name:     unique within the plan.
    action:   zero-argument callable, or one that accepts a
              :class:`~paperops.orchestrate.StepContext` for cooperative cancel.
    after:    steps that must succeed before this one may open.
    resources: resources held exclusively for the duration of the action.
    """

    name: str
    action: Action
    after: tuple[str, ...] = ()
    resources: tuple[str, ...] = ()


@dataclass(frozen=True)
class Plan:
    """A validated procedure: ordered name, steps and the resource universe."""

    name: str
    steps: tuple[Step, ...]
    resources: frozenset[str]

    def step(self, name: str) -> Step:
        for candidate in self.steps:
            if candidate.name == name:
                return candidate
        raise UnknownStepError(f"{self.name} does not declare step {name}")

    def names(self) -> tuple[str, ...]:
        return tuple(step.name for step in self.steps)


def plan(
    name: str,
    steps: Sequence[Step],
    resources: Iterable[str] = (),
) -> Plan:
    """Declare and validate a procedure, refusing malformed graphs at once."""
    if not steps:
        raise EmptyPlanError(f"{name or 'plan'} declares no steps")

    seen: set[str] = set()
    for step in steps:
        if not isinstance(step.name, str) or not step.name:
            raise PlanError("every step needs a non-empty name")
        if step.name in seen:
            raise DuplicateStepError(f"{name} declares step {step.name} twice")
        if not callable(step.action):
            raise PlanError(f"{name} step {step.name} needs a callable action")
        seen.add(step.name)

    declared = frozenset(resources)
    claimed: set[str] = set()
    for step in steps:
        for earlier in step.after:
            if earlier not in seen:
                raise UnknownStepError(
                    f"{name} step {step.name} waits on unknown step {earlier}"
                )
        for resource in step.resources:
            if not resource:
                raise PlanError(f"{name} step {step.name} claims an empty resource")
            claimed.add(resource)
    if declared:
        unknown = claimed - declared
        if unknown:
            raise UnknownResourceError(
                f"{name} claims undeclared resources: {sorted(unknown)}"
            )
    universe = declared | claimed

    _reject_cycle(name, steps)

    normalized = tuple(
        Step(
            name=step.name,
            action=step.action,
            after=tuple(dict.fromkeys(step.after)),
            resources=tuple(dict.fromkeys(step.resources)),
        )
        for step in steps
    )
    return Plan(name=name, steps=normalized, resources=frozenset(universe))


def _reject_cycle(name: str, steps: Sequence[Step]) -> None:
    deps = {step.name: tuple(step.after) for step in steps}
    colour: dict[str, str] = {}

    def visit(node: str) -> None:
        state = colour.get(node)
        if state == "grey":
            raise DependencyCycleError(f"{name} has a dependency cycle through {node}")
        if state == "black":
            return
        colour[node] = "grey"
        for earlier in deps[node]:
            visit(earlier)
        colour[node] = "black"

    for step in steps:
        visit(step.name)
