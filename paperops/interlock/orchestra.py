"""Branching procedures whose steps keep exclusive resources while they run.

A StageSequence walks one stage after another. An Orchestration instead
declares which step has to wait for which, and which resources a step keeps
while it runs; everything else may run at the same time. Two steps that want
the same resource are serialised by the pool, and a step that fails or is
cancelled hands every resource it kept back to the pool.

    pool = ResourcePool(("crane", "steam.header"))
    startup = Orchestration("line.startup", pool)
    startup.add("steam.drain", drain_header, resources=("steam.header",))
    startup.add("steam.admit", admit_steam, after=("steam.drain",), resources=("steam.header",))
    startup.add("glue.prepare", prepare_glue)                    # runs alongside steam
    startup.add("reel.change", change_spool, resources=("crane",))
    startup.add("reel.retract", retract_arm, after=("reel.change",), resources=("crane",))
    report = startup.run()
"""

import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Sequence

from ..errors import DuplicateStepError, UnknownResourceError, UnknownStepError
from .resources import ResourcePool

PENDING = "pending"
RUNNING = "running"
DONE = "done"
FAILED = "failed"
SKIPPED = "skipped"
CANCELLED = "cancelled"


@dataclass(frozen=True)
class Step:
    """One declared step: what runs, what it waits for, what it keeps."""

    name: str
    action: Callable[[], Any]
    after: tuple[str, ...] = ()
    resources: tuple[str, ...] = ()


@dataclass(frozen=True)
class RunReport:
    """How a run settled: every step's position and what went wrong."""

    orchestration: str
    steps: dict[str, str]
    errors: dict[str, str]
    complete: bool
    cancelled: bool

    @property
    def failed(self) -> bool:
        return FAILED in self.steps.values()

    @property
    def ok(self) -> bool:
        return self.complete and not self.cancelled and not self.failed

    def state(self) -> dict[str, Any]:
        return {
            "orchestration": self.orchestration,
            "steps": dict(self.steps),
            "errors": dict(self.errors),
            "complete": self.complete,
            "cancelled": self.cancelled,
            "failed": self.failed,
            "ok": self.ok,
        }


class Orchestration:
    """A declared procedure; branches run together, resources stay exclusive."""

    def __init__(self, name: str, pool: ResourcePool) -> None:
        self._name = name
        self._pool = pool
        self._steps: dict[str, Step] = {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def pool(self) -> ResourcePool:
        return self._pool

    def add(
        self,
        name: str,
        action: Callable[[], Any],
        *,
        after: Iterable[str] = (),
        resources: Iterable[str] = (),
    ) -> Step:
        """Declare a step; predecessors and resources have to be declared first.

        Requiring predecessors to exist already keeps a procedure from ever
        pointing back at itself, so a dependency cycle cannot be declared.
        """
        if name in self._steps:
            raise DuplicateStepError(f"{self._name} already declares step {name}")
        missing = [predecessor for predecessor in after if predecessor not in self._steps]
        if missing:
            raise UnknownStepError(f"{self._name} does not declare step {missing[0]}")
        for resource in resources:
            if not self._pool.declares(resource):
                raise UnknownResourceError(f"the pool does not declare {resource}")
        step = Step(
            name=name,
            action=action,
            after=tuple(dict.fromkeys(after)),
            resources=tuple(dict.fromkeys(resources)),
        )
        self._steps[name] = step
        return step

    def steps(self) -> tuple[Step, ...]:
        return tuple(self._steps.values())

    def start(self) -> "RunHandle":
        """Begin a run; every step gets its own line and waits for its turn."""
        if not self._steps:
            raise ValueError("an orchestration needs at least one step")
        handle = RunHandle(self._name, self._pool, self.steps())
        handle.start()
        return handle

    def run(self, timeout: float | None = None) -> RunReport:
        """Start a run and wait for it to settle."""
        return self.start().wait(timeout)

    def state(self) -> dict[str, Any]:
        return {
            "orchestration": self._name,
            "steps": [
                {"step": step.name, "after": list(step.after), "resources": list(step.resources)}
                for step in self.steps()
            ],
        }


class RunHandle:
    """One live run of an orchestration."""

    def __init__(self, orchestration: str, pool: ResourcePool, steps: Sequence[Step]) -> None:
        self._orchestration = orchestration
        self._pool = pool
        self._steps = tuple(steps)
        self._cancel = threading.Event()
        self._lock = threading.Lock()
        self._status = {step.name: PENDING for step in self._steps}
        self._errors: dict[str, str] = {}
        self._done = {step.name: threading.Event() for step in self._steps}
        self._threads = tuple(
            threading.Thread(
                target=self._run_step,
                args=(step,),
                name=f"{orchestration}:{step.name}",
                daemon=True,
            )
            for step in self._steps
        )

    def start(self) -> "RunHandle":
        for thread in self._threads:
            thread.start()
        return self

    def cancel(self) -> None:
        """Stop steps that have not started; running steps finish and release."""
        self._cancel.set()

    @property
    def cancelled(self) -> bool:
        return self._cancel.is_set()

    def done(self) -> bool:
        return all(event.is_set() for event in self._done.values())

    def wait(self, timeout: float | None = None) -> RunReport:
        """Block until every step settled, or until the timeout runs out."""
        deadline = None if timeout is None else time.monotonic() + timeout
        for thread in self._threads:
            remaining = None if deadline is None else max(0.0, deadline - time.monotonic())
            thread.join(remaining)
        return self.report()

    def report(self) -> RunReport:
        with self._lock:
            steps = dict(self._status)
            errors = dict(self._errors)
        return RunReport(
            orchestration=self._orchestration,
            steps=steps,
            errors=errors,
            complete=self.done(),
            cancelled=self.cancelled,
        )

    def _run_step(self, step: Step) -> None:
        try:
            for predecessor in step.after:
                self._done[predecessor].wait()
            if self._cancel.is_set():
                self._settle(step.name, CANCELLED)
                return
            if any(self._status_of(predecessor) != DONE for predecessor in step.after):
                self._settle(step.name, SKIPPED)
                return
            if step.resources and not self._pool.acquire(
                step.name, step.resources, should_stop=self._cancel.is_set
            ):
                self._settle(step.name, CANCELLED)
                return
            self._settle(step.name, RUNNING)
            try:
                step.action()
            except Exception as exc:
                with self._lock:
                    self._errors[step.name] = f"{type(exc).__name__}: {exc}"
                self._settle(step.name, FAILED)
            else:
                self._settle(step.name, DONE)
            finally:
                self._pool.release_all(step.name)
        finally:
            self._done[step.name].set()

    def _settle(self, step: str, status: str) -> None:
        with self._lock:
            self._status[step] = status

    def _status_of(self, step: str) -> str:
        with self._lock:
            return self._status[step]
