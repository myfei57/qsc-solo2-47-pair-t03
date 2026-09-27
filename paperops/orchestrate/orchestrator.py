"""Runs a declared plan: parallel branches, fair mutex, clean failure.

The scheduler thread is the single owner of step states. Workers only act on
resources handed to them by the pool and report back, so states never change
concurrently. A branch that fails aborts the run: every step that had not
started yet is cancelled and the failing step releases everything it held
before the run settles, leaving no resource behind.
"""

import inspect
from concurrent.futures import ThreadPoolExecutor
from threading import Condition, Event, Thread
from typing import Any, Callable

from ..clock import IdSequence
from ..event import EventBus, topics
from ..errors import PlanCancelledError
from .plan import Plan
from .resources import ResourcePool
from .status import (
    CANCELLED,
    FAILED,
    PENDING,
    READY,
    RUNNING,
    SUCCEEDED,
    WAITING,
    StepContext,
    StepResult,
    RunReport,
)


class RunHandle:
    """A live run: wait for it, inspect it, or cancel it."""

    def __init__(self, scheduler: "_RunScheduler") -> None:
        self._scheduler = scheduler

    @property
    def run_id(self) -> str:
        return self._scheduler.run_id

    @property
    def plan(self) -> Plan:
        return self._scheduler.plan

    def wait(self, timeout: float | None = None) -> RunReport:
        return self._scheduler.wait(timeout)

    def cancel(self, reason: str = "cancelled") -> bool:
        """Ask for a cooperative abort; True if the run was still live."""
        return self._scheduler.abort(reason)

    def is_done(self) -> bool:
        return self._scheduler.settled.is_set()

    def state_of(self, name: str) -> str:
        return self._scheduler.state_of(name)

    def states(self) -> dict[str, str]:
        return self._scheduler.states()

    def holders(self) -> dict[str, str]:
        return self._scheduler.pool.holders()

    def waiting(self) -> dict[str, tuple[str, ...]]:
        return self._scheduler.pool.waiting()


class Orchestrator:
    """Declares resources and starts plans, one executor per run."""

    def __init__(
        self,
        bus: EventBus | None = None,
        max_workers: int | None = None,
        executor_factory: Callable[[int], Any] | None = None,
    ) -> None:
        self._bus = bus or EventBus()
        self._max_workers = max_workers
        self._executor_factory = executor_factory or (
            lambda workers: ThreadPoolExecutor(max_workers=workers)
        )
        self._ids = IdSequence("run")
        self._handles: dict[str, RunHandle] = {}

    @property
    def bus(self) -> EventBus:
        return self._bus

    def start(self, plan: Plan) -> RunHandle:
        """Launch a plan on a scheduler thread and return its handle."""
        run_id = self._ids.next()
        scheduler = _RunScheduler(
            plan=plan,
            run_id=run_id,
            bus=self._bus,
            max_workers=self._max_workers or max(1, len(plan.steps)),
            executor_factory=self._executor_factory,
        )
        handle = RunHandle(scheduler)
        self._handles[run_id] = handle
        scheduler.start()
        return handle

    def run(self, plan: Plan, timeout: float | None = None) -> RunReport:
        """Launch a plan and block until it settles."""
        return self.start(plan).wait(timeout)


class _RunScheduler:
    """The state machine and its single driving thread for one plan run."""

    def __init__(
        self,
        plan: Plan,
        run_id: str,
        bus: EventBus,
        max_workers: int,
        executor_factory: Callable[[int], Any],
    ) -> None:
        self.plan = plan
        self.run_id = run_id
        self._bus = bus
        self._max_workers = max_workers
        self._executor_factory = executor_factory

        self._cond = Condition()
        self.settled = Event()
        self._aborted = Event()
        self._abort_reason = ""

        self._state: dict[str, str] = {step.name: PENDING for step in plan.steps}
        self._error: dict[str, str] = {}
        self._value: dict[str, Any] = {}
        self._held: dict[str, tuple[str, ...]] = {}
        self._running: set[str] = set()
        self._failed_step: str | None = None
        self._report: RunReport | None = None
        self._thread = Thread(target=self._drive, name=f"run-{run_id}", daemon=True)
        self.pool = ResourcePool(plan.resources, wake=self._wake)
        self._executor = executor_factory(max_workers)

    # ------------------------------------------------------------- lifecycle

    def start(self) -> None:
        self._thread.start()

    def wait(self, timeout: float | None = None) -> RunReport:
        if not self.settled.wait(timeout):
            raise TimeoutError(f"{self.run_id} did not settle within {timeout}s")
        return self._report  # type: ignore[return-value]

    def abort(self, reason: str) -> bool:
        with self._cond:
            if self.settled.is_set():
                return False
            first = not self._aborted.is_set()
            self._aborted.set()
            self._abort_reason = reason
        if first:
            self._wake()
        return first

    def _wake(self) -> None:
        with self._cond:
            self._cond.notify_all()

    # -------------------------------------------------------------- snapshots

    def state_of(self, name: str) -> str:
        with self._cond:
            return self._state[name]

    def states(self) -> dict[str, str]:
        with self._cond:
            return dict(self._state)

    # ------------------------------------------------------------ drive loop

    def _drive(self) -> None:
        self._publish(topics.TOPIC_RUN_STARTED, {"plan": self.plan.name})
        executor = self._executor
        try:
            while True:
                with self._cond:
                    self._cond.wait_for(self._settle_or_progress)
                    if self._aborted.is_set():
                        self._cancel_rest()
                    else:
                        self._seat_ready(executor)
                    done = self._is_settled()
                if done:
                    break
            self._finish()
        finally:
            executor.shutdown(wait=True)

    def _settle_or_progress(self) -> bool:
        """Wait predicate: aborted, finished, or something can be seated now."""
        if self._aborted.is_set() or self._is_settled():
            return True
        return any(
            self._state[name] == PENDING and self._predecessors_ok(name)
            for name in self._state
        ) or any(
            self._state[name] in (READY, WAITING) and self.pool.grantable(name)
            for name in self._state
        )

    def _predecessors_ok(self, name: str) -> bool:
        return all(
            self._state[dep] == SUCCEEDED for dep in self.plan.step(name).after
        )

    def _seat_ready(self, executor: Any) -> None:
        """Grant queued branches first, then enrol newly-ready ones."""
        for step in self.plan.steps:
            name = step.name
            if self._state[name] in (READY, WAITING) and self.pool.grantable(name):
                self._launch(name, executor)

        for step in self.plan.steps:
            name = step.name
            if self._state[name] != PENDING or not self._predecessors_ok(name):
                continue
            if not step.resources:
                self._state[name] = READY
                self._publish(topics.TOPIC_STEP_READY, {"step": name})
                self._launch(name, executor)
                continue
            self.pool.request(name, step.resources)
            if self.pool.grantable(name):
                self._state[name] = READY
                self._publish(topics.TOPIC_STEP_READY, {"step": name})
                self._launch(name, executor)
            else:
                self._state[name] = WAITING
                self._publish(
                    topics.TOPIC_STEP_WAITING,
                    {"step": name, "resources": list(step.resources)},
                )

    def _launch(self, name: str, executor: Any) -> None:
        step = self.plan.step(name)
        claims = self.pool.grant(name) if step.resources else ()
        self._held[name] = claims
        self._state[name] = RUNNING
        self._running.add(name)
        self._publish(topics.TOPIC_STEP_GRANTED, {"step": name, "resources": list(claims)})
        self._publish(topics.TOPIC_RESOURCE_GRANTED, {"step": name, "resources": list(claims)})
        self._publish(topics.TOPIC_STEP_STARTED, {"step": name})
        context = StepContext(
            run_id=self.run_id,
            name=name,
            resources=claims,
            is_cancelled=self._aborted.is_set,
        )
        executor.submit(self._run_step, name, step.action, context)

    # ---------------------------------------------------------------- workers

    def _run_step(self, name: str, action: Callable[..., Any], context: StepContext) -> None:
        try:
            self._finish_ok(name, _invoke(action, context))
        except Exception as error:  # actions are operator code, never trusted
            self._finish_bad(name, error)
        finally:
            for resource in self.pool.release(name):
                self._publish(topics.TOPIC_RESOURCE_RELEASED, {"step": name, "resource": resource})
            self._wake()

    def _finish_ok(self, name: str, value: Any) -> None:
        with self._cond:
            if self._aborted.is_set():
                # The run was torn down while the action ran; do not adopt it.
                self._settle(name, CANCELLED)
                return
            self._value[name] = value
            self._settle(name, SUCCEEDED)
            self._publish(topics.TOPIC_STEP_SUCCEEDED, {"step": name})

    def _finish_bad(self, name: str, error: Exception) -> None:
        message = f"{type(error).__name__}: {error}"
        with self._cond:
            if self._aborted.is_set() and isinstance(error, PlanCancelledError):
                self._settle(name, CANCELLED)
                self._publish(topics.TOPIC_STEP_CANCELLED, {"step": name, "reason": self._abort_reason})
            else:
                first = self._failed_step is None
                self._error[name] = message
                self._settle(name, FAILED)
                if first:
                    self._failed_step = name
                    self._aborted.set()
                    self._abort_reason = f"{name} failed"
                self._publish(topics.TOPIC_STEP_FAILED, {"step": name, "error": message})

    def _settle(self, name: str, state: str) -> None:
        self._state[name] = state
        self._running.discard(name)

    # ------------------------------------------------------------- teardown

    def _cancel_rest(self) -> None:
        """Mark everything not yet running as cancelled and leave the queues."""
        for name in list(self._state):
            if self._state[name] in (PENDING, READY, WAITING):
                self.pool.withdraw(name)
                self._state[name] = CANCELLED
                self._publish(
                    topics.TOPIC_STEP_CANCELLED,
                    {"step": name, "reason": self._abort_reason},
                )

    def _is_settled(self) -> bool:
        return not self._running and all(
            state in (SUCCEEDED, FAILED, CANCELLED) for state in self._state.values()
        )

    def _finish(self) -> None:
        with self._cond:
            if self._failed_step is not None:
                overall = FAILED
            elif any(state == CANCELLED for state in self._state.values()):
                overall = CANCELLED
            else:
                overall = SUCCEEDED

            results = []
            for step in self.plan.steps:
                name = step.name
                results.append(
                    StepResult(
                        name=name,
                        state=self._state[name],
                        value=self._value.get(name),
                        error=self._error.get(name),
                        resources=self._held.get(name, ()),
                    )
                )
            self._report = RunReport(
                run_id=self.run_id,
                plan=self.plan.name,
                state=overall,
                steps=tuple(results),
                failed_step=self._failed_step,
            )
            report = self._report
            self.settled.set()
        self._publish(
            topics.TOPIC_RUN_FINISHED,
            {"plan": self.plan.name, "state": overall, "failed_step": report.failed_step},
        )

    def _publish(self, topic: str, payload: dict[str, Any]) -> None:
        payload = {"run_id": self.run_id, **payload}
        self._bus.publish(topic, payload)


def _invoke(action: Callable[..., Any], context: StepContext) -> Any:
    """Call a zero-argument action, or pass the context when it asks for one."""
    try:
        parameters = inspect.signature(action).parameters
    except (TypeError, ValueError):
        return action()
    accepts = any(
        parameter.kind
        in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.POSITIONAL_ONLY)
        for parameter in parameters.values()
    ) or any(
        parameter.kind == inspect.Parameter.VAR_POSITIONAL
        for parameter in parameters.values()
    )
    return action(context) if accepts else action()
