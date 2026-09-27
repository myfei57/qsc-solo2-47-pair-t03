"""Declarative orchestration: parallel branches, fair mutex, clean teardown."""

import threading
import time

import pytest

from paperops.errors import (
    DependencyCycleError,
    DuplicateStepError,
    EmptyPlanError,
    PlanCancelledError,
    PlanError,
    UnknownResourceError,
    UnknownStepError,
)
from paperops.event import topics
from paperops.orchestrate import (
    Orchestrator,
    Step,
    StepContext,
    plan,
)


# ----------------------------------------------------------------- plan rules


def test_plan_refuses_empty_and_duplicate_steps():
    with pytest.raises(EmptyPlanError):
        plan("p", [])
    with pytest.raises(DuplicateStepError):
        plan("p", [Step("a", lambda: 1), Step("a", lambda: 2)])


def test_plan_refuses_unknown_dependency_and_undeclared_resource():
    with pytest.raises(UnknownStepError):
        plan("p", [Step("a", lambda: 1, after=("ghost",))])
    with pytest.raises(UnknownResourceError):
        plan(
            "p",
            [Step("a", lambda: 1, resources=("header",))],
            resources=("other",),
        )


def test_plan_refuses_cycles_including_self_loops():
    cyclic = [
        Step("a", lambda: 1, after=("c",)),
        Step("b", lambda: 1, after=("a",)),
        Step("c", lambda: 1, after=("b",)),
    ]
    with pytest.raises(DependencyCycleError):
        plan("p", cyclic)
    with pytest.raises(DependencyCycleError):
        plan("p", [Step("a", lambda: 1, after=("a",))])


def test_plan_needs_a_callable_action_and_dedupes_claims():
    with pytest.raises(PlanError):
        plan("p", [Step("a", "not callable")])  # type: ignore[arg-type]
    declared = plan(
        "p",
        [Step("a", lambda: 1, resources=("r", "r", "r"))],
        resources=("r",),
    )
    assert declared.resources == frozenset({"r"})
    assert declared.step("a").resources == ("r",)
    with pytest.raises(UnknownStepError):
        declared.step("ghost")


def test_implicit_resources_are_registered():
    declared = plan("p", [Step("a", lambda: 1, resources=("hdr", "pump"))])
    assert declared.resources == frozenset({"hdr", "pump"})


# --------------------------------------------------------------- sequencing


def test_independent_steps_run_on_parallel_branches():
    lock = threading.Lock()
    seen = {"active": 0, "max": 0}

    def action():
        with lock:
            seen["active"] += 1
            seen["max"] = max(seen["max"], seen["active"])
        time.sleep(0.1)
        with lock:
            seen["active"] -= 1

    procedure = plan("par", [Step("a", action), Step("b", action), Step("c", action)])
    report = Orchestrator().run(procedure, timeout=5)

    assert report.state == "succeeded"
    assert seen["max"] == 3
    assert report.succeeded() == ("a", "b", "c")


def test_dependencies_open_only_after_every_predecessor_succeeds():
    order: list[str] = []

    def mark(tag):
        def action():
            order.append(tag)
            return tag

        return action

    procedure = plan(
        "dag",
        [
            Step("a", mark("a")),
            Step("b", mark("b"), after=("a",)),
            Step("c", mark("c"), after=("a",)),
            Step("d", mark("d"), after=("b", "c")),
        ],
    )
    report = Orchestrator().run(procedure)

    assert order.index("a") < order.index("b")
    assert order.index("a") < order.index("c")
    assert order.index("b") < order.index("d")
    assert order.index("c") < order.index("d")
    assert report.result("d").value == "d"
    assert report.as_dict()["state"] == "succeeded"


# ------------------------------------------------------------------- mutexes


def test_steps_claiming_the_same_resource_never_overlap():
    lock = threading.Lock()
    holder: dict[str, str] = {}
    overlaps: list[tuple[str, str]] = []

    def action(tag):
        def body():
            with lock:
                if "hdr" in holder:
                    overlaps.append((tag, holder["hdr"]))
                holder["hdr"] = tag
            time.sleep(0.04)
            with lock:
                holder.pop("hdr")

        return body

    procedure = plan(
        "mutex",
        [
            Step("a", action("a"), resources=("hdr",)),
            Step("b", action("b"), resources=("hdr",)),
            Step("c", action("c"), resources=("hdr",)),
        ],
    )
    handle = Orchestrator().start(procedure)
    report = handle.wait(5)

    assert report.state == "succeeded"
    assert overlaps == []
    assert handle.holders() == {}


def test_loser_of_a_race_waits_behind_the_holder_then_proceeds():
    first_in = threading.Event()
    order: list[str] = []

    procedure = plan(
        "race",
        [
            Step(
                "a",
                lambda: (first_in.wait(2), order.append("a")),
                resources=("hdr",),
            ),
            Step("b", lambda: order.append("b"), resources=("hdr",)),
        ],
    )
    orchestrator = Orchestrator()
    handle = orchestrator.start(procedure)

    # Both steps are ready; a won and runs, b sits behind it in the queue.
    deadline = time.monotonic() + 2
    while not handle.waiting() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert handle.waiting() == {"hdr": ("b",)}
    assert handle.holders() == {"hdr": "a"}

    first_in.set()
    report = handle.wait(5)
    assert report.state == "succeeded"
    assert order == ["a", "b"]
    assert handle.holders() == {}


def test_multi_resource_request_is_granted_atomically():
    lock = threading.Lock()
    held: set[str] = set()
    splits: list[str] = []

    def action(tag, wanted):
        def body():
            with lock:
                for resource in wanted:
                    if resource in held:
                        splits.append(resource)
                held.update(wanted)
            time.sleep(0.03)
            with lock:
                held.difference_update(wanted)

        return body

    procedure = plan(
        "atomic",
        [
            Step("a", action("a", ("pump", "valve")), resources=("pump", "valve")),
            Step("b", action("b", ("pump", "valve")), resources=("valve", "pump")),
        ],
    )
    report = Orchestrator().run(procedure, 5)
    assert report.state == "succeeded"
    assert splits == []
    assert report.result("a").resources == ("pump", "valve")
    assert report.result("b").resources == ("valve", "pump")


# ------------------------------------------------------------- failure path


def test_failed_branch_releases_resources_and_cancels_the_rest():
    def boom():
        raise RuntimeError("valve stuck")

    procedure = plan(
        "stop",
        [
            Step("prep", lambda: None, resources=("hdr",)),
            Step("open", boom, after=("prep",)),
            Step("downstream", lambda: None, after=("open",), resources=("hdr",)),
            Step("sibling", lambda: None, after=("open",)),
        ],
    )
    orchestrator = Orchestrator()
    handle = orchestrator.start(procedure)
    report = handle.wait(5)

    assert report.state == "failed"
    assert report.failed_step == "open"
    assert report.result("open").state == "failed"
    assert report.result("open").error == "RuntimeError: valve stuck"
    assert report.cancelled() == ("downstream", "sibling")
    assert handle.holders() == {}
    assert handle.waiting() == {}


def test_two_failures_share_one_failure_origin():
    procedure = plan(
        "two",
        [
            Step("a", lambda: (_ for _ in ()).throw(RuntimeError("a"))),
            Step("b", lambda: (_ for _ in ()).throw(RuntimeError("b"))),
        ],
    )
    report = Orchestrator().run(procedure, 5)
    assert report.state == "failed"
    assert report.failed_step in ("a", "b")
    assert report.result("a").state in ("failed", "cancelled")
    assert report.result("b").state in ("failed", "cancelled")


# -------------------------------------------------------------- cancellation


def test_cancel_aborts_waiting_and_cooperating_steps_cleanly():
    entered = threading.Event()

    def long_action(ctx: StepContext) -> None:
        entered.set()
        for _ in range(200):
            time.sleep(0.01)
            ctx.check_cancelled()

    def never() -> None:
        raise AssertionError("queued step must not run after cancel")

    orchestrator = Orchestrator()
    procedure = plan(
        "cancel",
        [
            Step("a", long_action, resources=("hdr",)),
            Step("b", never, resources=("hdr",)),
        ],
    )
    handle = orchestrator.start(procedure)
    assert entered.wait(2)
    deadline = time.monotonic() + 2
    while not handle.waiting() and time.monotonic() < deadline:
        time.sleep(0.01)

    assert handle.cancel("operator stop") is True
    report = handle.wait(5)

    assert report.state == "cancelled"
    assert report.result("a").state == "cancelled"
    assert report.result("b").state == "cancelled"
    assert handle.holders() == {}
    assert handle.cancel() is False  # idempotent once settled


def test_action_returning_after_abort_is_not_adopted():
    def fail() -> None:
        raise RuntimeError("first")

    def slow_deaf() -> int:
        time.sleep(0.05)
        return 42

    procedure = plan(
        "deaf",
        [
            Step("f", fail),
            Step("s", slow_deaf, resources=("r",)),
        ],
    )
    orchestrator = Orchestrator()
    handle = orchestrator.start(procedure)
    report = handle.wait(5)

    assert report.state == "failed"
    assert report.failed_step == "f"
    assert report.result("s").state == "cancelled"
    assert report.result("s").value is None
    assert handle.holders() == {}


def test_plan_cancelled_error_only_counts_as_cancel_when_aborted():
    # If an action raises PlanCancelledError on its own (no abort), it is a failure.
    procedure = plan("odd", [Step("a", lambda: (_ for _ in ()).throw(PlanCancelledError("x")))])
    report = Orchestrator().run(procedure, 5)
    assert report.state == "failed"
    assert report.failed_step == "a"


# -------------------------------------------------------------------- events


def test_run_publishes_grant_and_release_events():
    orchestrator = Orchestrator()
    procedure = plan(
        "ev",
        [Step("a", lambda: 1, resources=("hdr",)), Step("b", lambda: 2, after=("a",))],
    )
    report = orchestrator.run(procedure, 5)
    bus = orchestrator.bus

    topics_seen = [event.topic for event in bus.records()]
    assert topics_seen[0] == topics.TOPIC_RUN_STARTED
    assert topics_seen[-1] == topics.TOPIC_RUN_FINISHED
    assert topics.TOPIC_RESOURCE_GRANTED in topics_seen
    assert topics.TOPIC_RESOURCE_RELEASED in topics_seen
    assert topics_seen.count(topics.TOPIC_RUN_FINISHED) == 1

    release = next(
        event.payload
        for event in bus.records()
        if event.topic == topics.TOPIC_RESOURCE_RELEASED
    )
    assert release == {"run_id": report.run_id, "step": "a", "resource": "hdr"}


def test_waiting_is_announced_when_a_resource_is_contended():
    entered = threading.Event()
    release = threading.Event()
    orchestrator = Orchestrator()
    procedure = plan(
        "wait-event",
        [
            Step("a", lambda: (entered.set(), release.wait(2)), resources=("hdr",)),
            Step("b", lambda: None, resources=("hdr",)),
        ],
    )
    handle = orchestrator.start(procedure)
    assert entered.wait(2)
    deadline = time.monotonic() + 2
    while orchestrator.bus.count(topics.TOPIC_STEP_WAITING) == 0 and time.monotonic() < deadline:
        time.sleep(0.01)
    release.set()
    assert handle.wait(5).state == "succeeded"
    payload = next(
        event.payload
        for event in orchestrator.bus.records()
        if event.topic == topics.TOPIC_STEP_WAITING
    )
    assert payload["step"] == "b"
