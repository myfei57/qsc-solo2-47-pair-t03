"""Branching orchestration with exclusive resources."""

import threading
import time

import pytest

from paperops.errors import (
    DuplicateStepError,
    ResourceBusyError,
    ResourceHolderError,
    UnknownResourceError,
    UnknownStepError,
)
from paperops.interlock import Orchestration, ResourcePool
from paperops.interlock.orchestra import CANCELLED, DONE, FAILED, SKIPPED


def noop():
    pass


# ---------------------------------------------------------------------- pool


def test_pool_keeps_a_single_holder():
    pool = ResourcePool(("crane", "steam.header"))
    pool.try_acquire("glue.prepare", ("crane",))
    assert pool.holder("crane") == "glue.prepare"
    assert pool.held_by("glue.prepare") == ("crane",)
    with pytest.raises(ResourceBusyError, match="held by glue.prepare"):
        pool.try_acquire("reel.change", ("crane",))
    pool.release("glue.prepare", "crane")
    assert pool.holder("crane") is None
    assert pool.free() == ("crane", "steam.header")


def test_pool_release_needs_the_holder():
    pool = ResourcePool(("crane",))
    pool.try_acquire("reel.change", ("crane",))
    with pytest.raises(ResourceHolderError):
        pool.release("broke.clear", "crane")
    pool.release("reel.change", "crane")
    with pytest.raises(ResourceHolderError):
        pool.release("reel.change", "crane")


def test_pool_refuses_resources_it_does_not_declare():
    pool = ResourcePool(("crane",))
    with pytest.raises(UnknownResourceError):
        pool.try_acquire("reel.change", ("crane", "steam.header"))
    with pytest.raises(UnknownResourceError):
        pool.holder("steam.header")
    with pytest.raises(UnknownResourceError):
        pool.release("reel.change", "steam.header")


def test_pool_release_all_returns_everything_the_holder_keeps():
    pool = ResourcePool(("crane", "steam.header"))
    pool.try_acquire("dry.line", ("crane", "steam.header"))
    released = pool.release_all("dry.line")
    assert set(released) == {"crane", "steam.header"}
    assert pool.free() == ("crane", "steam.header")
    assert pool.release_all("dry.line") == ()


def test_pool_acquire_waits_for_the_holder_to_let_go():
    pool = ResourcePool(("crane",))
    pool.try_acquire("first", ("crane",))
    acquired = threading.Event()

    def contender():
        pool.acquire("second", ("crane",))
        acquired.set()

    thread = threading.Thread(target=contender)
    thread.start()
    assert acquired.wait(0.1) is False
    pool.release("first", "crane")
    assert acquired.wait(1.0) is True
    thread.join(1.0)
    assert pool.holder("crane") == "second"


def test_pool_acquire_can_be_asked_to_stop_waiting():
    pool = ResourcePool(("crane",))
    pool.try_acquire("first", ("crane",))
    stop = threading.Event()
    outcome = []

    def contender():
        outcome.append(
            pool.acquire("second", ("crane",), should_stop=stop.is_set, poll_interval=0.01)
        )

    thread = threading.Thread(target=contender)
    thread.start()
    time.sleep(0.05)
    stop.set()
    thread.join(1.0)
    assert outcome == [False]
    assert pool.holder("crane") == "first"


def test_pool_declaration_contracts():
    with pytest.raises(ValueError):
        ResourcePool(())
    with pytest.raises(ValueError):
        ResourcePool(("crane", "crane"))
    pool = ResourcePool(("crane", "steam.header"))
    assert pool.resources() == ("crane", "steam.header")
    assert pool.declares("crane") is True
    assert pool.state() == {"resources": {"crane": None, "steam.header": None}}


# ------------------------------------------------------------- declaration


def test_orchestration_declaration_contracts():
    pool = ResourcePool(("crane",))
    startup = Orchestration("line.startup", pool)
    startup.add("reel.change", noop, resources=("crane",))
    with pytest.raises(DuplicateStepError):
        startup.add("reel.change", noop)
    with pytest.raises(UnknownStepError):
        startup.add("reel.retract", noop, after=("reel.missing",))
    with pytest.raises(UnknownResourceError):
        startup.add("reel.retract", noop, resources=("steam.header",))
    step = startup.add("reel.retract", noop, after=("reel.change",), resources=("crane",))
    assert step.after == ("reel.change",)
    assert startup.steps()[-1].name == "reel.retract"
    assert startup.state()["steps"][1] == {
        "step": "reel.retract",
        "after": ["reel.change"],
        "resources": ["crane"],
    }


def test_orchestration_cannot_declare_a_cycle():
    startup = Orchestration("line.startup", ResourcePool(("crane",)))
    startup.add("first", noop)
    startup.add("second", noop, after=("first",))
    with pytest.raises(UnknownStepError):
        startup.add("third", noop, after=("fourth",))


def test_empty_orchestration_refuses_to_start():
    startup = Orchestration("line.startup", ResourcePool(("crane",)))
    with pytest.raises(ValueError):
        startup.start()


# --------------------------------------------------------------------- run


def test_independent_branches_run_together():
    startup = Orchestration("line.startup", ResourcePool(("crane",)))
    barrier = threading.Barrier(2)

    def branch():
        barrier.wait(2.0)  # breaks unless both branches are inside at once

    startup.add("steam.admit", branch)
    startup.add("glue.prepare", branch)
    report = startup.run(5.0)
    assert report.ok is True
    assert report.steps == {"steam.admit": DONE, "glue.prepare": DONE}


def test_dependencies_order_the_branches():
    startup = Orchestration("dry.procedure", ResourcePool(("crane",)))
    order = []
    lock = threading.Lock()

    def record(name):
        with lock:
            order.append(name)

    startup.add("dry.drain", lambda: record("drain"))
    startup.add("dry.steam", lambda: record("steam"), after=("dry.drain",))
    startup.add("dry.warm", lambda: record("warm"), after=("dry.steam",))
    startup.add("glue.prepare", lambda: record("glue"))
    report = startup.run(5.0)
    assert report.ok is True
    assert order.index("drain") < order.index("steam") < order.index("warm")


def test_shared_resource_serializes_the_branches_that_want_it():
    pool = ResourcePool(("crane",))
    moves = Orchestration("crane.moves", pool)
    intervals = []
    lock = threading.Lock()

    def move(name):
        started = time.monotonic()
        time.sleep(0.05)
        ended = time.monotonic()
        with lock:
            intervals.append((name, started, ended))

    moves.add("reel.change", lambda: move("reel"), resources=("crane",))
    moves.add("broke.clear", lambda: move("broke"), resources=("crane",))
    report = moves.run(5.0)
    assert report.ok is True
    assert len(intervals) == 2
    first, second = sorted(intervals, key=lambda interval: interval[1])
    assert first[2] <= second[1], "two holders overlapped on the same resource"
    assert pool.free() == ("crane",)


def test_steps_with_several_resources_do_not_trap_each_other():
    pool = ResourcePool(("r1", "r2", "r3"))
    moves = Orchestration("moves", pool)
    moves.add("a", lambda: time.sleep(0.02), resources=("r1", "r2"))
    moves.add("b", lambda: time.sleep(0.02), resources=("r2", "r3"))
    moves.add("c", lambda: time.sleep(0.02), resources=("r1", "r3"))
    report = moves.run(5.0)
    assert report.ok is True
    assert pool.free() == ("r1", "r2", "r3")


def test_failed_branch_releases_skips_dependents_and_lets_the_rest_finish():
    pool = ResourcePool(("steam.header", "crane"))
    startup = Orchestration("line.startup", pool)

    def admit():
        raise RuntimeError("valve stuck")

    startup.add("steam.admit", admit, resources=("steam.header",))
    startup.add("dry.warm", noop, after=("steam.admit",))
    startup.add("dry.run", noop, after=("dry.warm",))
    startup.add("glue.prepare", lambda: time.sleep(0.02), resources=("crane",))
    report = startup.run(5.0)
    assert report.complete is True
    assert report.failed is True
    assert report.ok is False
    assert report.steps["steam.admit"] == FAILED
    assert report.steps["dry.warm"] == SKIPPED
    assert report.steps["dry.run"] == SKIPPED
    assert report.steps["glue.prepare"] == DONE
    assert "valve stuck" in report.errors["steam.admit"]
    assert pool.free() == ("steam.header", "crane")
    pool.try_acquire("check", ("steam.header",))  # the header came cleanly back


def test_waiting_branch_gets_the_resource_back_when_the_holder_fails():
    pool = ResourcePool(("steam.header",))
    startup = Orchestration("line.startup", pool)
    holding = threading.Event()
    moments = {}

    def admit():
        holding.set()
        time.sleep(0.1)  # keep the header while the contender lines up
        moments["admit raised"] = time.monotonic()
        raise RuntimeError("valve stuck")

    def warm():
        moments["warm started"] = time.monotonic()

    startup.add("steam.admit", admit, resources=("steam.header",))
    startup.add("trigger", lambda: holding.wait(1.0) or None)
    startup.add("dry.warm", warm, after=("trigger",), resources=("steam.header",))
    report = startup.run(5.0)
    assert report.steps["steam.admit"] == FAILED
    assert report.steps["dry.warm"] == DONE
    assert moments["warm started"] >= moments["admit raised"]
    assert pool.holder("steam.header") is None


def test_cancel_stops_pending_steps_and_returns_every_resource():
    pool = ResourcePool(("crane",))
    shutdown = Orchestration("line.shutdown", pool)
    started = threading.Event()
    release_me = threading.Event()

    def long_move():
        started.set()
        assert release_me.wait(2.0)

    shutdown.add("reel.retract", long_move, resources=("crane",))
    shutdown.add("trigger", lambda: started.wait(1.0) or None)
    shutdown.add("broke.clear", noop, after=("trigger",), resources=("crane",))
    shutdown.add("reel.bite", noop, after=("reel.retract",))
    handle = shutdown.start()
    assert started.wait(1.0) is True
    handle.cancel()
    release_me.set()
    report = handle.wait(5.0)
    assert report.cancelled is True
    assert report.complete is True
    assert report.steps["reel.retract"] == DONE  # a running step finishes
    assert report.steps["broke.clear"] == CANCELLED
    assert report.steps["reel.bite"] == CANCELLED
    assert pool.free() == ("crane",)


def test_wait_can_time_out_and_the_run_settles_later():
    shutdown = Orchestration("line.shutdown", ResourcePool(("crane",)))
    gate = threading.Event()
    shutdown.add("reel.retract", lambda: gate.wait(2.0) or None)
    handle = shutdown.start()
    early = handle.wait(0.05)
    assert early.complete is False
    assert early.steps["reel.retract"] == "running"
    gate.set()
    report = handle.wait(2.0)
    assert report.ok is True
    assert handle.done() is True
    assert report.state()["steps"] == {"reel.retract": DONE}
