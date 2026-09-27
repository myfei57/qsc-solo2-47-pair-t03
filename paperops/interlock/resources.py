"""Exclusive named resources shared between the branches of a procedure."""

import threading
from typing import Any, Callable, Iterable, Sequence

from ..errors import ResourceBusyError, ResourceHolderError, UnknownResourceError

POLL_INTERVAL = 0.05


class ResourcePool:
    """Named resources with a single holder at a time.

    Taking resources is all or nothing: a holder either keeps every resource it
    asked for or waits keeping none, so two holders cannot trap each other with
    each keeping half of what the other needs.
    """

    def __init__(self, resources: Sequence[str]) -> None:
        if not resources:
            raise ValueError("a pool needs at least one resource")
        if len(set(resources)) != len(resources):
            raise ValueError("a pool cannot declare the same resource twice")
        self._holders: dict[str, str | None] = {name: None for name in resources}
        self._condition = threading.Condition()

    def resources(self) -> tuple[str, ...]:
        return tuple(self._holders)

    def declares(self, resource: str) -> bool:
        return resource in self._holders

    def holder(self, resource: str) -> str | None:
        with self._condition:
            self._require_declared(resource)
            return self._holders[resource]

    def held_by(self, holder: str) -> tuple[str, ...]:
        with self._condition:
            return self._held_by(holder)

    def free(self) -> tuple[str, ...]:
        with self._condition:
            return tuple(name for name, kept in self._holders.items() if kept is None)

    def try_acquire(self, holder: str, resources: Iterable[str]) -> None:
        """Take every resource at once, or refuse and name what is busy."""
        with self._condition:
            needed = self._checked(resources)
            busy = self._busy(needed)
            if busy:
                held = ", ".join(f"{name} (held by {self._holders[name]})" for name in busy)
                raise ResourceBusyError(f"{holder} cannot acquire: {held}")
            self._keep(holder, needed)

    def acquire(
        self,
        holder: str,
        resources: Iterable[str],
        *,
        should_stop: Callable[[], bool] | None = None,
        poll_interval: float = POLL_INTERVAL,
    ) -> bool:
        """Wait until every resource is free, then take them all at once.

        Only a should_stop that asks to give up turns the wait into a False.
        """
        with self._condition:
            needed = self._checked(resources)
            while True:
                if should_stop is not None and should_stop():
                    return False
                if not self._busy(needed):
                    self._keep(holder, needed)
                    return True
                self._condition.wait(poll_interval)

    def release(self, holder: str, resource: str) -> None:
        """Hand one resource back; only the holder may."""
        with self._condition:
            self._require_declared(resource)
            kept = self._holders[resource]
            if kept != holder:
                if kept is None:
                    raise ResourceHolderError(f"{resource} is free, {holder} holds nothing")
                raise ResourceHolderError(f"{resource} is held by {kept}, not {holder}")
            self._holders[resource] = None
            self._condition.notify_all()

    def release_all(self, holder: str) -> tuple[str, ...]:
        """Hand back everything a holder keeps, the way a branch lets go."""
        with self._condition:
            released = self._held_by(holder)
            for name in released:
                self._holders[name] = None
            if released:
                self._condition.notify_all()
            return released

    def state(self) -> dict[str, Any]:
        with self._condition:
            return {"resources": dict(self._holders)}

    def _held_by(self, holder: str) -> tuple[str, ...]:
        return tuple(name for name, kept in self._holders.items() if kept == holder)

    def _require_declared(self, resource: str) -> None:
        if resource not in self._holders:
            raise UnknownResourceError(f"the pool does not declare {resource}")

    def _checked(self, resources: Iterable[str]) -> tuple[str, ...]:
        needed = tuple(dict.fromkeys(resources))
        for name in needed:
            self._require_declared(name)
        return needed

    def _busy(self, needed: tuple[str, ...]) -> tuple[str, ...]:
        return tuple(name for name in needed if self._holders[name] is not None)

    def _keep(self, holder: str, needed: tuple[str, ...]) -> None:
        for name in needed:
            self._holders[name] = holder
