"""Exclusive resources shared between preparation branches.

A request names the whole set of resources a step needs and joins the tail of
each resource's FIFO queue. The request is granted only when the step stands
at the head of **every** queue it joined and every resource is free, so
grants are all-or-nothing: a step never holds one resource while waiting on
another, which removes hold-and-wait deadlocks and keeps two devices from
racing the same resource. Queue order is the order in which the scheduler
offered the step, so the loser of a race waits its turn rather than retrying
blindly.
"""

from threading import Condition
from typing import Callable, Iterable


class ResourcePool:
    """A set of named resources, each usable by at most one step at a time."""

    def __init__(
        self,
        resources: Iterable[str] = (),
        wake: Callable[[], None] | None = None,
    ) -> None:
        self._wake = wake or (lambda: None)
        self._cond = Condition()
        self._holder: dict[str, str] = {}
        self._queue: dict[str, list[str]] = {name: [] for name in resources}
        # The complete resource set each queued step is asking for.
        self._requests: dict[str, tuple[str, ...]] = {}

    def declare(self, name: str) -> None:
        with self._cond:
            self._queue.setdefault(name, [])

    def holder(self, resource: str) -> str | None:
        with self._cond:
            return self._holder.get(resource)

    def holders(self) -> dict[str, str]:
        with self._cond:
            return dict(self._holder)

    def waiting(self) -> dict[str, tuple[str, ...]]:
        with self._cond:
            return {
                resource: tuple(queue)
                for resource, queue in self._queue.items()
                if queue
            }

    def free(self, resource: str) -> bool:
        with self._cond:
            return resource not in self._holder

    def request(self, step: str, resources: Iterable[str]) -> None:
        """Join the tail of each queue for the whole request, as one unit."""
        names = tuple(dict.fromkeys(resources))
        with self._cond:
            if step not in self._requests:
                self._requests[step] = names
            for resource in names:
                queue = self._queue.setdefault(resource, [])
                if step not in queue:
                    queue.append(step)

    def withdraw(self, step: str) -> tuple[str, ...]:
        """Pull a cancelled step out of every queue it was still waiting on."""
        with self._cond:
            request = self._requests.pop(step, ())
            for resource in request:
                queue = self._queue.get(resource, [])
                if step in queue:
                    queue.remove(step)
        if request:
            self._wake()
        return tuple(request)

    def _grantable(self, step: str) -> bool:
        request = self._requests.get(step)
        if not request:
            return False
        for resource in request:
            queue = self._queue.get(resource, [])
            if not queue or queue[0] != step:
                return False
            if resource in self._holder:
                return False
        return True

    def grantable(self, step: str) -> bool:
        with self._cond:
            return self._grantable(step)

    def grant(self, step: str) -> tuple[str, ...]:
        """Hand the whole request over at once; nothing piecemeal."""
        with self._cond:
            if not self._grantable(step):
                return ()
            claims = self._requests.pop(step)
            for resource in claims:
                self._holder[resource] = step
                self._queue[resource].pop(0)
            return claims

    def release(self, step: str) -> tuple[str, ...]:
        """Return everything the step holds; idempotent after a failed run."""
        with self._cond:
            freed = [
                resource
                for resource, owner in self._holder.items()
                if owner == step
            ]
            for resource in freed:
                del self._holder[resource]
        if freed:
            self._wake()
        return tuple(freed)
