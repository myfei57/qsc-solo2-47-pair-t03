"""Declarative orchestration of start/stop preparation.

Declare a procedure as a plan of steps; steps with no ordering between them
run on parallel branches, and steps naming the same resource are serialised by
a fair mutex. A failed or cancelled branch aborts the run and every held
resource is returned cleanly before the run settles.
"""

from .orchestrator import Orchestrator, RunHandle
from .plan import Plan, Step, plan
from .resources import ResourcePool
from .status import (
    CANCELLED,
    FAILED,
    PENDING,
    READY,
    RUNNING,
    SUCCEEDED,
    WAITING,
    RunReport,
    StepContext,
    StepResult,
)

__all__ = [
    "CANCELLED",
    "FAILED",
    "PENDING",
    "READY",
    "RUNNING",
    "SUCCEEDED",
    "WAITING",
    "Orchestrator",
    "Plan",
    "ResourcePool",
    "RunHandle",
    "RunReport",
    "Step",
    "StepContext",
    "StepResult",
    "plan",
]
