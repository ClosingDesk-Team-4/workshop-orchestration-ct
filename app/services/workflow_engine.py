"""
Workflow state machine for closing cases.

WHAT GATES A CLOSING -- AND WHAT DOES NOT:

There is no document-hash or content-integrity verification anywhere in this
service. Nothing checks that the package a partner claims to have produced is the
package the borrower actually signed. Only two gates stand between `draft` and
`closed`:

1. `can_transition()` below -- ordering only. It compares positions in
   `WorkflowState` and never inspects document content.
2. `FundingChecklist.all_cleared` (`app/services/funding_service.py`) -- operator-
   asserted booleans. `patch_checklist_items()` stores caller-supplied items
   verbatim, so "cleared" means "someone said so", not "verified".

Both are reachable without authentication: `/webhooks/partner` performs no
signature check and passes `target_state` straight into `apply_transition()`
(see `app/services/partner_webhook_service.py`). The `SIGNING_SCHEDULED` branch
below widens this further, letting any pre-signing state leapfrog to
`signing_scheduled`. Per README, that unsigned/replayable ingest path is a
deliberate training weakness, not an oversight to patch in isolation.

DO NOT REPEAT: do not build anything that treats arrival at `funding_ready` or
`closed` as evidence that document integrity was verified -- it is not. If you
add such verification, gate `apply_transition()` itself and pair it with HMAC
signature plus replay-cache enforcement at ingest; a hash check bolted onto the
unsigned webhook path is bypassed by re-posting the envelope.
"""

from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import ClosingCase, WorkflowEvent, WorkflowState
from app.models.enums import ActorType


_ORDER: list[str] = [s.value for s in WorkflowState]


def _index(state: str) -> int:
    if state not in _ORDER:
        return -1
    return _ORDER.index(state)


def can_transition(from_state: str, to_state: str) -> bool:
    if from_state == to_state:
        return False
    i, j = _index(from_state), _index(to_state)
    if i < 0 or j < 0:
        return False
    # Allow forward steps of 1, or jump to closed from funding_ready only
    if j == i + 1:
        return True
    # Scheduling a signing often leapfrogs intermediate LOS-driven milestones.
    if to_state == WorkflowState.SIGNING_SCHEDULED.value and i < _index(
        WorkflowState.SIGNING_SCHEDULED.value
    ):
        return True
    if from_state == WorkflowState.FUNDING_READY.value and to_state == WorkflowState.CLOSED.value:
        return True
    return False


async def apply_transition(
    db: AsyncSession,
    closing: ClosingCase,
    to_state: str,
    *,
    actor_type: ActorType,
    actor_id: str | None,
    payload: dict[str, Any] | None = None,
    correlation_id: str | None = None,
) -> WorkflowEvent:
    prev = closing.state
    if not can_transition(prev, to_state):
        raise ValueError(f"Invalid transition {prev} -> {to_state}")

    closing.state = to_state
    evt = WorkflowEvent(
        closing_id=closing.id,
        from_state=prev,
        to_state=to_state,
        actor_type=actor_type.value,
        actor_id=actor_id,
        payload=payload,
        correlation_id=correlation_id,
    )
    db.add(evt)
    await db.flush()
    return evt


async def get_closing_or_404(db: AsyncSession, closing_id: uuid.UUID) -> ClosingCase | None:
    result = await db.execute(select(ClosingCase).where(ClosingCase.id == closing_id))
    return result.scalar_one_or_none()
