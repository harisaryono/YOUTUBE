# Stage 12 Policy Requeue

Stage 12 hardens the control plane from "safe actions" into "safe requeue".

## Goal

- Keep retry requests persistent.
- Prevent retrying blocked jobs.
- Let the daemon consume only eligible retry queue items.

## Core pieces

- `orchestrator/state.py`
  - retry queue table and state helpers.
- `orchestrator/actions.py`
  - `retry-failed` dry-run and enqueue mode.
- `orchestrator/planner.py`
  - convert pending retry queue rows into jobs.
- `orchestrator/dispatcher.py`
  - refuse launch when policy blockers exist even at dispatch time.
- `orchestrator/daemon.py`
  - mark retry queue rows running/completed/failed as jobs progress.

## Invariants

- `retry-failed --dry-run` must not mutate queue state.
- `retry-failed --no-dry-run` must only enqueue eligible rows.
- Pause/quarantine/policy blockers must prevent launch.
- Retry queue items must not be duplicated while pending.

## Operator View

- `doctor` should show retry queue counts.
- `explain` should mention retry queue summary.
- Dashboard should remain read-only for the queue, with retry dry-run available.
