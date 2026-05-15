# Stage 11 Safe Actions

Objective:
- keep control actions safe, auditable, and reversible.

Commands:
- `./scripts/orchestrator.sh pause-stage <stage> --minutes N --reason ...`
- `./scripts/orchestrator.sh resume-stage <stage>`
- `./scripts/orchestrator.sh pause-group <group> --minutes N --reason ...`
- `./scripts/orchestrator.sh resume-group <group>`
- `./scripts/orchestrator.sh retry-failed --stage <stage> --limit N --dry-run`
- `./scripts/orchestrator.sh quarantine-channel <CHANNEL_ID> --reason ...`
- `./scripts/orchestrator.sh unquarantine-channel <CHANNEL_ID>`

Rules:
- pause/resume must be stored in state and visible to `doctor`
- quarantine must block affected channel jobs and appear in dashboard snapshots
- retry-failed defaults to dry-run
- all actions must emit audit events
- dashboard should read from orchestrator actions/policies, not implement its own action logic

Files:
- `orchestrator/actions.py`
- `orchestrator/policies.py`
- `orchestrator/state.py`
- `orchestrator/doctor.py`
- `flask_app/app.py`
- `flask_app/templates/admin_orchestrator.html`
