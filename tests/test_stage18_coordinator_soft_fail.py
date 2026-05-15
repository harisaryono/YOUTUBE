from orchestrator.safety import ProviderHealth, SafetyDecision, check_provider_health, safety_gate_for_job


class DummyState:
    def __init__(self):
        self.values = {"coordinator_last_ok": "1"}

    def get(self, key, default=""):
        return self.values.get(key, default)

    def set(self, key, value):
        self.values[key] = value

    def list_active_cooldowns(self):
        return []

    def is_cooldown_active(self, scope):
        return False

    def get_cooldown(self, scope):
        return None


class DummySystemHealth:
    disk_free_gb = 999
    mem_available_mb = 999999


class DummyYouTubeHealth:
    global_cooldown_active = False
    errors = []


def test_provider_health_soft_fails_after_previous_ok(monkeypatch):
    import local_services

    def boom(*args, **kwargs):
        raise RuntimeError("temporary coordinator status timeout")

    monkeypatch.setattr(local_services, "coordinator_status_accounts", boom)
    state = DummyState()
    health = check_provider_health({"safety": {"soft_fail_coordinator_status": True}}, state)  # type: ignore[arg-type]

    assert health.coordinator_available is True
    assert health.ok is True
    assert health.status_source == "stale_last_ok_after_status_error"
    assert health.warnings
    assert "temporary coordinator status timeout" in state.get("coordinator_last_error")


def test_provider_health_hard_fails_without_previous_ok(monkeypatch):
    import local_services

    def boom(*args, **kwargs):
        raise RuntimeError("coordinator really down")

    monkeypatch.setattr(local_services, "coordinator_status_accounts", boom)
    state = DummyState()
    state.values["coordinator_last_ok"] = "0"
    health = check_provider_health({"safety": {"soft_fail_coordinator_status": True}}, state)  # type: ignore[arg-type]

    assert health.coordinator_available is False
    assert health.ok is False
    assert health.errors


def test_provider_stage_runs_with_soft_failed_coordinator_status():
    provider_health = ProviderHealth()
    provider_health.coordinator_available = True
    provider_health.status_source = "stale_last_ok_after_status_error"
    provider_health.warnings.append("soft status failure")

    decision = safety_gate_for_job(
        {"stage": "resume", "scope": "channel:UC123"},
        {"system": {}, "resume": {"require_lease": True}},
        DummySystemHealth(),
        provider_health,
        DummyYouTubeHealth(),
        DummyState(),  # type: ignore[arg-type]
    )

    assert decision.verdict == "RUN"


def test_provider_stage_waits_when_coordinator_confirmed_unavailable():
    provider_health = ProviderHealth()
    provider_health.coordinator_available = False
    provider_health.errors.append("Coordinator unavailable")

    decision = safety_gate_for_job(
        {"stage": "resume", "scope": "channel:UC123"},
        {"system": {}, "resume": {"require_lease": True}},
        DummySystemHealth(),
        provider_health,
        DummyYouTubeHealth(),
        DummyState(),  # type: ignore[arg-type]
    )

    assert decision.verdict == "WAIT"
    assert decision.reason_code == "DEFER_PROVIDER_UNAVAILABLE"
