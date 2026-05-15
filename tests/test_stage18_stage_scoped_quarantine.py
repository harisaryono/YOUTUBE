from orchestrator.policies import policy_blockers_for_job, quarantine_stages_from_payload


class DummyState:
    def __init__(self, payload):
        self.payload = payload

    def get(self, key, default=""):
        if key.startswith("quarantine:channel:"):
            return self.payload
        return default

    def is_quarantined_channel(self, channel_id):
        return bool(self.payload)


def test_quarantine_stages_explicit_transcript_only():
    stages = quarantine_stages_from_payload({"stages": ["transcript"]})
    assert stages == {"transcript"}


def test_transcript_scoped_quarantine_blocks_only_transcript():
    state = DummyState('{"reason":"transcript blocked", "active": true, "stages": ["transcript"]}')

    transcript_blockers = policy_blockers_for_job(state, stage="transcript", scope="channel:UC123")
    resume_blockers = policy_blockers_for_job(state, stage="resume", scope="channel:UC123")
    format_blockers = policy_blockers_for_job(state, stage="format", scope="channel:UC123")

    assert transcript_blockers
    assert transcript_blockers[0]["type"] == "quarantine"
    assert resume_blockers == []
    assert format_blockers == []


def test_reason_inference_transcript_does_not_block_resume():
    state = DummyState('{"reason":"karantina history channel karena transkrip", "active": true}')

    assert policy_blockers_for_job(state, stage="transcript", scope="channel:UC123")
    assert policy_blockers_for_job(state, stage="resume", scope="channel:UC123") == []


def test_legacy_unscoped_quarantine_still_blocks_all_stages_when_no_inference():
    state = DummyState('{"reason":"manual quarantine", "active": true}')

    assert policy_blockers_for_job(state, stage="transcript", scope="channel:UC123")
    assert policy_blockers_for_job(state, stage="resume", scope="channel:UC123")


def test_audio_scoped_quarantine_blocks_audio_only():
    state = DummyState('{"reason":"audio download failed", "active": true, "stages": ["audio_download"]}')

    assert policy_blockers_for_job(state, stage="audio_download", scope="channel:UC123")
    assert policy_blockers_for_job(state, stage="transcript", scope="channel:UC123") == []
    assert policy_blockers_for_job(state, stage="resume", scope="channel:UC123") == []
