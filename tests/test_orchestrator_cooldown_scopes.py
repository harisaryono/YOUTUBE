"""
Regression tests for orchestrator cooldown scope separation.
"""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)


class TestCooldownScopeSeparation(unittest.TestCase):
    def test_resume_failure_stays_stage_scoped(self) -> None:
        from orchestrator.daemon import _cooldown_scopes_for_failure
        from orchestrator.error_analyzer import ErrorClassification, _cooldown_scopes_for_row

        classification = ErrorClassification(
            error_type="coordinator_unavailable",
            description="Coordinator unavailable",
            cooldown_seconds=300,
            severity="warning",
            recommendation="Check coordinator service",
            suggested_scope="coordinator",
        )

        failure_scopes = _cooldown_scopes_for_failure("resume", "channel:HISTORY", classification)
        report_scopes = _cooldown_scopes_for_row("resume", "channel:HISTORY", classification)

        self.assertEqual(failure_scopes, ["stage:resume"])
        self.assertEqual(report_scopes, ["stage:resume"])

    def test_asr_failure_stays_stage_scoped(self) -> None:
        from orchestrator.daemon import _cooldown_scopes_for_failure
        from orchestrator.error_analyzer import ErrorClassification, _cooldown_scopes_for_row

        classification = ErrorClassification(
            error_type="coordinator_unavailable",
            description="Coordinator unavailable",
            cooldown_seconds=300,
            severity="warning",
            recommendation="Check coordinator service",
            suggested_scope="coordinator",
        )

        failure_scopes = _cooldown_scopes_for_failure("asr", "channel:HISTORY", classification)
        report_scopes = _cooldown_scopes_for_row("asr", "channel:HISTORY", classification)

        self.assertEqual(failure_scopes, ["stage:asr"])
        self.assertEqual(report_scopes, ["stage:asr"])

    def test_transcript_pressure_is_youtube_scoped(self) -> None:
        from orchestrator.daemon import _cooldown_scopes_for_failure
        from orchestrator.error_analyzer import ErrorClassification, _cooldown_scopes_for_row

        classification = ErrorClassification(
            error_type="youtube_403",
            description="YouTube access denied",
            cooldown_seconds=3600,
            severity="blocking",
            recommendation="Check cookies/proxy/IP",
            suggested_scope="youtube",
        )

        failure_scopes = _cooldown_scopes_for_failure("transcript", "channel:HISTORY", classification)
        report_scopes = _cooldown_scopes_for_row("transcript", "channel:HISTORY", classification)

        self.assertEqual(failure_scopes, ["youtube:content"])
        self.assertEqual(report_scopes, ["youtube:content"])

    def test_geo_blocked_is_classified_without_cooldown(self) -> None:
        from orchestrator.error_analyzer import classify_error

        classification = classify_error(
            "ERROR: The uploader has not made this video available in your country"
        )

        self.assertEqual(classification.error_type, "youtube_geo_blocked")
        self.assertEqual(classification.cooldown_seconds, 0)
        self.assertTrue(classification.suggested_scope in {"video", "youtube:content", ""})

    def test_format_unavailable_is_terminal(self) -> None:
        from orchestrator.error_analyzer import classify_error

        classification = classify_error("This video format is unavailable")

        self.assertEqual(classification.error_type, "format_unavailable")
        self.assertEqual(classification.cooldown_seconds, 0)
        self.assertEqual(classification.suggested_scope, "video")

    def test_stage_cooldown_reason_is_normalized(self) -> None:
        from orchestrator.state import OrchestratorState

        with TemporaryDirectory() as tmpdir:
            state = OrchestratorState(Path(tmpdir) / "orch.sqlite3")
            state.set_cooldown(
                "stage:resume",
                "Stage cooldown: Stage cooldown: Coordinator unavailable",
                300,
            )
            cooldown = state.get_cooldown("stage:resume")
            self.assertIsNotNone(cooldown)
            self.assertEqual(cooldown["reason"], "Coordinator unavailable")

    def test_quarantine_clears_redundant_channel_cooldown(self) -> None:
        from orchestrator.actions import quarantine_channel
        from orchestrator.state import OrchestratorState

        with TemporaryDirectory() as tmpdir:
            state = OrchestratorState(Path(tmpdir) / "orch.sqlite3")
            state.set_cooldown("channel:HISTORY", "Coordinator unavailable", 300)
            result = quarantine_channel(
                state,
                "HISTORY",
                "geo/region blocks dominate transcript backlog; isolate channel",
            )

            self.assertTrue(result.ok)
            self.assertTrue(state.is_quarantined_channel("HISTORY"))
            self.assertIsNone(state.get_cooldown("channel:HISTORY"))

    def test_quarantined_history_is_excluded_from_transcript_candidates(self) -> None:
        from orchestrator import db_queries
        from orchestrator.state import OrchestratorState

        with TemporaryDirectory() as tmpdir:
            state = OrchestratorState(Path(tmpdir) / "orch.sqlite3")

            pre_rows = db_queries.find_videos_need_transcript({}, state, limit=500)
            self.assertTrue(
                any(str(row.get("channel_identifier") or "") == "HISTORY" for row in pre_rows),
                "Expected HISTORY to appear in transcript candidates before quarantine",
            )

            state.quarantine_channel("HISTORY", "geo/region blocks dominate transcript backlog; isolate channel")
            post_rows = db_queries.find_videos_need_transcript({}, state, limit=500)

            self.assertFalse(
                any(str(row.get("channel_identifier") or "") == "HISTORY" for row in post_rows),
                "Quarantined HISTORY should not appear in transcript candidates",
            )


if __name__ == "__main__":
    unittest.main()
