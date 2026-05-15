"""
Regression tests for orchestrator cooldown scope separation.
"""

from __future__ import annotations

import os
import sys
import unittest

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

    def test_transcript_can_still_use_channel_scope(self) -> None:
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

        self.assertIn("channel:HISTORY", failure_scopes)
        self.assertIn("channel:HISTORY", report_scopes)
        self.assertIn("youtube:content", failure_scopes)
        self.assertIn("youtube:content", report_scopes)

    def test_geo_blocked_is_classified_without_cooldown(self) -> None:
        from orchestrator.error_analyzer import classify_error

        classification = classify_error(
            "ERROR: The uploader has not made this video available in your country"
        )

        self.assertEqual(classification.error_type, "youtube_geo_blocked")
        self.assertEqual(classification.cooldown_seconds, 0)
        self.assertEqual(classification.suggested_scope, "youtube:content")


if __name__ == "__main__":
    unittest.main()
