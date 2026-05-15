"""
Regression tests for transcript geo/region block classification.
"""

from __future__ import annotations

import os
import sys
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)


class TestTranscriptGeoBlockClassification(unittest.TestCase):
    def setUp(self) -> None:
        from recover_transcripts import TranscriptRecoverer

        self.recoverer = TranscriptRecoverer.__new__(TranscriptRecoverer)

    def test_country_restricted_message_is_geo_blocked(self) -> None:
        text = "ERROR: The uploader has not made this video available in your country"

        self.assertTrue(self.recoverer._looks_like_geo_region_block_error(text))
        self.assertEqual(self.recoverer._transcript_failure_kind(text), "geo_blocked")
        self.assertFalse(self.recoverer._looks_like_retry_later_error(text))

    def test_region_locked_message_is_geo_blocked(self) -> None:
        text = "This video is only available in Argentina, Brazil, and Chile"

        self.assertTrue(self.recoverer._looks_like_geo_region_block_error(text))
        self.assertEqual(self.recoverer._transcript_failure_kind(text), "geo_blocked")

    def test_retry_later_message_stays_retry_later(self) -> None:
        text = "429 Too Many Requests - rate limit"

        self.assertFalse(self.recoverer._looks_like_geo_region_block_error(text))
        self.assertEqual(self.recoverer._transcript_failure_kind(text), "retry_later")


if __name__ == "__main__":
    unittest.main()
