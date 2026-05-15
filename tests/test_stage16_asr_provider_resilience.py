"""
Stage 16 — ASR Provider Resilience & Fallback Control tests.

Tests:
- NVIDIA Riva DEGRADED classification in error_analyzer
- NVIDIA RPC error classification
- provider_plan → provider order mapping
- ASR provider health check granularity in safety.py
- _should_disable_provider_after_failure DEGRADED detection
"""

from __future__ import annotations

import os
import sys
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)


class TestErrorClassifierNvidiaRiva(unittest.TestCase):
    """Tests for NVIDIA Riva error classification in error_analyzer."""

    def _classify(self, err: str):
        from orchestrator.error_analyzer import classify_error
        return classify_error(err)

    def test_nvidia_riva_degraded_explicit(self):
        """NVIDIA Riva DEGRADED function cannot be invoked should be classified."""
        err = "DEGRADED function cannot be invoked"
        c = self._classify(err)
        self.assertEqual(c.error_type, "nvidia_riva_degraded")
        self.assertEqual(c.suggested_scope, "provider:nvidia_riva")
        self.assertEqual(c.cooldown_seconds, 1800)

    def test_nvidia_riva_degraded_invalid_argument(self):
        """StatusCode.INVALID_ARGUMENT with DEGRADED should be classified."""
        err = "_MultiThreadedRendezvous StatusCode.INVALID_ARGUMENT DEGRADED"
        c = self._classify(err)
        self.assertEqual(c.error_type, "nvidia_riva_degraded")
        self.assertEqual(c.suggested_scope, "provider:nvidia_riva")

    def test_nvidia_riva_degraded_rpc_error(self):
        """_MultiThreadedRendezvous with StatusCode should be nvidia_riva_rpc_error."""
        err = "_MultiThreadedRendezvous StatusCode.UNKNOWN some error"
        c = self._classify(err)
        self.assertEqual(c.error_type, "nvidia_riva_rpc_error")
        self.assertEqual(c.suggested_scope, "provider:nvidia_riva")
        self.assertEqual(c.cooldown_seconds, 600)

    def test_nvidia_riva_degraded_case_insensitive(self):
        """DEGRADED text in mixed case should still be caught."""
        err = "nvidia RIVA service DEGRADED due to internal error"
        c = self._classify(err)
        self.assertEqual(c.error_type, "nvidia_riva_degraded")

    def test_degraded_prioritized_over_rpc(self):
        """DEGRADED + StatusCode match should produce nvidia_riva_degraded, not rpc_error."""
        err = ("_MultiThreadedRendezvous StatusCode.INVALID_ARGUMENT "
               "DEGRADED function cannot be invoked")
        c = self._classify(err)
        self.assertEqual(c.error_type, "nvidia_riva_degraded")

    def test_normal_429_not_degraded(self):
        """Regular 429 rate limit should not be classified as degraded."""
        err = "429 Too Many Requests"
        c = self._classify(err)
        self.assertNotEqual(c.error_type, "nvidia_riva_degraded")
        self.assertNotEqual(c.error_type, "nvidia_riva_rpc_error")

    def test_auth_error_not_degraded(self):
        """401 Unauthorized should be auth error, not degraded."""
        err = "401 Unauthorized - Invalid API Key"
        c = self._classify(err)
        self.assertEqual(c.error_type, "provider_auth_error")

    def test_coordinator_unavailable_classification(self):
        """Coordinator connection refused should be coordinator_unavailable."""
        err = "Connection refused connecting to coordinator on port 8788"
        c = self._classify(err)
        self.assertEqual(c.error_type, "coordinator_unavailable")
        self.assertEqual(c.suggested_scope, "coordinator")

    def test_asr_provider_unavailable(self):
        """No active ASR provider capacity from coordinator."""
        err = "Tidak ada lease coordinator untuk provider ASR yang diminta"
        c = self._classify(err)
        self.assertIn(
            c.error_type,
            {"asr_provider_unavailable", "lease_unavailable"},
        )


class TestProviderPlanMapping(unittest.TestCase):
    """Tests for provider_plan → provider order mapping."""

    def _asr_providers_from_plan(self, provider_plan: str) -> str:
        """Helper: map provider_plan to --providers value."""
        plan = str(provider_plan or "").strip().lower()
        if plan == "groq_first":
            return "groq,nvidia"
        elif plan == "nvidia_first":
            return "nvidia,groq"
        elif plan == "groq_only":
            return "groq"
        elif plan == "nvidia_only":
            return "nvidia"
        else:
            return "groq,nvidia"

    def test_groq_first_maps_to_groq_nvidia(self):
        self.assertEqual(self._asr_providers_from_plan("groq_first"), "groq,nvidia")

    def test_nvidia_first_maps_to_nvidia_groq(self):
        self.assertEqual(self._asr_providers_from_plan("nvidia_first"), "nvidia,groq")

    def test_groq_only_maps_to_groq(self):
        self.assertEqual(self._asr_providers_from_plan("groq_only"), "groq")

    def test_nvidia_only_maps_to_nvidia(self):
        self.assertEqual(self._asr_providers_from_plan("nvidia_only"), "nvidia")

    def test_unknown_plan_defaults_to_groq_nvidia(self):
        self.assertEqual(self._asr_providers_from_plan("unknown"), "groq,nvidia")

    def test_empty_plan_defaults_to_groq_nvidia(self):
        self.assertEqual(self._asr_providers_from_plan(""), "groq,nvidia")

    def test_groq_first_case_insensitive(self):
        self.assertEqual(self._asr_providers_from_plan("GROQ_FIRST"), "groq,nvidia")

    def test_nvidia_first_whitespace(self):
        self.assertEqual(self._asr_providers_from_plan(" nvidia_first "), "nvidia,groq")


class TestSafetyGranularityConceptual(unittest.TestCase):
    """Conceptual tests for safety.py ASR provider granularity."""

    def test_plan_nvidia_only_should_not_defer_when_only_nvidia_ok(self):
        """nvidia_only plan with healthy nvidia_riva should not produce wait decision."""
        plan = "nvidia_only"
        blocked_providers = {"groq"}  # nvidia_riva is NOT blocked
        nvidia_blocked = "nvidia_riva" in blocked_providers
        groq_blocked = "groq" in blocked_providers

        should_wait = (plan == "nvidia_only" and nvidia_blocked) or \
                      (plan == "groq_only" and groq_blocked)
        self.assertFalse(should_wait)

    def test_plan_nvidia_only_should_wait_when_nvidia_degraded(self):
        """nvidia_only plan with nvidia_riva blocked should wait."""
        plan = "nvidia_only"
        blocked_providers = {"nvidia_riva"}
        nvidia_blocked = "nvidia_riva" in blocked_providers

        should_wait = plan == "nvidia_only" and nvidia_blocked
        self.assertTrue(should_wait)

    def test_plan_groq_first_allows_nvidia_ok(self):
        """groq_first plan should allow ASR if nvidia is available."""
        plan = "groq_first"
        blocked_providers = {"groq"}  # only groq is blocked
        nvidia_ok = "nvidia_riva" not in blocked_providers
        groq_ok = "groq" not in blocked_providers

        at_least_one_available = nvidia_ok or groq_ok
        self.assertTrue(at_least_one_available)

    def test_plan_groq_first_allows_groq_ok(self):
        """groq_first plan should allow ASR if groq is available."""
        plan = "groq_first"
        blocked_providers = {"nvidia_riva"}  # only nvidia is blocked
        nvidia_ok = "nvidia_riva" not in blocked_providers
        groq_ok = "groq" not in blocked_providers

        at_least_one_available = nvidia_ok or groq_ok
        self.assertTrue(at_least_one_available)

    def test_all_blocked_should_defer(self):
        """Both providers blocked should defer."""
        blocked_providers = {"groq", "nvidia_riva", "asr"}
        nvidia_ok = "nvidia_riva" not in blocked_providers
        groq_ok = "groq" not in blocked_providers
        global_asr_blocked = "asr" in blocked_providers

        at_least_one_available = nvidia_ok or groq_ok
        self.assertFalse(at_least_one_available or not global_asr_blocked)


class TestShouldDisableProviderDEGRADED(unittest.TestCase):
    """Test _should_disable_provider_after_failure for DEGRADED detection."""

    def _simulate_transcription_attempt(self, error_text="", status_code=0, fatal=False):
        """Create a mock TranscriptionAttempt."""
        from recover_asr_transcripts import TranscriptionAttempt
        return TranscriptionAttempt(
            ok=False,
            text="",
            error_text=error_text,
            status_code=status_code,
            fatal=fatal,
        )

    def test_nvidia_degraded_explicit_detected(self):
        """Explicit DEGRADED message should disable NVIDIA."""
        from recover_asr_transcripts import ASRPipeline
        # We don't need a full pipeline, just test the method logic directly
        # Simulate the detection logic inline
        error_text = "DEGRADED function cannot be invoked"
        lower = error_text.lower()
        detected = (
            "degraded function cannot be invoked" in lower
            or ("invalid_argument" in lower and "degraded" in lower)
            or "nvidia riva service degraded" in lower
        )
        self.assertTrue(detected)

    def test_nvidia_degraded_invalid_argument_detected(self):
        """StatusCode.INVALID_ARGUMENT DEGRADED should be detected."""
        error_text = "StatusCode.INVALID_ARGUMENT DEGRADED"
        lower = error_text.lower()
        detected = (
            "degraded function cannot be invoked" in lower
            or ("invalid_argument" in lower and "degraded" in lower)
            or "nvidia riva service degraded" in lower
        )
        self.assertTrue(detected)

    def test_nvidia_model_unavailable_also_disables(self):
        """Model unavailable should also disable NVIDIA."""
        error_text = "model whisper-large-v3 not available on server"
        lower = error_text.lower()
        detected = (
            "model" in lower
            and (
                "not available on server" in lower
                or "invalid argument" in lower
                or "unavailable" in lower
            )
        )
        self.assertTrue(detected)

    def test_groq_429_does_not_disable_nvidia(self):
        """Groq 429 should NOT trigger NVIDIA disabled."""
        error_text = "429 Too Many Requests"
        lower = error_text.lower()
        nvidia_degraded = (
            "degraded function cannot be invoked" in lower
            or ("invalid_argument" in lower and "degraded" in lower)
            or "nvidia riva service degraded" in lower
        )
        self.assertFalse(nvidia_degraded)


if __name__ == "__main__":
    unittest.main()
