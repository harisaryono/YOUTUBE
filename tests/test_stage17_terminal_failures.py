from orchestrator.error_analyzer import classify_error, _cooldown_scopes_for_row
from orchestrator.terminal_failures import is_terminal_failure, terminal_failure_policy


def test_geo_blocked_is_terminal_no_global_cooldown():
    c = classify_error("The uploader has not made this video available in your country", exit_code=1)
    assert c.error_type == "youtube_geo_blocked"
    assert c.cooldown_seconds == 0
    assert is_terminal_failure(c.error_type)
    assert terminal_failure_policy(c.error_type)["retry_strategy"] == "matching_region_proxy"
    assert _cooldown_scopes_for_row("transcript", "channel:UC123", c) == []


def test_no_subtitle_routes_to_asr():
    c = classify_error("No transcripts were found")
    assert c.error_type == "no_subtitle"
    policy = terminal_failure_policy(c.error_type)
    assert policy["route_to_asr"] is True
    assert policy["target_stage"] == "audio_download"
    assert _cooldown_scopes_for_row("transcript", "channel:UC123", c) == []


def test_member_only_no_normal_retry():
    c = classify_error("This video is available to this channel's members")
    assert c.error_type == "member_only"
    policy = terminal_failure_policy(c.error_type)
    assert policy["retryable"] is False
    assert policy["normal_retry"] is False
    assert _cooldown_scopes_for_row("transcript", "channel:UC123", c) == []


def test_private_and_age_restricted_terminal():
    private = classify_error("Private video", exit_code=1)
    age = classify_error("Confirm your age to watch this video", exit_code=1)
    assert private.error_type == "private_video"
    assert age.error_type == "age_restricted"
    assert is_terminal_failure(private.error_type)
    assert is_terminal_failure(age.error_type)
    assert terminal_failure_policy(age.error_type)["retry_strategy"] == "valid_cookies_required"


def test_bot_detection_is_not_terminal_and_keeps_cooldown():
    c = classify_error("Sign in to confirm you're not a bot", exit_code=1)
    assert c.error_type == "youtube_bot_detection"
    assert c.cooldown_seconds > 0
    assert not is_terminal_failure(c.error_type)
    assert _cooldown_scopes_for_row("transcript", "channel:UC123", c)


def test_exit_code_one_does_not_override_terminal_classification():
    c = classify_error("requested format is not available", exit_code=1)
    assert c.error_type == "format_unavailable"
    assert c.cooldown_seconds == 0
    assert is_terminal_failure(c.error_type)
