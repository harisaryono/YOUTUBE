#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import sqlite3
import sys
from pathlib import Path

from local_services import (
    coordinator_report_provider_event,
    is_provider_blocking_enabled,
    parse_provider_quota_block,
    is_transient_provider_limit_error,
    upsert_provider_model_block,
)
from provider_encryption import decrypt_api_key
from update_latest_channel_videos import ProviderAccountLease, generate_resume_markdown


def load_account(providers_db: str, account_id: int) -> ProviderAccountLease:
    acc = sqlite3.connect(providers_db, timeout=60)
    acc.row_factory = sqlite3.Row
    try:
        row = acc.execute(
            "SELECT * FROM provider_accounts WHERE id=? AND is_active=1",
            (int(account_id),),
        ).fetchone()
        if row is None:
            raise RuntimeError(f"account not found: {account_id}")

        raw_headers = str(row["extra_headers_json"] or "").strip()
        headers = json.loads(raw_headers) if raw_headers else {}
        api_key = decrypt_api_key(str(row["api_key"] or ""))
        if not api_key:
            raise RuntimeError(f"empty api key for account: {account_id}")

        return ProviderAccountLease(
            id=int(row["id"]),
            provider=str(row["provider"]),
            account_name=str(row["account_name"]),
            usage_method=str(row["usage_method"] or ""),
            api_key=api_key,
            endpoint_url=str(row["endpoint_url"] or ""),
            model_name=str(row["model_name"] or ""),
            extra_headers={str(k): str(v) for k, v in headers.items()},
            lease_token="",
        )
    finally:
        acc.close()


def main() -> int:
    if len(sys.argv) != 6:
        raise SystemExit(
            "usage: resume_account_runner.py <account_id> <tasks_csv> <report_csv> <db_path> <providers_db>"
        )

    account_id = int(sys.argv[1])
    tasks_csv, report_csv, db_path, providers_db = sys.argv[2:6]
    account = load_account(providers_db, account_id)

    db = sqlite3.connect(db_path, timeout=60)
    db.row_factory = sqlite3.Row
    rows = list(csv.DictReader(open(tasks_csv, encoding="utf-8")))
    report_path = Path(report_csv)
    report_path.parent.mkdir(parents=True, exist_ok=True)

    stopped_due_to_limit = False
    with report_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "video_id",
                "channel_name",
                "status",
                "summary_file_path",
                "provider",
                "account_id",
                "account_name",
                "note",
            ],
        )
        writer.writeheader()

        for idx, row in enumerate(rows, start=1):
            video_id = str(row["video_id"])
            title = str(row["title"])
            channel_name = str(row["channel_name"])
            transcript_path = Path(str(row["transcript_file_path"]))

            existing = db.execute(
                "SELECT COALESCE(summary_file_path,'') FROM videos WHERE video_id=?",
                (video_id,),
            ).fetchone()
            if existing and str(existing[0] or "").strip():
                writer.writerow(
                    {
                        "video_id": video_id,
                        "channel_name": channel_name,
                        "status": "skip_existing_summary",
                        "summary_file_path": str(existing[0]),
                        "provider": "",
                        "account_id": "",
                        "account_name": "",
                        "note": "",
                    }
                )
                f.flush()
                continue

            if not transcript_path.exists():
                writer.writerow(
                    {
                        "video_id": video_id,
                        "channel_name": channel_name,
                        "status": "missing_transcript",
                        "summary_file_path": "",
                        "provider": "",
                        "account_id": "",
                        "account_name": "",
                        "note": str(transcript_path),
                    }
                )
                f.flush()
                continue

            print(f"[{idx}/{len(rows)}] resume {channel_name} {video_id}", flush=True)
            try:
                transcript = transcript_path.read_text(encoding="utf-8")
                result = generate_resume_markdown(account, title=title, transcript=transcript)
                if not str(result or "").strip():
                    raise RuntimeError("empty_result")

                out_path = transcript_path.parent.parent / "resume" / f"{video_id}_summary.md"
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_text(result, encoding="utf-8")
                db.execute(
                    "UPDATE videos SET summary_file_path=? WHERE video_id=?",
                    (str(out_path), video_id),
                )
                db.commit()
                writer.writerow(
                    {
                        "video_id": video_id,
                        "channel_name": channel_name,
                        "status": "ok",
                        "summary_file_path": str(out_path),
                        "provider": account.provider,
                        "account_id": account.id,
                        "account_name": account.account_name,
                        "note": "",
                    }
                )
                print(f"   ok via {account.provider}:{account.account_name}", flush=True)
            except Exception as exc:
                note = str(exc)
                writer.writerow(
                    {
                        "video_id": video_id,
                        "channel_name": channel_name,
                        "status": "error",
                        "summary_file_path": "",
                        "provider": account.provider,
                        "account_id": account.id,
                        "account_name": account.account_name,
                        "note": note,
                    }
                )
                print(f"   error via {account.provider}:{account.account_name}: {note}", flush=True)

                stop_account = False
                if is_provider_blocking_enabled(account.provider):
                    try:
                        report_resp = coordinator_report_provider_event(
                            provider_account_id=account.id,
                            provider=account.provider,
                            model_name=account.model_name,
                            reason=note,
                            source="resume_account_runner",
                            payload={"video_id": video_id, "title": title},
                        )
                        decision = report_resp.get("decision") or {}
                        action = str(decision.get("action") or "").strip()
                        if action in {"blocked", "disabled"}:
                            stop_account = True
                    except Exception as report_exc:
                        print(
                            f"   report-event gagal untuk {account.provider}:{account.account_name}: {report_exc}",
                            flush=True,
                        )
                        block = parse_provider_quota_block(account.provider, note)
                        if block:
                            upsert_provider_model_block(
                                account.id,
                                account.provider,
                                account.model_name,
                                str(block.get("blocked_until") or "").strip(),
                                limit_value=int(block.get("limit") or 0),
                                used_value=int(block.get("used") or 0),
                                requested_value=int(block.get("requested") or 0),
                                reason=note,
                                source="resume_account_runner_local_fallback",
                            )
                            print(
                                f"   local block saved for {account.provider}:{account.account_name}",
                                flush=True,
                            )

                    if is_transient_provider_limit_error(note, provider=account.provider):
                        stop_account = True

                if stop_account:
                    stopped_due_to_limit = True
                    print(
                        f"   stop worker {account.provider}:{account.account_name} karena quota/rate-limit; "
                        "sisa task dibiarkan belum diproses",
                        flush=True,
                    )
                    f.flush()
                    break

            f.flush()

    db.close()
    return 10 if stopped_due_to_limit else 0


if __name__ == "__main__":
    raise SystemExit(main())
