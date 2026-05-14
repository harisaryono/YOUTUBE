#!/usr/bin/env python3
import subprocess
import time
import sys
import os
from pathlib import Path

from local_services import service_env

# Configuration - use relative paths based on script location
SCRIPT_DIR = Path(__file__).resolve().parent.parent
ROOT = SCRIPT_DIR
VENV_DIR = Path(
    os.environ.get("YOUTUBE_VENV_DIR")
    or os.environ.get("EXTERNAL_VENV_DIR")
    or "/media/harry/DATA120B/venv_youtube"
)
PYTHON_BIN = str(VENV_DIR / "bin" / "python3")
SCRIPT = str(ROOT / "fill_missing_resumes_youtube_db.py")
DB = str(ROOT / "youtube_transcripts.db")
MODEL = "openai/gpt-oss-120b"
PROVIDERS = ["nvidia", "groq", "cerebras"]
CHANNELS = [] # Empty list = all channels
LOG_DIR = ROOT / "out/agent_logs"
REPORT_DIR = ROOT / "tmp/reports"

def main():
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Starting resume generation for providers: {', '.join(PROVIDERS)}")
    print(f"Primary model: {MODEL}")
    
    processes = []
    
    worker_counts = {
        "nvidia": 7,
        "groq": 5,
        "cerebras": 1
    }

    for provider, count in worker_counts.items():
        for i in range(1, count + 1):
            agent_id = f"{provider}_{i}"
            log_file = LOG_DIR / f"resume_{agent_id}.log"
            report_csv = REPORT_DIR / f"report_{agent_id}.csv"
            
            cmd = [
                PYTHON_BIN, SCRIPT,
                "--db", DB,
                "--provider", provider,
                "--model", MODEL,
                "--report-csv", str(report_csv),
                "--limit", "100"
            ]
            for c in CHANNELS:
                cmd.extend(["--channel", c])
            
            print(f"Launching worker {agent_id}...")
            # Set environment variables for the subprocess
            env = os.environ.copy()
            env["YT_PROVIDER_COORDINATOR_URL"] = service_env("YT_PROVIDER_COORDINATOR_URL", "http://127.0.0.1:8788")
            
            # Use setsid to detach from current session
            with open(log_file, "w") as f: # Use "w" to clear old logs
                p = subprocess.Popen(
                    cmd,
                    stdout=f,
                    stderr=subprocess.STDOUT,
                    start_new_session=True,
                    cwd=str(ROOT), # Explicitly set CWD
                    env=env       # Override env vars
                )
                processes.append((agent_id, p.pid))
            
            time.sleep(1)

    print("\nLaunched Workers:")
    for aid, pid in processes:
        print(f"  - {aid}: PID {pid}")
    
    print(f"\nLogs are available in {LOG_DIR}")
    print(f"Reports will be in {REPORT_DIR}")

if __name__ == "__main__":
    main()
