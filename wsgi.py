#!/usr/bin/env python3
"""WSGI entrypoint for Passenger/Apache deployments."""

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from flask_app.app import app as application  # noqa: E402

