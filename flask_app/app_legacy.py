#!/usr/bin/env python3
"""
Legacy compatibility shim for the old root-level app.py entrypoint.

Prefer `flask_app.app` for the active web application.
"""

from flask_app.app import app, run_server  # noqa: F401


if __name__ == "__main__":
    run_server()
