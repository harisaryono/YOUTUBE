import os

# Fast startup for local web run: skip expensive DB startup checks.
os.environ.setdefault("WEBAPP_STARTUP_INTEGRITY", "0")
os.environ.setdefault("WEBAPP_STARTUP_CLEANUP_WAL", "0")
os.environ.setdefault("WEBAPP_STARTUP_RECONCILE_FILES", "0")

from webapp.app import app


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
