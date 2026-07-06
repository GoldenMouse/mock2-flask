"""Databricks App — Entity Manager (Flask JSON API + JS frontend).

The frontend (templates/index.html) is a single-page app that talks to the
JSON endpoints below. Data access lives in `data.py`, which uses a live SQL
warehouse in production and an in-memory mock locally.

Authentication is handled by the Databricks Apps platform (workspace SSO), not
by a custom login. The signed-in user is forwarded to the app via
`X-Forwarded-*` headers; `/api/current_user` surfaces those. Running locally
with mock data, it returns a stand-in "local dev" user.
"""

import os
import threading
from functools import wraps

from flask import Flask, jsonify, render_template, request

import data

app = Flask(__name__)


def api(fn):
    """Wrap a JSON endpoint so backend errors return {'error': ...} not a 500 page."""

    @wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - surface message to the client
            app.logger.exception("API error in %s", fn.__name__)
            return jsonify({"error": str(exc)}), 500

    return wrapper


def current_user():
    """Identify the caller from Databricks Apps headers, or a local fallback."""
    email = request.headers.get("X-Forwarded-Email")
    username = (
        request.headers.get("X-Forwarded-Preferred-Username")
        or request.headers.get("X-Forwarded-User")
        or email
    )
    if username:
        name = username.split("@")[0]
        parts = name.replace(".", " ").split()
        return {
            "userID": username,
            "userName": name,
            "userFirst": parts[0].capitalize() if parts else name,
            "userLast": parts[1].capitalize() if len(parts) > 1 else "",
            "email": email or "",
            "accessLevel": "User",
        }
    # Local development (mock mode) — no platform auth headers present.
    return {
        "userID": "local.dev",
        "userName": "local.dev",
        "userFirst": "Local",
        "userLast": "Dev",
        "email": "local.dev@example.com",
        "accessLevel": "Developer",
    }


# --------------------------------------------------------------------------- #
# Pages
# --------------------------------------------------------------------------- #
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/login")
def login():
    # Databricks Apps authenticates upstream, so there is no login form here.
    # The frontend redirects here on 401; send folks back to the app.
    return render_template("index.html")


# --------------------------------------------------------------------------- #
# Auth (platform-provided identity)
# --------------------------------------------------------------------------- #
@app.route("/api/current_user")
@api
def api_current_user():
    return jsonify(current_user())


@app.route("/api/logout", methods=["POST"])
@api
def api_logout():
    # Sessions are managed by the Databricks Apps platform; nothing to clear.
    return jsonify({"success": True})


# --------------------------------------------------------------------------- #
# Warehouse readiness (so the UI can wait out a cold start)
# --------------------------------------------------------------------------- #
@app.route("/api/warehouse_status")
@api
def warehouse_status():
    state = data.warehouse_state()
    return jsonify({"state": state, "ready": state == "RUNNING"})


# --------------------------------------------------------------------------- #
# Data API
# --------------------------------------------------------------------------- #
@app.route("/get_count")
@api
def get_count():
    return jsonify({"count": data.count_entries()})


@app.route("/search")
@api
def search():
    q = request.args.get("q", "").strip()
    show_inactive = request.args.get("showInactive", "false").lower() == "true"
    try:
        limit = int(request.args.get("limit", 100))
    except (TypeError, ValueError):
        limit = 100
    limit = max(1, min(limit, 1000))
    return jsonify(data.search_entries(q, show_inactive, limit))


@app.route("/add", methods=["POST"])
@api
def add():
    payload = request.get_json(force=True, silent=True) or {}
    if not (payload.get("nameEntry") or "").strip():
        return jsonify({"error": "Name is required"}), 400
    actor = current_user()["userID"]
    values = data.coerce(payload, default_user_id=actor)
    new_id = data.create_entry(values, actor)
    return jsonify({"success": True, "entryID": new_id})


@app.route("/update", methods=["POST"])
@api
def update():
    payload = request.get_json(force=True, silent=True) or {}
    entry_id = int(payload["entryID"])
    if not (payload.get("nameEntry") or "").strip():
        return jsonify({"error": "Name is required"}), 400
    data.update_entry(entry_id, data.coerce(payload), current_user()["userID"])
    return jsonify({"success": True})


@app.route("/delete", methods=["POST"])
@api
def delete():
    payload = request.get_json(force=True, silent=True) or {}
    data.delete_entry(int(payload["entryID"]))
    return jsonify({"success": True})


@app.route("/toggle_active", methods=["POST"])
@api
def toggle_active():
    payload = request.get_json(force=True, silent=True) or {}
    new_value = data.toggle_active(int(payload["entryID"]), current_user()["userID"])
    return jsonify({"success": True, "active": new_value})


@app.route("/reload_cache", methods=["POST"])
@api
def reload_cache():
    # No server-side cache; the count is read live. Kept for frontend compat.
    return jsonify({"success": True, "count": data.count_entries()})


@app.route("/healthz")
def healthz():
    return {"status": "ok", "mock": data.use_mock()}


def _start_warehouse_warmup():
    """Kick off the (possibly ~10 min) warehouse start in the background at boot,
    so the cold start overlaps app startup instead of a user's first request."""

    def _warm():
        try:
            data.ensure_warehouse_running(block=True)
        except Exception:  # noqa: BLE001
            app.logger.exception("Warehouse warm-up failed")

    threading.Thread(target=_warm, name="warehouse-warmup", daemon=True).start()


# Runs once per worker process on import (covers both gunicorn and local dev).
if not data.use_mock() and os.environ.get("AUTO_START_WAREHOUSE", "1") == "1":
    _start_warehouse_warmup()


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), debug=True)
