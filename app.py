"""Databricks App — Flask CRUD over spark_catalog.marketarm.tblentry.

Runs as a Databricks App. Data access lives in `data.py`, which transparently
uses a live SQL warehouse in production and an in-memory mock locally (see
`data.use_mock`). Auth to the warehouse is handled by the app's service
principal via the Databricks SDK; no tokens are stored in code.
"""

import os
from datetime import datetime

from flask import Flask, redirect, render_template, request, url_for

import data

app = Flask(__name__)


def _coerce(form):
    """Build a column->value dict from submitted form data."""
    values = {}
    for field in data.FIELDS:
        raw = form.get(field, "").strip()
        if field == "Active":
            values[field] = form.get("Active") == "on"
        else:
            values[field] = raw or None
    return values


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        action = request.form.get("action")
        if action == "create":
            data.create_entry(_coerce(request.form))
        elif action == "update":
            data.update_entry(int(request.form["entryID"]), _coerce(request.form))
        elif action == "delete":
            data.delete_entry(int(request.form["entryID"]))
        return redirect(url_for("index"))

    # GET: optionally editing an existing row.
    edit_id = request.args.get("edit", type=int)
    editing = data.get_entry(edit_id) if edit_id is not None else None
    return render_template(
        "index.html",
        entries=data.list_entries(),
        fields=data.FIELDS,
        editing=editing,
        mock=data.use_mock(),
        now=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    )


@app.route("/healthz")
def healthz():
    return {"status": "ok", "mock": data.use_mock()}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), debug=True)
