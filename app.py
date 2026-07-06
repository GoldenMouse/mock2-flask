"""Databricks App — Flask CRUD over spark_catalog.marketarm.tblentry.

Runs as a Databricks App. Auth to the SQL warehouse is handled automatically
via the app's service principal (the Databricks SDK Config picks up the
injected OAuth credentials); we only need the warehouse HTTP path.
"""

import os
from datetime import datetime

from databricks import sql
from databricks.sdk.core import Config
from flask import Flask, redirect, render_template, request, url_for

app = Flask(__name__)

TABLE = "spark_catalog.marketarm.tblentry"

# Editable columns in display order. entryID is the key and is managed by the
# app, so it is not in this list.
FIELDS = [
    "category",
    "nameEntry",
    "citizen",
    "alias",
    "alias2",
    "alias3",
    "URL",
    "PriorWCDJ",
    "LastWCDJ",
    "userID",
    "Description",
    "Active",
]

# Databricks-injected config (host + OAuth for the app service principal).
_cfg = Config()


def get_connection():
    """Open a connection to the configured SQL warehouse."""
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    return sql.connect(
        server_hostname=_cfg.host,
        http_path=http_path,
        credentials_provider=lambda: _cfg.authenticate,
    )


def query(statement, params=None, fetch=True):
    """Run a statement, optionally returning rows as list[dict]."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(statement, params or {})
            if not fetch:
                return None
            columns = [c[0] for c in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def list_entries():
    return query(f"SELECT * FROM {TABLE} ORDER BY entryID DESC")


def get_entry(entry_id):
    rows = query(
        f"SELECT * FROM {TABLE} WHERE entryID = %(id)s", {"id": entry_id}
    )
    return rows[0] if rows else None


def _coerce(form):
    """Build a column->value dict from submitted form data."""
    values = {}
    for field in FIELDS:
        raw = form.get(field, "").strip()
        if field == "Active":
            values[field] = form.get("Active") == "on"
        elif field == "LastWCDJ":
            values[field] = raw or None  # timestamp string or NULL
        else:
            values[field] = raw or None
    return values


def create_entry(form):
    values = _coerce(form)
    # No identity column in the DDL, so allocate the next id ourselves.
    next_id_rows = query(f"SELECT COALESCE(MAX(entryID), 0) + 1 AS nid FROM {TABLE}")
    values["entryID"] = next_id_rows[0]["nid"]

    cols = ["entryID"] + FIELDS
    placeholders = ", ".join(f"%({c})s" for c in cols)
    query(
        f"INSERT INTO {TABLE} ({', '.join(cols)}) VALUES ({placeholders})",
        values,
        fetch=False,
    )


def update_entry(entry_id, form):
    values = _coerce(form)
    values["entryID"] = entry_id
    assignments = ", ".join(f"{c} = %({c})s" for c in FIELDS)
    query(
        f"UPDATE {TABLE} SET {assignments} WHERE entryID = %(entryID)s",
        values,
        fetch=False,
    )


def delete_entry(entry_id):
    query(f"DELETE FROM {TABLE} WHERE entryID = %(id)s", {"id": entry_id}, fetch=False)


@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        action = request.form.get("action")
        if action == "create":
            create_entry(request.form)
        elif action == "update":
            update_entry(int(request.form["entryID"]), request.form)
        elif action == "delete":
            delete_entry(int(request.form["entryID"]))
        return redirect(url_for("index"))

    # GET: optionally editing an existing row.
    edit_id = request.args.get("edit", type=int)
    editing = get_entry(edit_id) if edit_id is not None else None
    entries = list_entries()
    return render_template(
        "index.html",
        entries=entries,
        fields=FIELDS,
        editing=editing,
        now=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
    )


@app.route("/healthz")
def healthz():
    return {"status": "ok"}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), debug=True)
