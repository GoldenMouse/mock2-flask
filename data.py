"""Data-access layer for the MarketArm / Entity Manager app.

Two interchangeable backends behind the same functions:

* **Databricks** — queries the real `spark_catalog.marketarm.tblentry` Delta
  table via a SQL warehouse. Used when `DATABRICKS_HTTP_PATH` is set.
* **Mock** — an in-memory list seeded with sample rows. Used automatically for
  local development when no warehouse is configured, or when `USE_MOCK=1`.

The Flask app only calls the module-level functions and never needs to know
which backend is active. Rows are returned with `entryID` serialized as a
string so the frontend's strict `===` comparisons work regardless of backend.
"""

import datetime
import os


def _ident(part):
    """Backtick-quote a SQL identifier (catalog/schema/table) from config."""
    return "`" + part.strip().replace("`", "``") + "`"


# Catalog and schema are configurable (set them in app.yaml); the table name is
# fixed. Defaults preserve the original spark_catalog.marketarm.tblentry target.
CATALOG = os.environ.get("DATABRICKS_CATALOG", "spark_catalog")
SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "marketarm")
TABLE = f"{_ident(CATALOG)}.{_ident(SCHEMA)}.{_ident('tblentry')}"

# Editable columns in display order. entryID is the key, managed by the app.
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

# Columns a free-text search scans.
SEARCH_COLS = [
    "entryID",
    "category",
    "nameEntry",
    "citizen",
    "alias",
    "alias2",
    "alias3",
    "URL",
    "PriorWCDJ",
    "userID",
    "Description",
]


def use_mock():
    """Mock mode when explicitly requested or when no warehouse is configured."""
    if os.environ.get("USE_MOCK") == "1":
        return True
    return not os.environ.get("DATABRICKS_HTTP_PATH")


def coerce(payload, default_user_id=None):
    """Normalize an incoming JSON entry into a column->value dict."""
    values = {}
    for field in FIELDS:
        raw = payload.get(field)
        if field == "Active":
            values[field] = bool(raw)
        elif isinstance(raw, str):
            values[field] = raw.strip() or None
        else:
            values[field] = raw if raw not in ("", None) else None
    if default_user_id is not None and not values.get("userID"):
        values["userID"] = default_user_id
    return values


def _serialize(row):
    """Normalize a row for the frontend: entryID as string, temporal values as
    strings. LastWCDJ is a date (matches the date input); audit columns keep
    their time component."""
    row = dict(row)
    if row.get("entryID") is not None:
        row["entryID"] = str(row["entryID"])
    for key, value in list(row.items()):
        if isinstance(value, (datetime.date, datetime.datetime)):
            fmt = "%Y-%m-%d" if key == "LastWCDJ" else "%Y-%m-%d %H:%M:%S"
            row[key] = value.strftime(fmt)
    return row


# --------------------------------------------------------------------------- #
# Databricks backend
# --------------------------------------------------------------------------- #
def _get_connection():
    from databricks import sql
    from databricks.sdk.core import Config

    cfg = Config()
    return sql.connect(
        server_hostname=cfg.host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        credentials_provider=lambda: cfg.authenticate,
    )


def _query(statement, params=None, fetch=True):
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(statement, params or {})
            if not fetch:
                return None
            columns = [c[0] for c in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


def _db_search(q, show_inactive, limit):
    where, params = [], {}
    if not show_inactive:
        where.append("Active = true")
    if q:
        params["q"] = f"%{q}%"
        cols = [
            f"CAST({c} AS STRING) LIKE %(q)s" if c == "entryID" else f"{c} LIKE %(q)s"
            for c in SEARCH_COLS
        ]
        where.append("(" + " OR ".join(cols) + ")")
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    rows = _query(
        f"SELECT * FROM {TABLE} {clause} ORDER BY entryID DESC LIMIT {int(limit)}",
        params,
    )
    return [_serialize(r) for r in rows]


def _db_get(entry_id):
    rows = _query(f"SELECT * FROM {TABLE} WHERE entryID = %(id)s", {"id": entry_id})
    return rows[0] if rows else None


def _db_count():
    return _query(f"SELECT COUNT(*) AS c FROM {TABLE}")[0]["c"]


def _db_create(values, actor, now):
    next_id = _query(f"SELECT COALESCE(MAX(entryID), 0) + 1 AS nid FROM {TABLE}")[0]["nid"]
    row = {**values, "entryID": next_id, "createdAt": now, "updatedBy": actor, "updatedAt": now}
    cols = ["entryID"] + FIELDS + ["createdAt", "updatedBy", "updatedAt"]
    placeholders = ", ".join(f"%({c})s" for c in cols)
    _query(
        f"INSERT INTO {TABLE} ({', '.join(cols)}) VALUES ({placeholders})",
        row,
        fetch=False,
    )
    return next_id


def _db_update(entry_id, values, actor, now):
    row = {**values, "entryID": entry_id, "updatedBy": actor, "updatedAt": now}
    set_cols = FIELDS + ["updatedBy", "updatedAt"]
    assignments = ", ".join(f"{c} = %({c})s" for c in set_cols)
    _query(
        f"UPDATE {TABLE} SET {assignments} WHERE entryID = %(entryID)s",
        row,
        fetch=False,
    )


def _db_delete(entry_id):
    _query(f"DELETE FROM {TABLE} WHERE entryID = %(id)s", {"id": entry_id}, fetch=False)


def _db_toggle(entry_id, actor, now):
    current = _db_get(entry_id)
    if current is None:
        raise ValueError(f"Entry {entry_id} not found")
    new_value = not current.get("Active")
    _query(
        f"UPDATE {TABLE} SET Active = %(a)s, updatedBy = %(u)s, updatedAt = %(t)s "
        f"WHERE entryID = %(id)s",
        {"a": new_value, "u": actor, "t": now, "id": entry_id},
        fetch=False,
    )
    return new_value


# --------------------------------------------------------------------------- #
# Mock backend (in-memory, seeded)
# --------------------------------------------------------------------------- #
_MOCK_ROWS = [
    {
        "entryID": 1,
        "category": "Individual",
        "nameEntry": "Ivan Petrov",
        "citizen": "Russia",
        "alias": "I. Petrov",
        "alias2": "Vanya",
        "alias3": None,
        "URL": "https://example.gov/sanctions/1001",
        "PriorWCDJ": "2023-11-04",
        "LastWCDJ": "2024-06-12",
        "userID": "analyst01",
        "Description": "Listed for procurement network activity.",
        "Active": True,
        "createdAt": datetime.datetime(2024, 6, 12, 9, 30, 0),
        "updatedBy": "analyst01",
        "updatedAt": datetime.datetime(2024, 6, 12, 9, 30, 0),
    },
    {
        "entryID": 2,
        "category": "Entity",
        "nameEntry": "Northwind Trading LLC",
        "citizen": "UAE",
        "alias": "Northwind Ltd",
        "alias2": None,
        "alias3": None,
        "URL": "https://example.gov/sanctions/1002",
        "PriorWCDJ": None,
        "LastWCDJ": "2024-02-01",
        "userID": "analyst02",
        "Description": "Front company; shipping intermediary.",
        "Active": True,
        "createdAt": datetime.datetime(2024, 2, 1, 14, 15, 0),
        "updatedBy": "analyst02",
        "updatedAt": datetime.datetime(2024, 2, 1, 14, 15, 0),
    },
    {
        "entryID": 3,
        "category": "Vessel",
        "nameEntry": "MV Sea Falcon",
        "citizen": "Panama",
        "alias": "Sea Falcon",
        "alias2": "IMO 9345678",
        "alias3": None,
        "URL": None,
        "PriorWCDJ": "2022-08-19",
        "LastWCDJ": "2023-12-20",
        "userID": "analyst01",
        "Description": "Delisted after review.",
        "Active": False,
        "createdAt": datetime.datetime(2022, 8, 19, 8, 0, 0),
        "updatedBy": "analyst03",
        "updatedAt": datetime.datetime(2023, 12, 20, 16, 45, 0),
    },
]


def _mock_next_id():
    return max((r["entryID"] for r in _MOCK_ROWS), default=0) + 1


def _mock_get(entry_id):
    return next((r for r in _MOCK_ROWS if r["entryID"] == entry_id), None)


def _mock_search(q, show_inactive, limit):
    rows = sorted(_MOCK_ROWS, key=lambda r: r["entryID"], reverse=True)
    if not show_inactive:
        rows = [r for r in rows if r.get("Active")]
    if q:
        ql = q.lower()
        rows = [
            r
            for r in rows
            if any(
                r.get(c) is not None and ql in str(r.get(c)).lower()
                for c in SEARCH_COLS
            )
        ]
    return [_serialize(r) for r in rows[:limit]]


def _mock_count():
    return len(_MOCK_ROWS)


def _mock_create(values, actor, now):
    new_id = _mock_next_id()
    _MOCK_ROWS.append(
        {**values, "entryID": new_id, "createdAt": now, "updatedBy": actor, "updatedAt": now}
    )
    return new_id


def _mock_update(entry_id, values, actor, now):
    for i, r in enumerate(_MOCK_ROWS):
        if r["entryID"] == entry_id:
            # Merge over the existing row so createdAt (and userID) are preserved.
            _MOCK_ROWS[i] = {
                **r,
                **values,
                "entryID": entry_id,
                "updatedBy": actor,
                "updatedAt": now,
            }
            break


def _mock_delete(entry_id):
    global _MOCK_ROWS
    _MOCK_ROWS = [r for r in _MOCK_ROWS if r["entryID"] != entry_id]


def _mock_toggle(entry_id, actor, now):
    row = _mock_get(entry_id)
    if row is None:
        raise ValueError(f"Entry {entry_id} not found")
    row["Active"] = not row.get("Active")
    row["updatedBy"] = actor
    row["updatedAt"] = now
    return row["Active"]


# --------------------------------------------------------------------------- #
# Public dispatch
# --------------------------------------------------------------------------- #
def search_entries(q, show_inactive, limit):
    return _mock_search(q, show_inactive, limit) if use_mock() else _db_search(q, show_inactive, limit)


def count_entries():
    return _mock_count() if use_mock() else _db_count()


def get_entry(entry_id):
    return _mock_get(entry_id) if use_mock() else _db_get(entry_id)


def create_entry(values, actor):
    now = datetime.datetime.now()
    new_id = _mock_create(values, actor, now) if use_mock() else _db_create(values, actor, now)
    return str(new_id)


def update_entry(entry_id, values, actor):
    now = datetime.datetime.now()
    return (
        _mock_update(entry_id, values, actor, now)
        if use_mock()
        else _db_update(entry_id, values, actor, now)
    )


def delete_entry(entry_id):
    return _mock_delete(entry_id) if use_mock() else _db_delete(entry_id)


def toggle_active(entry_id, actor):
    now = datetime.datetime.now()
    return _mock_toggle(entry_id, actor, now) if use_mock() else _db_toggle(entry_id, actor, now)
