"""Data-access layer for the MarketArm app.

Two interchangeable backends behind the same functions:

* **Databricks** — queries the real `spark_catalog.marketarm.tblentry` Delta
  table via a SQL warehouse. Used when `DATABRICKS_HTTP_PATH` is set.
* **Mock** — an in-memory list seeded with sample rows. Used automatically for
  local development when no warehouse is configured, or when `USE_MOCK=1`.

The Flask app only calls the module-level functions (`list_entries`,
`get_entry`, `create_entry`, `update_entry`, `delete_entry`) and never needs to
know which backend is active.
"""

import os

TABLE = "spark_catalog.marketarm.tblentry"

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


def use_mock():
    """Mock mode when explicitly requested or when no warehouse is configured."""
    if os.environ.get("USE_MOCK") == "1":
        return True
    return not os.environ.get("DATABRICKS_HTTP_PATH")


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


def _db_list():
    return _query(f"SELECT * FROM {TABLE} ORDER BY entryID DESC")


def _db_get(entry_id):
    rows = _query(f"SELECT * FROM {TABLE} WHERE entryID = %(id)s", {"id": entry_id})
    return rows[0] if rows else None


def _db_create(values):
    next_id = _query(f"SELECT COALESCE(MAX(entryID), 0) + 1 AS nid FROM {TABLE}")[0]["nid"]
    row = {**values, "entryID": next_id}
    cols = ["entryID"] + FIELDS
    placeholders = ", ".join(f"%({c})s" for c in cols)
    _query(
        f"INSERT INTO {TABLE} ({', '.join(cols)}) VALUES ({placeholders})",
        row,
        fetch=False,
    )


def _db_update(entry_id, values):
    row = {**values, "entryID": entry_id}
    assignments = ", ".join(f"{c} = %({c})s" for c in FIELDS)
    _query(
        f"UPDATE {TABLE} SET {assignments} WHERE entryID = %(entryID)s",
        row,
        fetch=False,
    )


def _db_delete(entry_id):
    _query(f"DELETE FROM {TABLE} WHERE entryID = %(id)s", {"id": entry_id}, fetch=False)


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
        "LastWCDJ": "2024-06-12 09:30:00",
        "userID": "analyst01",
        "Description": "Listed for procurement network activity.",
        "Active": True,
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
        "LastWCDJ": "2024-02-01 14:15:00",
        "userID": "analyst02",
        "Description": "Front company; shipping intermediary.",
        "Active": True,
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
        "LastWCDJ": "2023-12-20 00:00:00",
        "userID": "analyst01",
        "Description": "Delisted after review.",
        "Active": False,
    },
]


def _mock_next_id():
    return (max((r["entryID"] for r in _MOCK_ROWS), default=0)) + 1


def _mock_list():
    return sorted(_MOCK_ROWS, key=lambda r: r["entryID"], reverse=True)


def _mock_get(entry_id):
    return next((r for r in _MOCK_ROWS if r["entryID"] == entry_id), None)


def _mock_create(values):
    _MOCK_ROWS.append({**values, "entryID": _mock_next_id()})


def _mock_update(entry_id, values):
    for i, r in enumerate(_MOCK_ROWS):
        if r["entryID"] == entry_id:
            _MOCK_ROWS[i] = {**values, "entryID": entry_id}
            break


def _mock_delete(entry_id):
    global _MOCK_ROWS
    _MOCK_ROWS = [r for r in _MOCK_ROWS if r["entryID"] != entry_id]


# --------------------------------------------------------------------------- #
# Public dispatch
# --------------------------------------------------------------------------- #
def list_entries():
    return _mock_list() if use_mock() else _db_list()


def get_entry(entry_id):
    return _mock_get(entry_id) if use_mock() else _db_get(entry_id)


def create_entry(values):
    return _mock_create(values) if use_mock() else _db_create(values)


def update_entry(entry_id, values):
    return _mock_update(entry_id, values) if use_mock() else _db_update(entry_id, values)


def delete_entry(entry_id):
    return _mock_delete(entry_id) if use_mock() else _db_delete(entry_id)
