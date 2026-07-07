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
import logging
import os
import re
import threading
import time
from urllib.parse import urlparse

log = logging.getLogger(__name__)


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
_warehouse_ready = False
_start_issued = False
_warehouse_lock = threading.Lock()


def _warehouse_id():
    """Extract the warehouse id from the configured HTTP path."""
    match = re.search(r"/warehouses/([^/?]+)", os.environ.get("DATABRICKS_HTTP_PATH", ""))
    return match.group(1) if match else None


def _auto_start_enabled():
    return os.environ.get("AUTO_START_WAREHOUSE", "1") == "1"


def _warehouse_client():
    from databricks.sdk import WorkspaceClient

    return WorkspaceClient()


def _read_state(client, warehouse_id):
    state = client.warehouses.get(warehouse_id).state
    return state.value if state is not None else None


def warehouse_state():
    """Fast, non-blocking lookup of the warehouse state, for the status endpoint.

    Returns 'RUNNING' whenever there is nothing to wait for (mock mode,
    auto-start disabled, or no warehouse id). Returns None if it can't be read.
    """
    if use_mock() or not _auto_start_enabled():
        return "RUNNING"
    warehouse_id = _warehouse_id()
    if not warehouse_id:
        return "RUNNING"
    if _warehouse_ready:
        return "RUNNING"
    try:
        return _read_state(_warehouse_client(), warehouse_id)
    except Exception as exc:  # noqa: BLE001
        log.warning("Could not read warehouse state: %s", exc)
        return None


def ensure_warehouse_running():
    """Ensure the SQL warehouse is starting / running — non-blocking.

    Starts the warehouse once if it's stopped and returns immediately; it does
    not wait for RUNNING (a cold classic warehouse can take ~10 minutes). The UI
    polls warehouse_state() to show a loading state until it's ready, and the
    connect-retry loop in _get_connection() rides out any brief remaining window.
    Gated by AUTO_START_WAREHOUSE ("1" default; set "0" to disable).
    """
    global _warehouse_ready, _start_issued
    if _warehouse_ready:
        return
    if use_mock() or not _auto_start_enabled():
        _warehouse_ready = True
        return
    warehouse_id = _warehouse_id()
    if not warehouse_id:
        _warehouse_ready = True
        return

    client = _warehouse_client()
    if _read_state(client, warehouse_id) == "RUNNING":
        _warehouse_ready = True
        return

    with _warehouse_lock:
        if not _start_issued:
            _start_issued = True
            log.warning(
                "SQL warehouse %s is not running — starting it "
                "(a cold start can take ~10 minutes)...", warehouse_id,
            )
            try:
                client.warehouses.start(warehouse_id)
            except Exception as exc:  # noqa: BLE001 - permission/transient
                log.warning("Could not issue warehouse start (%s).", exc)


def _server_hostname(cfg):
    """Return the bare warehouse host (no scheme/path).

    sql.connect's server_hostname must be a hostname like
    'dbc-xxxx.cloud.databricks.com'. DATABRICKS_HOST / cfg.host include the
    'https://' scheme, which would cause a DNS/name-resolution error if passed
    straight through. DATABRICKS_SERVER_HOSTNAME overrides this if set (use the
    exact 'Server hostname' from the warehouse's Connection details tab).
    """
    override = os.environ.get("DATABRICKS_SERVER_HOSTNAME")
    if override:
        return override.strip().replace("https://", "").replace("http://", "").strip("/")
    host = (cfg.host or "").strip()
    if "://" not in host:
        host = "https://" + host
    return urlparse(host).hostname


def _describe_error(exc):
    """Unwrap the exception chain so the real root cause is visible.

    The SQL connector wraps failures as 'Error during request to server'; the
    actual cause (ConnectionResetError, SSL cert failure, HTTP 403, ...) is
    down the __cause__/__context__ chain.
    """
    parts, seen, cur = [], set(), exc
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        parts.append(f"{type(cur).__name__}: {cur}")
        cur = cur.__cause__ or cur.__context__
    return "  <-  ".join(parts)


def _get_connection():
    from databricks import sql
    from databricks.sdk.core import Config

    # Best-effort on-demand start; non-blocking. The UI has already polled the
    # warehouse to RUNNING before firing data requests, so this is just a safety
    # net alongside the connect-retry loop below.
    ensure_warehouse_running()

    cfg = Config()
    server_hostname = _server_hostname(cfg)
    http_path = os.environ["DATABRICKS_HTTP_PATH"]
    log.info("Connecting to warehouse host=%s http_path=%s", server_hostname, http_path)
    attempts = int(os.environ.get("CONNECT_RETRIES", "4"))
    # Cloud Fetch downloads results directly from cloud object storage, which a
    # corporate VPN/proxy/firewall often resets ("connection aborted"). Set
    # USE_CLOUD_FETCH=0 to stream results inline through the warehouse instead.
    use_cloud_fetch = os.environ.get("USE_CLOUD_FETCH", "1") == "1"

    connect_kwargs = dict(
        server_hostname=server_hostname,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
        use_cloud_fetch=use_cloud_fetch,
    )
    # TLS escape hatches for corporate networks that intercept HTTPS. Point
    # DATABRICKS_TLS_CA_FILE at your corporate CA bundle (.pem); as a last resort
    # DATABRICKS_TLS_NO_VERIFY=1 disables verification (insecure — testing only).
    ca_file = os.environ.get("DATABRICKS_TLS_CA_FILE")
    if ca_file:
        connect_kwargs["_tls_trusted_ca_file"] = ca_file
    if os.environ.get("DATABRICKS_TLS_NO_VERIFY") == "1":
        connect_kwargs["_tls_no_verify"] = True

    last_error = None
    for attempt in range(attempts):
        try:
            return sql.connect(**connect_kwargs)
        except Exception as exc:  # noqa: BLE001 - retry transient cold-start resets
            last_error = exc
            wait = 2 * (2 ** attempt)  # 2s, 4s, 8s, ...
            log.warning(
                "Warehouse connect attempt %d/%d failed; retrying in %ss. Cause: %s",
                attempt + 1, attempts, wait, _describe_error(exc),
            )
            if attempt < attempts - 1:
                time.sleep(wait)
    log.error(
        "Warehouse connection failed after %d attempts. Root cause: %s",
        attempts, _describe_error(last_error),
    )
    raise last_error


def _query(statement, params=None, fetch=True):
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(statement, params or {})
            if not fetch:
                return None
            # Convert the Arrow result to row dicts ourselves via to_pylist().
            # This bypasses the connector's fetchall() -> _convert_arrow_table ->
            # df.to_numpy(na_value=None) path, which raises a TypeError on integer
            # columns with some pandas versions. to_pylist() maps column names to
            # native Python values (ints, None, datetime) directly.
            return cur.fetchall_arrow().to_pylist()


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
