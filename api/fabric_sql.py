import os
import struct
import time

import pyodbc
from azure.identity import AzureCliCredential

SQL_COPT_SS_ACCESS_TOKEN = 1256
SQL_ATTR_LOGIN_TIMEOUT = 103
SQL_ATTR_CONNECTION_TIMEOUT = 113
SCOPE = "https://database.windows.net/.default"

_cred = AzureCliCredential()
_token_cache = {"token_struct": None, "exp": 0}


def _get_access_token_struct() -> bytes:
    """Return an access token packed as the SQL Server expects.

    The Azure CLI credential is used to request an access token for the
    Fabric SQL resource scope. The underlying ODBC driver expects the
    token as a little-endian UTF-16 byte string prefixed by the length
    as a 32-bit unsigned integer; this function builds that structure
    and caches it together with its expiry time. Cached tokens are
    reused until they are within 60 seconds of expiry.

    Returns:
        bytes: The packed token structure suitable for passing to
        ``pyodbc.connect(..., attrs_before={SQL_COPT_SS_ACCESS_TOKEN: ...})``.
    """
    import time as _t

    now = _t.time()
    if _token_cache["token_struct"] and now < (_token_cache["exp"] - 120):
        return _token_cache["token_struct"]

    tok = _cred.get_token(SCOPE)
    tb = tok.token.encode("utf-16-le")
    ts = struct.pack("<I", len(tb)) + tb
    _token_cache["token_struct"] = ts
    _token_cache["exp"] = tok.expires_on
    return ts


def get_fabric_connection() -> pyodbc.Connection:
    """Create and return a pyodbc connection to the Fabric SQL server.

    Environment variables ``FABRIC_SQL_SERVER`` and
    ``FABRIC_SQL_DATABASE`` must be set. The function builds an ODBC
    connection string for ``ODBC Driver 18 for SQL Server``, obtains an
    Azure AD access token via :func:`_get_access_token_struct`, and
    supplies it to the driver via the ``attrs_before`` parameter. The
    connection is retried a few times on transient failures.

    Returns:
        pyodbc.Connection: A live connection object.

    Raises:
        RuntimeError: If required environment variables are missing.
        pyodbc.Error: The last connection error if all retries fail.
    """
    server = os.getenv("FABRIC_SQL_SERVER")
    database = os.getenv("FABRIC_SQL_DATABASE")
    if not server or not database:
        raise RuntimeError("Missing FABRIC_SQL_SERVER or FABRIC_SQL_DATABASE")

    conn_str = (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server={server};"
        f"Database={database};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "MARS_Connection=no;"
        "Application Name=go-api-fabric;"
        "Connection Timeout=120;"
        "LoginTimeout=120;"
    )

    token_struct = _get_access_token_struct()
    attrs = {
        SQL_COPT_SS_ACCESS_TOKEN: token_struct,
        SQL_ATTR_LOGIN_TIMEOUT: 120,
        SQL_ATTR_CONNECTION_TIMEOUT: 120,
    }

    last = None
    for i in range(4):
        try:
            return pyodbc.connect(conn_str, attrs_before=attrs, timeout=30)
        except pyodbc.Error as e:
            last = e
            time.sleep(2 * (i + 1))
    raise last


def fetch_all(cursor, sql: str, params: tuple | None = None, limit: int = 50) -> list[dict]:
    params = params or ()
    cursor.execute(sql, params)
    cols = [c[0] for c in cursor.description]
    rows = cursor.fetchmany(limit)  # cur.fetchall() for everything, i used limit for testing purposes
    return [dict(zip(cols, row)) for row in rows]
