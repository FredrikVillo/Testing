"""Utility functions for querying the analyticsroutinedev SQL Server database."""
import pyodbc
import os as _os

# Use environment variables if set, otherwise use defaults
SERVER   = _os.getenv("DB_SERVER", "DESKTOP-R9S4CFK")
DATABASE = _os.getenv("SOURCE_DB", "analyticsroutinedev")
DRIVER   = _os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
USERNAME = _os.getenv("DB_UID", "sa")
PASSWORD = _os.getenv("DB_PWD", "(catalystone123)")

if USERNAME and PASSWORD:
    _CONN_STR = (
        f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};"
        f"UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;Encrypt=no;"
    )
else:
    _CONN_STR = (
        f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={DATABASE};"
        "Trusted_Connection=yes;TrustServerCertificate=yes;Encrypt=no;"
    )

def run_query(sql: str, max_rows: int = 50):
    """Run a read-only query and return a list of rows (as dicts)."""
    with pyodbc.connect(_CONN_STR) as conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchmany(max_rows)
        return [dict(zip(columns, row)) for row in rows]
