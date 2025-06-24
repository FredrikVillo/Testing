import pyodbc
import json
import re
import random
import string
import time
from faker import Faker

# --- CONFIG ---
SOURCE_DB = "analyticsroutinedev"
DEST_DB = "analyticsroutinedev_copy"
SERVER = "DESKTOP-R9S4CFK"
UID = "sa"
PWD = "(catalystone123)"
DRIVER = "ODBC Driver 18 for SQL Server"

# --- SETUP ---
fake = Faker()

PII_KEYWORDS = [
    'name', 'email', 'address', 'phone', 'ssn', 'guid', 'account', 'user', 'password', 'secret', 'dob', 'birth', 'surname', 'firstname', 'lastname', 'city', 'zip', 'postal', 'contact', 'mobile', 'personal', 'credit', 'iban', 'bic', 'card', 'passport', 'license', 'tax', 'vat', 'pin', 'token', 'key', 'profile', 'ip', 'mac', 'device', 'location', 'lat', 'lng', 'geo', 'avatar', 'photo', 'picture', 'url', 'website', 'social', 'twitter', 'facebook', 'linkedin', 'instagram', 'slack', 'skype', 'telegram', 'whatsapp', 'wechat', 'line', 'snapchat', 'tiktok', 'discord', 'github', 'bitbucket', 'gitlab', 'bank', 'iban', 'bic', 'swift', 'accountnumber', 'routing', 'sortcode', 'iban', 'bic', 'swift', 'accountnumber', 'routing', 'sortcode', 'iban', 'bic', 'swift', 'accountnumber', 'routing', 'sortcode'
]

# --- UTILS ---
def is_pii_column(colname, datatype):
    name = colname.lower()
    # Only treat as PII if it's a string-like column
    if datatype not in ('nvarchar', 'varchar', 'text', 'nchar', 'char'):
        return False
    for kw in PII_KEYWORDS:
        if kw in name:
            return True
    return False

def get_connection(db, autocommit=False):
    return pyodbc.connect(f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={db};UID={UID};PWD={PWD};TrustServerCertificate=yes;Encrypt=no;", autocommit=autocommit)

def get_tables(cursor):
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    return [row[0] for row in cursor.fetchall()]

def get_columns(cursor, table):
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, table)
    return cursor.fetchall()

def get_primary_keys(cursor, table):
    cursor.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
        AND TABLE_NAME = ?
    """, table)
    return [row[0] for row in cursor.fetchall()]

def get_foreign_keys(cursor, table):
    cursor.execute("""
        SELECT k.COLUMN_NAME, k2.TABLE_NAME, k2.COLUMN_NAME
        FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k ON rc.CONSTRAINT_NAME = k.CONSTRAINT_NAME
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k2 ON rc.UNIQUE_CONSTRAINT_NAME = k2.CONSTRAINT_NAME
        WHERE k.TABLE_NAME = ?
    """, table)
    return cursor.fetchall()

def create_database(cursor, dbname):
    cursor.execute(f"IF DB_ID('{dbname}') IS NULL CREATE DATABASE [{dbname}]")

def create_table(dest_cursor, table, columns, pk_cols, fk_defs):
    col_defs = []
    pk_set = set(pk_cols)
    for col, typ in columns:
        t = typ.lower()
        is_pk = col in pk_set
        if t in ("nvarchar", "varchar"):
            if is_pk:
                col_defs.append(f"[{col}] {t}(64)")  # PK must be fixed length
            else:
                col_defs.append(f"[{col}] {t}(max)")
        elif t in ("nchar", "char"):
            col_defs.append(f"[{col}] {t}(32)")
        elif t in ("int", "bigint", "tinyint", "smallint"):
            col_defs.append(f"[{col}] {t}")
        elif t in ("datetime", "date", "time"):
            col_defs.append(f"[{col}] {t}")
        else:
            if is_pk:
                col_defs.append(f"[{col}] nvarchar(64)")
            else:
                col_defs.append(f"[{col}] nvarchar(max)")
    pk = f", PRIMARY KEY ({', '.join('['+c+']' for c in pk_cols)})" if pk_cols else ""
    # FKs will be added after all tables are created
    dest_cursor.execute(f"CREATE TABLE [{table}] ({', '.join(col_defs)}{pk})")

def generate_synthetic_value(col, typ):
    name = col.lower()
    if 'name' in name:
        return fake.name()
    if 'email' in name:
        return fake.email()
    if 'address' in name:
        return fake.address()
    if 'phone' in name:
        return fake.phone_number()
    if 'guid' in name or 'uuid' in name:
        return str(fake.uuid4())
    if 'password' in name or 'secret' in name:
        return fake.password(length=16)
    if 'date' in typ:
        return fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S')
    if 'user' in name:
        return fake.user_name()
    if 'account' in name:
        return fake.bban()
    if 'company' in name or 'org' in name:
        return fake.company()
    if 'city' in name:
        return fake.city()
    if 'country' in name:
        return fake.country()
    if 'zip' in name or 'postal' in name:
        return fake.postcode()
    if typ in ("int", "bigint", "tinyint", "smallint"):
        return random.randint(1, 100000)
    return fake.word()

def main():
    # Use autocommit for master connection to allow CREATE DATABASE
    with get_connection(SOURCE_DB) as src_conn, get_connection('master', autocommit=True) as master_conn:
        src_cur = src_conn.cursor()
        master_cur = master_conn.cursor()
        create_database(master_cur, DEST_DB)
    with get_connection(SOURCE_DB) as src_conn, get_connection(DEST_DB) as dest_conn:
        src_cur = src_conn.cursor()
        dest_cur = dest_conn.cursor()
        tables = get_tables(src_cur)
        table_schemas = {}
        pk_map = {}
        for table in tables:
            columns = get_columns(src_cur, table)
            pk_cols = get_primary_keys(src_cur, table)
            fk_defs = get_foreign_keys(src_cur, table)
            table_schemas[table] = (columns, pk_cols, fk_defs)
            create_table(dest_cur, table, columns, pk_cols, fk_defs)
            pk_map[table] = pk_cols
        dest_conn.commit()
        # Insert synthetic data
        for table in tables:
            columns, pk_cols, fk_defs = table_schemas[table]
            src_cur.execute(f"SELECT * FROM [{table}]")
            colnames = [desc[0] for desc in src_cur.description]
            rows = src_cur.fetchall()
            for row in rows:
                new_row = []
                for (col, typ), val in zip(columns, row):
                    if is_pii_column(col, typ):
                        new_row.append(generate_synthetic_value(col, typ))
                    else:
                        new_row.append(val)
                qmarks = ','.join('?' for _ in colnames)
                dest_cur.execute(f"INSERT INTO [{table}] ({','.join('['+c+']' for c in colnames)}) VALUES ({qmarks})", new_row)
        dest_conn.commit()
    print(f"✅ Synthetic database '{DEST_DB}' created with anonymized data.")

if __name__ == "__main__":
    main()
