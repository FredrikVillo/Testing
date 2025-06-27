import pyodbc
import random
from faker import Faker
import requests
import csv
import os
import sys
from tqdm import tqdm  # Progress bar
import hashlib
try:
    import pyffx
    FFX_KEY = "my-secret-key"  # Change to a secure key in production
    ffx_int = pyffx.Integer(FFX_KEY, length=6)  # Adjust length as needed
    ffx_str = pyffx.String(FFX_KEY, alphabet="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz", length=8)
except ImportError:
    pyffx = None
    ffx_int = None
    ffx_str = None

# --- CONFIG ---
import os as _os
SOURCE_DB = _os.getenv("SOURCE_DB", "analyticsroutinedev")
DEST_DB = _os.getenv("DEST_DB", "analyticsroutinedev_copy")
SERVER = _os.getenv("DB_SERVER", "DESKTOP-R9S4CFK")
UID = _os.getenv("DB_UID", "sa")
PWD = _os.getenv("DB_PWD", "(catalystone123)")
DRIVER = _os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")

# --- SETUP ---
fake = Faker()

PII_KEYWORDS = [
    'name', 'email', 'address', 'phone', 'ssn', 'guid', 'account', 'user', 'password', 'secret', 'dob', 'birth', 'surname', 'firstname', 'lastname', 'city', 'zip', 'postal', 'contact', 'mobile', 'personal', 'credit', 'iban', 'bic', 'card', 'passport', 'license', 'tax', 'vat', 'pin', 'token', 'key', 'profile', 'ip', 'mac', 'device', 'location', 'lat', 'lng', 'geo', 'avatar', 'photo', 'picture', 'url', 'website', 'social', 'twitter', 'facebook', 'linkedin', 'instagram', 'slack', 'skype', 'telegram', 'whatsapp', 'wechat', 'line', 'snapchat', 'tiktok', 'discord', 'github', 'bitbucket', 'gitlab', 'bank', 'iban', 'bic', 'swift', 'accountnumber', 'routing', 'sortcode', 'iban', 'bic', 'swift', 'accountnumber', 'routing', 'sortcode', 'iban', 'bic', 'swift', 'accountnumber', 'routing', 'sortcode'
]

# --- UTILS ---
def is_pii_column(colname: str, datatype: str) -> bool:
    name = colname.lower()
    # Only treat as PII if it's a string-like column
    if datatype not in ('nvarchar', 'varchar', 'text', 'nchar', 'char'):
        return False
    for kw in PII_KEYWORDS:
        if kw in name:
            return True
    return False

def get_connection(db: str, autocommit: bool = False):
    try:
        return pyodbc.connect(f"DRIVER={{{DRIVER}}};SERVER={SERVER};DATABASE={db};UID={UID};PWD={PWD};TrustServerCertificate=yes;Encrypt=no;", autocommit=autocommit)
    except Exception as e:
        print(f"[DB ERROR] Could not connect to database: {e}")
        sys.exit(1)

def get_tables(cursor) -> list:
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    return [row[0] for row in cursor.fetchall()]

def get_columns(cursor, table: str) -> list:
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

def query_ollama(prompt):
    url = "http://localhost:11434/api/generate"
    headers = {"Content-Type": "application/json"}
    data = {
        "model": "gemma3:4b",
        "prompt": prompt,
        "stream": False,
        "temperature": 0.7
    }
    response = requests.post(url, headers=headers, json=data)
    result = response.json()
    return result.get("response", "").strip()

def query_lmstudio(prompt: str) -> str:
    url = "http://localhost:1234/v1/completions"
    headers = {"Content-Type": "application/json"}
    prompt = prompt + " Please return only a short, plain text value, not code or structured data."
    data = {
        "model": "thebloke/mistral-7b-instruct-v0.1",
        "prompt": prompt,
        "max_tokens": 128,
        "temperature": 0.7
    }
    print(f"[LM Studio] Sending prompt: {prompt}")
    try:
        response = requests.post(url, headers=headers, json=data)
        result = response.json()
        generated_text = result.get("choices", [{}])[0].get("text", "").strip()
        print(f"[LM Studio] Received response: {generated_text}")
        return generated_text
    except Exception as e:
        print(f"[LM Studio ERROR] {e}")
        return "LMSTUDIO_ERROR"

FREE_TEXT_FIELDS = ["description", "comment", "bio", "notes", "summary", "about", "details", "remarks", "feedback", "message", "text", "content"]

FAKER_ONLY = ["name", "email", "address", "phone", "city", "country", "zip", "postal"]

def generate_synthetic_value(col, typ):
    name = col.lower()
    try:
        # Use Faker for common PII fields
        if any(f in name for f in FAKER_ONLY):
            if 'name' in name:
                val = fake.name()
            elif 'email' in name:
                val = fake.email()
            elif 'address' in name:
                val = fake.address()
            elif 'phone' in name:
                val = fake.phone_number()
            elif 'city' in name:
                val = fake.city()
            elif 'country' in name:
                val = fake.country()
            elif 'zip' in name or 'postal' in name:
                val = fake.postcode()
            else:
                val = fake.word()
            print(f"[PII SECURE] {col} (Faker): {val}")
            return val
        # Use Faker for free-text fields instead of LM Studio
        if typ in ("nvarchar", "varchar", "text", "nchar", "char") and any(f in name for f in FREE_TEXT_FIELDS):
            val = fake.sentence(nb_words=12)
            print(f"[PII SECURE] {col} (Faker Free-Text): {val}")
            return val
        # For other string fields, use Faker word
        if typ in ("nvarchar", "varchar", "text", "nchar", "char"):
            val = fake.word()
            return val
        # For int types, generate random int
        if typ in ("int", "bigint", "tinyint", "smallint"):
            val = random.randint(1, 100000)
            return val
        # For dates, generate plausible date
        if 'date' in typ:
            val = fake.date_time_this_decade().strftime('%Y-%m-%d %H:%M:%S')
            return val
        return fake.word()
    except Exception as e:
        print(f"[PII ERROR] Failed to generate value for {col}: {e}")
        return None

def generate_faker_values(columns, row):
    """Generate synthetic values using Faker for applicable columns."""
    synth_row = []
    for (col, typ), val in zip(columns, row):
        if is_pii_column(col, typ) and not any(f in col.lower() for f in FREE_TEXT_FIELDS):
            synth_row.append(generate_synthetic_value(col, typ))  # Use Faker
        else:
            synth_row.append(val)  # Keep original or defer to LM Studio
    return synth_row

def generate_lmstudio_values(columns, row):
    """Generate synthetic values using LM Studio for applicable columns."""
    synth_row = []
    for (col, typ), val in zip(columns, row):
        if is_pii_column(col, typ) and any(f in col.lower() for f in FREE_TEXT_FIELDS):
            synth_row.append(generate_synthetic_value(col, typ))  # Use LM Studio
        else:
            synth_row.append(val)  # Keep original or defer to Faker
    return synth_row

# Ensure output folder exists
OUTPUT_FOLDER = "output_csvs"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

def create_table_if_not_exists(dest_cursor, table, columns):
    col_defs = []
    for col, typ in columns:
        t = typ.lower()
        if t in ("nvarchar", "varchar"):
            col_defs.append(f"[{col}] {t}(max)")
        elif t in ("nchar", "char"):
            col_defs.append(f"[{col}] {t}(32)")
        elif t in ("int", "bigint", "tinyint", "smallint"):
            col_defs.append(f"[{col}] {t}")
        elif t in ("datetime", "date", "time"):
            col_defs.append(f"[{col}] {t}")
        else:
            col_defs.append(f"[{col}] nvarchar(max)")
    col_defs_str = ", ".join(col_defs)
    dest_cursor.execute(f"IF OBJECT_ID('[{table}]', 'U') IS NULL CREATE TABLE [{table}] ({col_defs_str})")

def format_preserve_mask(val):
    if pyffx is None:
        return val  # fallback if pyffx not installed
    try:
        if isinstance(val, int) or (isinstance(val, str) and val.isdigit()):
            return ffx_int.encrypt(int(val))
        elif isinstance(val, str) and val:
            return ffx_str.encrypt(val)
        else:
            return val
    except Exception:
        return val

def main():
    print(f"Connecting to source DB: {SOURCE_DB}...")
    with get_connection(SOURCE_DB) as src_conn:
        src_cur = src_conn.cursor()
        tables = get_tables(src_cur)
        print(f"Found tables: {tables}")
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)

        print(f"Connecting to destination DB: {DEST_DB}...")
        with get_connection("master", autocommit=True) as master_conn:
            master_cur = master_conn.cursor()
            master_cur.execute(f"IF DB_ID('{DEST_DB}') IS NULL CREATE DATABASE [{DEST_DB}]")
        with get_connection(DEST_DB) as dest_conn:
            dest_cur = dest_conn.cursor()

            for table in tables:
                print(f"\n--- Exporting table: {table} ---")
                columns = get_columns(src_cur, table)
                colnames = [col for col, _ in columns]
                create_table_if_not_exists(dest_cur, table, columns)
                dest_conn.commit()
                src_cur.execute(f"SELECT * FROM [{table}]")
                rows = src_cur.fetchall()
                print(f"  Rows to export: {len(rows)}")
                csv_filename = os.path.join(OUTPUT_FOLDER, f"{table}.csv")

                with open(csv_filename, mode="w", newline='', encoding="utf-8") as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(colnames)

                    for idx, row in enumerate(tqdm(rows, desc=f"Exporting {table}", unit="row")):
                        synth_row = []
                        for (col, typ), val in zip(columns, row):
                            col_l = col.lower()
                            if col_l == "description":
                                synth_row.append(None)
                            elif any(x in col_l for x in ["id", "ssn", "accountnumber"]):
                                synth_row.append(format_preserve_mask(val))
                            elif is_pii_column(col, typ) or any(f in col_l for f in FREE_TEXT_FIELDS):
                                synth_val = generate_synthetic_value(col, typ)
                                if synth_val is None:
                                    synth_row.append("NULL")
                                else:
                                    synth_row.append(synth_val)
                            else:
                                synth_row.append(val)
                        writer.writerow(synth_row)
                        # Insert into destination DB
                        placeholders = ','.join(['?' for _ in synth_row])
                        insert_sql = f"INSERT INTO [{table}] ({', '.join('['+c+']' for c in colnames)}) VALUES ({placeholders})"
                        try:
                            dest_cur.execute(insert_sql, synth_row)
                        except Exception as e:
                            print(f"[DEST_DB ERROR] Failed to insert into {table}: {e}")
                        if idx % 1000 == 0:
                            dest_conn.commit()
                    dest_conn.commit()
                print(f"    Done: {csv_filename}")
    print(f"\n✅ All tables exported to CSV files and written to destination DB: {DEST_DB}")

if __name__ == "__main__":
    main()
