import json
import random
import re
import string
import time
import uuid
import pyodbc
import requests

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

def extract_json(text):
    text = text.strip()
    if text.startswith('```'):
        text = text.replace('```json', '').replace('```', '').strip()
    match = re.search(r'\{[\s\S]*\}', text)
    return match.group(0) if match else text

def dtype_hint(sql_type):
    if sql_type in ("int", "bigint", "tinyint", "smallint"):
        return "Integer"
    if sql_type in ("bit",):
        return "0 or 1"
    if "char" in sql_type or "text" in sql_type:
        return "String"
    if "date" in sql_type or "time" in sql_type:
        return "yyyy-mm-dd hh:mm:ss.fff or null"
    return "Unknown"

def sql_type(sql_type):
    if "char" in sql_type:
        return "nvarchar(max)"
    if sql_type in ("int", "bigint"):
        return "int"
    if "date" in sql_type or "time" in sql_type:
        return "datetime"
    return "nvarchar(max)"

def get_table_schema(cursor, table):
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """, table)
    return cursor.fetchall()

def sample_rows(cursor, table, limit=5):
    cursor.execute(f"SELECT TOP {limit} * FROM {table}")
    colnames = [d[0] for d in cursor.description]
    rows = [dict(zip(colnames, r)) for r in cursor.fetchall()]
    for row in rows:
        for k in ("PASSWORD", "GUID", "ACCOUNT_GUID", "SECRET_KEY"):
            if k in row and row[k] is not None:
                row[k] = "GENERATE_NEW"
    return rows

def serialize_for_json(obj):
    if isinstance(obj, dict):
        return {k: serialize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [serialize_for_json(i) for i in obj]
    elif hasattr(obj, 'isoformat'):
        try:
            return obj.isoformat()
        except Exception:
            return str(obj)
    else:
        return obj

def build_prompt(schema, examples, extra_rules=""):
    col_lines = [f'  "{c}": {dtype_hint(t)}' for c, t in schema]
    schema_block = "{\n" + ",\n".join(col_lines) + "\n}"
    safe_examples = serialize_for_json(examples)
    prompt = f"""
You are generating synthetic HR-user records.

• Follow this schema (types are hints):\n{schema_block}

• Here are {len(examples)} real rows to imitate
  (PII and secrets are masked, do NOT copy values verbatim):
{json.dumps(safe_examples, indent=2)}

IMPORTANT: For any field with the value GENERATE_NEW, you must generate a realistic, unique value from scratch. Do not copy or repeat this value in your output. Do not use any real or redacted value from the examples.

{extra_rules}

Return one new record as valid JSON following the schema.
"""
    return prompt.strip()

def ensure_dest_table(cursor, schema, dest_table):
    cols = ", ".join(f"[{c}] {sql_type(t)}" for c, t in schema)
    cursor.execute(f"""
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{dest_table}') AND type in (N'U'))
    BEGIN
        CREATE TABLE {dest_table} ({cols})
    END
    """)

def insert_records(cursor, records, schema, dest_table):
    colnames = [c for c, _ in schema]
    qmarks = ", ".join("?" for _ in colnames)
    collist = ", ".join(f"[{c}]" for c in colnames)
    for rec in records:
        values = [rec.get(c) for c in colnames]
        cursor.execute(
            f"INSERT INTO {dest_table} ({collist}) VALUES ({qmarks})", values
        )

def get_next_employee_id(cursor, table):
    cursor.execute(f"SELECT ISNULL(MAX(EMPLOYEE_ID), 9999) FROM {table}")
    max_id = cursor.fetchone()[0]
    return max_id + 1

def normalize_datetime_fields(record, schema):
    from datetime import datetime
    for col, typ in schema:
        if 'date' in typ or 'time' in typ:
            val = record.get(col)
            if val and isinstance(val, str):
                # Try parsing common formats, output as 'YYYY-MM-DD HH:MM:SS'
                for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y"):
                    try:
                        dt = datetime.strptime(val[:19], fmt)
                        record[col] = dt.strftime("%Y-%m-%d %H:%M:%S")
                        break
                    except Exception:
                        continue
    return record

def main():
    SOURCE_TABLE = "AccessCatalystUsers"
    DEST_TABLE = "AccessCatalystUsers_Synthetic"
    NUM_RECORDS = int(input("How many synthetic records to generate? "))

    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=DESKTOP-R9S4CFK;"
        "DATABASE=HR_Synthetic;"
        "UID=sa;"
        "PWD=(catalystone123);"
        "TrustServerCertificate=yes;"
        "Encrypt=no;"
    )

    start_time = time.time()

    with pyodbc.connect(conn_str) as conn:
        cur = conn.cursor()

        schema = get_table_schema(cur, SOURCE_TABLE)
        examples = sample_rows(cur, SOURCE_TABLE)
        ensure_dest_table(cur, schema, DEST_TABLE)

        next_id = get_next_employee_id(cur, SOURCE_TABLE)

        for i in range(NUM_RECORDS):
            print(f"\n=== Generating record {i + 1} of {NUM_RECORDS} ===")
            extra_note = f"The EMPLOYEE_ID must be {next_id}."
            prompt = build_prompt(schema, examples, extra_rules=extra_note)

            raw_output = query_ollama(prompt)
            try:
                json_candidate = extract_json(raw_output)
                record = json.loads(json_candidate)
                record["EMPLOYEE_ID"] = next_id

                if not record.get("SECRET_KEY"):
                    record["SECRET_KEY"] = ''.join(random.choices(string.ascii_letters + string.digits, k=16))

                # Normalize date/time fields before insert
                record = normalize_datetime_fields(record, schema)

                insert_records(cur, [record], schema, DEST_TABLE)
                print(json.dumps(record, indent=2))
                next_id += 1

            except Exception as e:
                print("[ERROR] Could not parse/generate record:", e)
                print(raw_output)

        conn.commit()

    elapsed = time.time() - start_time
    print(f"\n✅ Done. Time elapsed: {elapsed:.2f} seconds")

if __name__ == "__main__":
    main()
