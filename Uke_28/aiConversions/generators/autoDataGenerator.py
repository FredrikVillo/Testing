import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, select
from faker import Faker
import random
import sys
import os
import pyodbc
from datetime import datetime, date, timedelta
from openai import AzureOpenAI

fake = Faker()

def get_sql_server_engine():
    # Bruk samme connection string som i load_generated_data.py
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=DESKTOP-R9S4CFK;"
        "DATABASE=sql_alchemy_test;"
        "UID=sa;PWD=(catalystone123);"
    )
    return pyodbc.connect(conn_str)

def read_Database_table(table_name, db_connection):
    cursor = db_connection.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    columns = [column[0] for column in cursor.description]
    rows = cursor.fetchall()
    return columns, rows

def rule_engine(field_name, field_type, description, settings, is_custom):
    # Enkel heuristikk: bruk AI på tekstfelt, ellers faker
    if field_type.lower() in ["varchar", "nvarchar", "text"] and not is_custom:
        return "use_ai"
    return "use_faker"

def get_azure_openai_client(api_key_path):
    with open(api_key_path, "r") as f:
        api_key = f.read().strip()
    client = AzureOpenAI(
        api_key=api_key,
        api_version="2025-01-01-preview",
        azure_endpoint="https://azureopenai-sin-dev.openai.azure.com"
    )
    return client

def generate_data_with_ai(client, field_name, field_type, description, settings, is_custom, max_length=None):
    # Strengere prompt for å få korte, enkle verdier
    prompt = (
        f"Generate a realistic, short, single value for the field '{field_name}' of type '{field_type}'. "
        f"Return only the value, no explanation, no formatting, no code block. "
        f"The value must be valid for a SQL column of type '{field_type}' and fit within the max length {max_length if max_length else ''}. "
        f"No quotes, no markdown, no extra text."
    )
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You generate realistic fake test data. Only output the value, never an explanation or code block."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=500
        )
        if response and hasattr(response, 'choices') and response.choices and hasattr(response.choices[0], 'message') and hasattr(response.choices[0].message, 'content'):
            ai_value = response.choices[0].message.content.strip()
            if max_length and isinstance(ai_value, str):
                ai_value = ai_value[:max_length]
            return ai_value
        else:
            print(f"[WARNING] Tomt eller ugyldig AI-svar for {field_name}. Bruker fallback-verdi.")
            return "AI"
    except Exception as e:
        print(f"[ERROR] AI-generering feilet for {field_name}: {e}. Bruker fallback-verdi.")
        return "AI"

def generate_data_with_faker(field_name, field_type, description, settings, is_custom):
    # Enkel mapping for noen vanlige felttyper
    if "name" in field_name.lower():
        return fake.name()
    # Strengtyper
    if field_type.lower() in ["varchar", "nvarchar", "text", "char"]:
        return fake.word()
    # Dato/dato-tid (kun hvis SQL-type er date/datetime)
    if field_type.lower() in ["date"]:
        return fake.date_object().strftime("%Y-%m-%d")
    if field_type.lower() in ["datetime", "smalldatetime", "timestamp", "datetime2"]:
        dt = fake.date_time_this_decade()
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    # GUID/UUID
    if field_type.lower() in ["uniqueidentifier"]:
        return str(fake.uuid4())
    # Talltyper
    if field_type.lower() in ["int", "bigint", "smallint", "tinyint"]:
        return random.randint(1, 1000)
    if field_type.lower() in ["float", "decimal", "numeric", "real"]:
        return float(round(random.uniform(1, 1000), 2))
    # Boolsk
    if field_type.lower() in ["bit"]:
        return random.choice([0, 1])
    # Fallback
    return fake.word()

def generate_data(client, field_name, field_type, description, settings, is_custom, max_length=None):
    # Tving int-felter til å alltid få int, uansett navn
    if field_type.lower() in ["int", "bigint", "smallint", "tinyint"]:
        return random.randint(1, 1000)
    rule = rule_engine(field_name, field_type, description, settings, is_custom)
    if rule == "use_ai":
        return generate_data_with_ai(client, field_name, field_type, description, settings, is_custom, max_length)
    elif rule == "use_faker":
        return generate_data_with_faker(field_name, field_type, description, settings, is_custom)
    else:
        raise ValueError("Ugyldig regel for datagenerering")

def write_to_database(table_name, data, db_connection):
    cursor = db_connection.cursor()
    if not data:
        return
    columns = list(data[0].keys())
    placeholders = ','.join(['?'] * len(columns))
    col_list = ','.join(columns)
    for row in data:
        values = [row.get(col) for col in columns]
        cursor.execute(f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})", values)
    db_connection.commit()

def get_all_table_names(db_connection):
    cursor = db_connection.cursor()
    cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'")
    return [row[0] for row in cursor.fetchall()]

def get_foreign_key_dependencies(db_connection):
    cursor = db_connection.cursor()
    cursor.execute('''
        SELECT
            fk_tab.TABLE_NAME AS FK_TABLE,
            pk_tab.TABLE_NAME AS PK_TABLE
        FROM
            INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS ref
            JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS fk_tab ON ref.CONSTRAINT_NAME = fk_tab.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk_tab ON ref.UNIQUE_CONSTRAINT_NAME = pk_tab.CONSTRAINT_NAME
        WHERE
            fk_tab.CONSTRAINT_TYPE = 'FOREIGN KEY'
    ''')
    deps = {}
    for fk_table, pk_table in cursor.fetchall():
        deps.setdefault(fk_table, set()).add(pk_table)
    return deps

def debug_print_dependencies(dependencies):
    print("[DEBUG] Foreign key dependencies (child -> parents):")
    for child, parents in dependencies.items():
        print(f"  {child} -> {list(parents)}")

def topological_sort_tables(table_names, dependencies):
    from collections import defaultdict, deque
    in_degree = defaultdict(int)
    graph = defaultdict(list)
    for table in table_names:
        in_degree[table] = 0
    for table, deps in dependencies.items():
        for dep in deps:
            graph[dep].append(table)
            in_degree[table] += 1
    queue = deque([t for t in table_names if in_degree[t] == 0])
    sorted_tables = []
    while queue:
        t = queue.popleft()
        sorted_tables.append(t)
        for neighbor in graph[t]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    if len(sorted_tables) != len(table_names):
        raise Exception("Cyclic dependency detected in table hierarchy!")
    return sorted_tables

def get_foreign_key_columns(db_connection, table_name):
    cursor = db_connection.cursor()
    cursor.execute('''
        SELECT kcu.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
          ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        WHERE tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
    ''', (table_name,))
    return [row[0] for row in cursor.fetchall()]

def get_identity_columns(db_connection, table_name):
    cursor = db_connection.cursor()
    cursor.execute('''
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ? AND COLUMNPROPERTY(object_id(TABLE_SCHEMA + '.' + TABLE_NAME), COLUMN_NAME, 'IsIdentity') = 1
    ''', (table_name,))
    return [row[0] for row in cursor.fetchall()]

def get_parent_table_and_pk(db_connection, table_name, fk_col):
    cursor = db_connection.cursor()
    cursor.execute('''
        SELECT pk.TABLE_NAME, pkc.COLUMN_NAME
        FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE fk ON rc.CONSTRAINT_NAME = fk.CONSTRAINT_NAME
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE pkc ON rc.UNIQUE_CONSTRAINT_NAME = pkc.CONSTRAINT_NAME
        JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS pk ON pkc.CONSTRAINT_NAME = pk.CONSTRAINT_NAME
        WHERE fk.TABLE_NAME = ? AND fk.COLUMN_NAME = ?
    ''', (table_name, fk_col))
    row = cursor.fetchone()
    if row:
        return row[0], row[1]
    return None, None

def get_first_pk_value(db_connection, table_name, pk_col):
    cursor = db_connection.cursor()
    cursor.execute(f"SELECT TOP 1 {pk_col} FROM {table_name}")
    result = cursor.fetchone()
    return result[0] if result else None

def write_to_database_with_fk_handling(table_name, data, db_connection):
    fk_cols = get_foreign_key_columns(db_connection, table_name)
    id_cols = get_identity_columns(db_connection, table_name)
    cursor = db_connection.cursor()
    if not data:
        return
    columns = [col for col in data[0].keys() if col not in id_cols]
    placeholders = ','.join(['?'] * len(columns))
    col_list = ','.join(columns)
    inserted_rows = []
    # Forhåndshent parent PK-verdier for alle FK
    fk_parent_ids = {}
    fk_nullable = {}
    for fk in fk_cols:
        # Sjekk om kolonnen er nullable
        cursor.execute('''SELECT IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?''', (table_name, fk))
        is_nullable = cursor.fetchone()
        fk_nullable[fk] = (is_nullable and is_nullable[0] == 'YES')
        parent_table, parent_pk = get_parent_table_and_pk(db_connection, table_name, fk)
        if parent_table and parent_pk:
            cursor.execute(f"SELECT {parent_pk} FROM {parent_table}")
            fk_parent_ids[fk] = [row[0] for row in cursor.fetchall()]
        else:
            fk_parent_ids[fk] = []
    # Første runde: sett FK til NULL hvis nullable, ellers til gyldig PK eller dummy-verdi
    for idx, row in enumerate(data):
        row_insert = {col: row[col] for col in columns}
        for fk in fk_cols:
            if fk_nullable.get(fk, True):
                row_insert[fk] = None
            else:
                parent_ids = fk_parent_ids.get(fk, [])
                if parent_ids:
                    row_insert[fk] = parent_ids[idx % len(parent_ids)]
                else:
                    # Hent datatype for FK
                    cursor.execute('''SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?''', (table_name, fk))
                    fk_type_row = cursor.fetchone()
                    fk_type = fk_type_row[0] if fk_type_row else None
                    if fk_type and fk_type.lower() in ["int", "bigint", "smallint", "tinyint"]:
                        row_insert[fk] = random.randint(1, 1000000)
                    elif fk_type and fk_type.lower() in ["uniqueidentifier"]:
                        row_insert[fk] = str(fake.uuid4())
                    else:
                        row_insert[fk] = "dummy"
        values = [row_insert.get(col) for col in columns]
        cursor.execute(f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})", values)
        inserted_rows.append(row_insert)
    db_connection.commit()
    # Andre runde: oppdater FK-felter til gyldige PK-verdier hvis mulig (kun for nullable FK)
    for fk in fk_cols:
        if not fk_nullable.get(fk, True):
            continue  # Hopp over NOT NULL-FK, de er allerede satt
        parent_table, parent_pk = get_parent_table_and_pk(db_connection, table_name, fk)
        if parent_table and parent_pk:
            cursor.execute(f"SELECT {parent_pk} FROM {parent_table}")
            parent_ids = [row[0] for row in cursor.fetchall()]
            if parent_ids:
                cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", (table_name,))
                all_cols = [r[0] for r in cursor.fetchall()]
                cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", (table_name,))
                all_col_types = {r[0]: r[1] for r in cursor.fetchall()}
                cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = ? AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1", (table_name,))
                pk_cols = [r[0] for r in cursor.fetchall()]
                pk_col_types = {col: all_col_types.get(col, None) for col in pk_cols}
                pk_col_indices = [all_cols.index(pk_col) + 2 for pk_col in pk_cols]  # +2 pga rn og 0-indeksering
                cursor.execute(f"SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 1)) AS rn, * FROM {table_name}")
                rows = cursor.fetchall()
                cursor.execute('''SELECT DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND COLUMN_NAME = ?''', (table_name, fk))
                fk_type_row = cursor.fetchone()
                fk_type = fk_type_row[0] if fk_type_row else None
                def convert_pk_val(val):
                    if fk_type is None:
                        return val
                    if fk_type.lower() in ["int", "bigint", "smallint", "tinyint"]:
                        try:
                            return int(val)
                        except Exception:
                            return None
                    if fk_type.lower() in ["float", "decimal", "numeric", "real"]:
                        try:
                            return float(val)
                        except Exception:
                            return None
                    if fk_type.lower() in ["bit"]:
                        return int(bool(val))
                    if fk_type.lower() in ["uniqueidentifier"]:
                        return str(val)
                    return str(val)
                def convert_pk_values(pk_values):
                    converted = []
                    for idx, val in enumerate(pk_values):
                        col = pk_cols[idx]
                        typ = pk_col_types.get(col, None)
                        if typ and typ.lower() in ["int", "bigint", "smallint", "tinyint"]:
                            try:
                                converted.append(int(val))
                            except Exception:
                                converted.append(None)
                        elif typ and typ.lower() in ["float", "decimal", "numeric", "real"]:
                            try:
                                converted.append(float(val))
                            except Exception:
                                converted.append(None)
                        elif typ and typ.lower() in ["bit"]:
                            converted.append(int(bool(val)))
                        elif typ and typ.lower() in ["uniqueidentifier"]:
                            converted.append(str(val))
                        else:
                            converted.append(str(val))
                    return converted
                for idx, db_row in enumerate(rows):
                    pk_val = parent_ids[idx % len(parent_ids)]
                    pk_val = convert_pk_val(pk_val)
                    set_pk = ' AND '.join([f"{col} = ?" for col in pk_cols])
                    update_sql = f"UPDATE {table_name} SET {fk} = ? WHERE {set_pk}"
                    pk_values = [db_row[i] for i in pk_col_indices]
                    pk_values = convert_pk_values(pk_values)
                    cursor.execute(update_sql, [pk_val] + pk_values)
    db_connection.commit()
    print(f"[INFO] To-pass FK-oppdatering kjørt for {table_name}.")

def get_table_columns_and_types(db_connection, table_name):
    cursor = db_connection.cursor()
    cursor.execute('''
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
    ''', (table_name,))
    return [(row[0], row[1]) for row in cursor.fetchall()]

def set_all_foreign_keys_nullable(db_connection):
    cursor = db_connection.cursor()
    # Finn alle FK-felter med datatype
    cursor.execute('''
        SELECT 
            OBJECT_NAME(fk_col.object_id) AS TableName,
            fk_col.name AS ColumnName,
            typ.name AS DataType,
            fk_col.max_length
        FROM sys.foreign_key_columns fkc
        JOIN sys.columns fk_col ON fkc.parent_object_id = fk_col.object_id AND fkc.parent_column_id = fk_col.column_id
        JOIN sys.types typ ON fk_col.user_type_id = typ.user_type_id
    ''')
    for table, col, dtype, maxlen in cursor.fetchall():
        # Bygg riktig ALTER TABLE-setning for datatype
        if dtype.lower() in ["varchar", "nvarchar", "char"]:
            type_str = f"{dtype}({int(maxlen) // 2 if dtype.lower() == 'nvarchar' else int(maxlen)})"
        elif dtype.lower() in ["decimal", "numeric"]:
            cursor2 = db_connection.cursor()
            cursor2.execute('''SELECT precision, scale FROM sys.columns WHERE object_id = OBJECT_ID(?) AND name = ?''', (table, col))
            pres, scale = cursor2.fetchone()
            type_str = f"{dtype}({pres},{scale})"
        else:
            type_str = dtype
        sql = f"ALTER TABLE {table} ALTER COLUMN {col} {type_str} NULL"
        try:
            cursor.execute(sql)
            print(f"[INFO] FK-kolonne satt til NULL: {table}.{col} ({type_str})")
        except Exception as e:
            print(f"[WARNING] Klarte ikke sette {table}.{col} til NULL: {e}")
    db_connection.commit()

def set_all_foreign_keys_not_null(db_connection):
    cursor = db_connection.cursor()
    # Finn alle FK-felter med datatype
    cursor.execute('''
        SELECT 
            OBJECT_NAME(fk_col.object_id) AS TableName,
            fk_col.name AS ColumnName,
            typ.name AS DataType,
            fk_col.max_length
        FROM sys.foreign_key_columns fkc
        JOIN sys.columns fk_col ON fkc.parent_object_id = fk_col.object_id AND fkc.parent_column_id = fk_col.column_id
        JOIN sys.types typ ON fk_col.user_type_id = typ.user_type_id
    ''')
    for table, col, dtype, maxlen in cursor.fetchall():
        # Bygg riktig ALTER TABLE-setning for datatype
        if dtype.lower() in ["varchar", "nvarchar", "char"]:
            type_str = f"{dtype}({int(maxlen) // 2 if dtype.lower() == 'nvarchar' else int(maxlen)})"
        elif dtype.lower() in ["decimal", "numeric"]:
            cursor2 = db_connection.cursor()
            cursor2.execute('''SELECT precision, scale FROM sys.columns WHERE object_id = OBJECT_ID(?) AND name = ?''', (table, col))
            pres, scale = cursor2.fetchone()
            type_str = f"{dtype}({pres},{scale})"
        else:
            type_str = dtype
        sql = f"ALTER TABLE {table} ALTER COLUMN {col} {type_str} NOT NULL"
        try:
            cursor.execute(sql)
            print(f"[INFO] FK-kolonne satt til NOT NULL: {table}.{col} ({type_str})")
        except Exception as e:
            print(f"[WARNING] Klarte ikke sette {table}.{col} til NOT NULL: {e}")
    db_connection.commit()

def get_unique_columns(db_connection, table_name):
    cursor = db_connection.cursor()
    # Hent PK-kolonner
    cursor.execute('''
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
        WHERE TABLE_NAME = ? AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
    ''', (table_name,))
    pk_cols = [r[0] for r in cursor.fetchall()]
    # Hent UNIQUE-kolonner
    cursor.execute('''
        SELECT kcu.COLUMN_NAME
        FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
        WHERE tc.TABLE_NAME = ? AND tc.CONSTRAINT_TYPE = 'UNIQUE'
    ''', (table_name,))
    unique_cols = [r[0] for r in cursor.fetchall()]
    return list(set(pk_cols + unique_cols))

def generate_unique_value(table_name, col, typ, i, unique_counters):
    if typ.lower() in ["int", "bigint", "smallint", "tinyint"]:
        val = unique_counters.get((table_name, col), 1)
        unique_counters[(table_name, col)] = val + 1
        return val
    elif typ.lower() in ["varchar", "nvarchar", "text", "char"]:
        return f"{table_name}_{col}_{i}_{random.randint(1000,9999)}"
    elif typ.lower() in ["uniqueidentifier"]:
        return str(fake.uuid4())
    else:
        return f"{table_name}_{col}_{i}_{random.randint(1000,9999)}"

def generate_unique_pk_combo(pk_cols, pk_types, i):
    # Fordel i over alle kolonner for å sikre unik kombinasjon
    combo = {}
    base = i + 1
    for idx, (col, typ) in enumerate(zip(pk_cols, pk_types)):
        if typ.lower() in ["int", "bigint", "smallint", "tinyint"]:
            combo[col] = base + idx * 10000  # offset for å sikre unikhet
        elif typ.lower() in ["varchar", "nvarchar", "text", "char"]:
            combo[col] = f"PK_{col}_{base}_{random.randint(1000,9999)}"
        elif typ.lower() in ["uniqueidentifier"]:
            combo[col] = str(fake.uuid4())
        else:
            combo[col] = f"PK_{col}_{base}_{random.randint(1000,9999)}"
    return combo

def get_next_pk_start(db_connection, table_name, pk_col):
    cursor = db_connection.cursor()
    try:
        cursor.execute(f"SELECT MAX([{pk_col}]) FROM {table_name}")
        result = cursor.fetchone()
        return (result[0] or 0) + 1
    except Exception:
        return 1

def get_column_max_lengths(db_connection, table_name):
    cursor = db_connection.cursor()
    cursor.execute('''
        SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
    ''', (table_name,))
    return {row[0]: row[2] for row in cursor.fetchall() if row[1].lower() in ["varchar", "nvarchar", "char"]}

def main():
    conn = get_sql_server_engine()
    set_all_foreign_keys_nullable(conn)
    table_names = get_all_table_names(conn)
    dependencies = get_foreign_key_dependencies(conn)
    debug_print_dependencies(dependencies)
    try:
        sorted_tables = topological_sort_tables(table_names, dependencies)
    except Exception as e:
        print("[WARNING] Syklisk avhengighet oppdaget! Bruker FK-handling for å sette FK-felter til NULL i første runde.")
        sorted_tables = table_names  # Faller tilbake til vilkårlig rekkefølge
    print(f"Tabellrekkefølge (avhengighetsorden): {sorted_tables}")
    description = ""
    settings = {}
    is_custom = False
    client = get_azure_openai_client("C:/Users/FredrikVillo/repos/TestDataGeneration/api_key.txt")
    for table_name in sorted_tables:
        print(f"Genererer data for tabell: {table_name}")
        col_types = get_table_columns_and_types(conn, table_name)
        columns = [col for col, _ in col_types]
        unique_cols = get_unique_columns(conn, table_name)
        # Finn PK-kolonner og deres typer
        cursor = conn.cursor()
        cursor.execute('''SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME = ? AND OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1''', (table_name,))
        pk_cols = [r[0] for r in cursor.fetchall()]
        pk_types = [typ for col, typ in col_types if col in pk_cols]
        data = []
        unique_counters = {}
        # Finn startverdi for int-PK
        pk_start = {}
        for col, typ in zip(pk_cols, pk_types):
            if typ.lower() in ["int", "bigint", "smallint", "tinyint"]:
                pk_start[col] = get_next_pk_start(conn, table_name, col)
        # Hent max_length for alle kolonner i tabellen
        col_max_lengths = get_column_max_lengths(conn, table_name)
        for i in range(2):
            row = {}
            # Hvis composite PK, generer unik kombinasjon
            if len(pk_cols) > 1:
                pk_combo = generate_unique_pk_combo(pk_cols, pk_types, i)
                for col in pk_cols:
                    row[col] = pk_combo[col]
            elif len(pk_cols) == 1 and pk_cols[0] in unique_cols:
                col = pk_cols[0]
                typ = pk_types[0]
                if typ.lower() in ["int", "bigint", "smallint", "tinyint"]:
                    row[col] = pk_start[col] + i
                else:
                    row[col] = generate_unique_value(table_name, col, typ, i, unique_counters)
            for col, typ in col_types:
                if col in pk_cols and (len(pk_cols) > 1 or (len(pk_cols) == 1 and col in row)):
                    continue  # Allerede satt
                if col in unique_cols:
                    row[col] = generate_unique_value(table_name, col, typ, i, unique_counters)
                else:
                    if typ.lower() in ["int", "bigint", "smallint", "tinyint"]:
                        row[col] = random.randint(1, 1000)
                    elif typ.lower() in ["varchar", "nvarchar", "char"]:
                        maxlen = col_max_lengths.get(col)
                        val = generate_data(client, col, typ, description, settings, is_custom, max_length=maxlen)
                        if maxlen and isinstance(val, str):
                            val = val[:maxlen]
                        row[col] = val
                    else:
                        row[col] = generate_data(client, col, typ, description, settings, is_custom)
            data.append(row)
        write_to_database_with_fk_handling(table_name, data, conn)
        print(f"✅ Genererte og skrev {len(data)} rader til {table_name}")
    set_all_foreign_keys_not_null(conn)
    conn.close()

if __name__ == "__main__":
    main()