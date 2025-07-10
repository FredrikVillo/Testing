import pyodbc
import json
import os
import re
import sys

# Always resolve paths relative to the project root
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))

def rel_path(*parts):
    return os.path.join(PROJECT_ROOT, *parts)

# Add dry run argument
dry_run = '--dry-run' in sys.argv

# Set canonical output directory for loader
if dry_run:
    DATA_DIR = rel_path('aiConversions', 'data', 'output', 'dryRun')
    print("[INFO] Dry run mode: loading data from 'dryRun' folder.")
else:
    DATA_DIR = rel_path('aiConversions', 'data', 'output', 'latest')
    print("[INFO] Loading data from 'latest' folder.")

# Koble til SQL Server med sa-login
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 17 for SQL Server};SERVER=DESKTOP-R9S4CFK;DATABASE=dry_run_test;UID=sa;PWD=(catalystone123);"
)
cursor = conn.cursor()

def clean_database(cursor):
    # Slett i omvendt FK-rekkefølge og reset identity
    tables_with_identity = [
        'USERPROFILE_HISTORY',
        'USERPROFILE_FIELD',
        'ACCESSCATALYST'
    ]
    # Sletting i omvendt rekkefølge av avhengigheter
    delete_order = [
        'USERPROFILE_HISTORY',
        'USERPROFILE',
        'USERPROFILE_FIELD',
        'ACCESSCATALYST',
        'EMPLOYEE',
        'ORGANIZATION',
        'SCALE',
        'SCALETYPE'
    ]
    print('Cleaning database...')
    for table in delete_order:
        try:
            cursor.execute(f"DELETE FROM {table}")
        except Exception as e:
            print(f"[WARNING] Could not delete from {table}: {e}")
    # Reset identity seed for tabeller med identity
    for table in tables_with_identity:
        try:
            cursor.execute(f"DBCC CHECKIDENT ('{table}', RESEED, 0)")
        except Exception as e:
            print(f"[WARNING] Could not reseed identity for {table}: {e}")
    cursor.connection.commit()
    print('Database cleaned and identity reseeded.')

# Hjelpefunksjon for å laste JSON
def load_json(filename):
    with open(os.path.join(DATA_DIR, filename), encoding='utf-8') as f:
        return json.load(f)

def get_table_columns_from_sql(sql_path, table_name):
    try:
        with open(sql_path, encoding='utf-8') as f:
            sql = f.read()
    except UnicodeDecodeError:
        with open(sql_path, encoding='utf-16') as f:
            sql = f.read()
    # Find the CREATE TABLE statement for the table
    pattern = rf"CREATE TABLE \[dbo\]\.\[{table_name}\]\((.*?)\)WITH"  # non-greedy match
    match = re.search(pattern, sql, re.DOTALL | re.IGNORECASE)
    if not match:
        raise ValueError(f"Could not find CREATE TABLE for {table_name}")
    cols_block = match.group(1)
    # Extract column names (first word in each line, between brackets)
    columns = []
    seen = set()
    for line in cols_block.splitlines():
        col_match = re.match(r'\s*\[([^\]]+)\]', line)
        if col_match:
            col = col_match.group(1)
            if col not in seen:
                columns.append(col)
                seen.add(col)
    return columns

def get_all_table_columns_from_sql(sql_path):
    try:
        with open(sql_path, encoding='utf-8') as f:
            sql = f.read()
    except UnicodeDecodeError:
        with open(sql_path, encoding='utf-16') as f:
            sql = f.read()
    # Finn alle CREATE TABLE ... ( ... )WITH ... blokker
    pattern = r"CREATE TABLE \[dbo\]\.\[(\w+)\]\((.*?)\)WITH"  # non-greedy match
    matches = re.findall(pattern, sql, re.DOTALL | re.IGNORECASE)
    table_columns = {}
    for table, cols_block in matches:
        columns = []
        seen = set()
        for line in cols_block.splitlines():
            col_match = re.match(r'\s*\[([^\]]+)\]', line)
            if col_match:
                col = col_match.group(1)
                if col not in seen:
                    columns.append(col)
                    seen.add(col)
        table_columns[table.upper()] = columns
    return table_columns

# Les alle tabellkolonner fra masterfilen
sql_master_path = rel_path('aiConversions', 'sql', 'automated_test.sql')
table_columns_dict = get_all_table_columns_from_sql(sql_master_path)

# Kjør automatisk database-clean før innlasting
clean_database(cursor)

# 1. SCALETYPE
print('Inserting SCALETYPE...')
scaletypes = load_json('scaletype_data.json')
scaletype_cols = table_columns_dict['SCALETYPE']
for row in scaletypes:
    values = [row.get(col) for col in scaletype_cols]
    placeholders = ', '.join(['?'] * len(scaletype_cols))
    col_list = ', '.join(scaletype_cols)
    cursor.execute(
        f"INSERT INTO SCALETYPE ({col_list}) VALUES ({placeholders})",
        values
    )
conn.commit()

# 2. SCALE
print('Inserting SCALE...')
if os.path.exists(os.path.join(DATA_DIR, 'scale_data_full.json')):
    scales = load_json('scale_data_full.json')
    scale_cols = table_columns_dict['SCALE']
    for row in scales:
        values = [row.get(col) for col in scale_cols]
        placeholders = ', '.join(['?'] * len(scale_cols))
        col_list = ', '.join(scale_cols)
        cursor.execute(
            f"INSERT INTO SCALE ({col_list}) VALUES ({placeholders})",
            values
        )
    conn.commit()

# 3. ORGANIZATION
print('Inserting ORGANIZATION...')
orgs = load_json('organization_data_with_gpt.json')
org_cols = table_columns_dict['ORGANIZATION']
for row in orgs:
    values = [row.get(col) for col in org_cols]
    placeholders = ', '.join(['?'] * len(org_cols))
    col_list = ', '.join(org_cols)
    cursor.execute(
        f"INSERT INTO ORGANIZATION ({col_list}) VALUES ({placeholders})",
        values
    )
conn.commit()

# 4. EMPLOYEE
print('Inserting EMPLOYEE...')
employees = load_json('employee_data_full.json')
emp_cols = table_columns_dict['EMPLOYEE']
for row in employees:
    values = [row.get(col) for col in emp_cols]
    placeholders = ', '.join(['?'] * len(emp_cols))
    col_list = ', '.join(emp_cols)
    cursor.execute(
        f"INSERT INTO EMPLOYEE ({col_list}) VALUES ({placeholders})",
        values
    )
conn.commit()

# 5. ACCESSCATALYST
print('Inserting ACCESSCATALYST...')
access = load_json('accesscatalyst_data.json')
access_cols = [col for col in table_columns_dict['ACCESSCATALYST'] if col.upper() != 'ACCESSCATALYST']  # ekskluder identity
access_seen = set()
access_skipped = 0
for idx, row in enumerate(access):
    # Deduplication: use tuple of all non-identity columns as key
    key = tuple(row.get(col) for col in access_cols)
    if key in access_seen:
        access_skipped += 1
        continue
    access_seen.add(key)
    values = [row.get(col) for col in access_cols]
    placeholders = ', '.join(['?'] * len(access_cols))
    col_list = ', '.join(access_cols)
    cursor.execute(
        f"INSERT INTO ACCESSCATALYST ({col_list}) VALUES ({placeholders})",
        values
    )
conn.commit()
if access_skipped:
    print(f"[WARNING] Skipped {access_skipped} duplicate ACCESSCATALYST rows.")

# 6. USERPROFILE_FIELD
print('Inserting USERPROFILE_FIELD...')
fields = load_json('userprofile_field_data.json')
userprofile_field_cols = [col for col in table_columns_dict['USERPROFILE_FIELD'] if col.upper() != 'USERPROFILE_FIELD_ID']  # ekskluder identity
skipped_rows = []
inserted_count = 0
field_seen = set()
for idx, row in enumerate(fields):
    # Only use keys that are in the SQL schema
    filtered_row = {col: row.get(col) for col in userprofile_field_cols}
    # Warn if there are extra fields in the JSON row
    extra_fields = set(row.keys()) - set(userprofile_field_cols)
    if extra_fields:
        print(f"[WARNING] Row {idx} contains extra fields not in schema: {extra_fields}")
    # Validation: FIELD_NAME must not be None or empty
    fieldname_val = filtered_row.get('FIELD_NAME')
    if fieldname_val is None or (isinstance(fieldname_val, str) and not fieldname_val.strip()):
        skipped_rows.append({'index': idx, 'row': row})
        continue
    # Deduplication: use tuple of all non-identity columns as key (from schema only)
    key = tuple(filtered_row.get(col) for col in userprofile_field_cols)
    if key in field_seen:
        continue
    field_seen.add(key)
    values = [filtered_row.get(col) for col in userprofile_field_cols]
    placeholders = ', '.join(['?'] * len(userprofile_field_cols))
    col_list = ', '.join(userprofile_field_cols)
    cursor.execute(
        f"INSERT INTO USERPROFILE_FIELD ({col_list}) VALUES ({placeholders})",
        values
    )
    inserted_count += 1
conn.commit()
if skipped_rows:
    print(f"[WARNING] Skipped {len(skipped_rows)} USERPROFILE_FIELD rows due to missing FIELD_NAME:")
    for item in skipped_rows:
        print(f"  Row {item['index']}: {item['row']}")
else:
    print(f"Inserted {inserted_count} USERPROFILE_FIELD rows.")

# 7. USERPROFILE
print('Inserting USERPROFILE...')
profiles = load_json('userprofile_data.json')
userprofile_cols = table_columns_dict['USERPROFILE']
# Load valid ACCESSCATALYST IDs from accesscatalyst_data.json
access_rows = load_json('accesscatalyst_data.json')
valid_accesscatalyst_ids = set(row['ACCESSCATALYST'] for row in access_rows)
skipped_profiles = []
profile_seen = set()
for idx, row in enumerate(profiles):
    ac_val = row.get('ACCESSCATALYST')
    # Stricter validation: must be int (not bool/float/str), not None, and in valid set
    if ac_val is None or type(ac_val) is not int or ac_val not in valid_accesscatalyst_ids:
        if len(skipped_profiles) < 5:
            print(f"[ERROR] Skipping USERPROFILE row {idx}: Invalid ACCESSCATALYST value: {ac_val} (type: {type(ac_val)}) | Row: {row}")
        elif len(skipped_profiles) == 5:
            print("[ERROR] ... (more skipped rows, see summary below)")
        skipped_profiles.append({'index': idx, 'row': row})
        continue
    # Deduplication: use only the primary key columns as key
    pk_key = (row.get('USERFIELD'), row.get('ACCESSCATALYST'))
    if pk_key in profile_seen:
        continue
    profile_seen.add(pk_key)
    values = [row.get(col) for col in userprofile_cols]
    placeholders = ', '.join(['?'] * len(userprofile_cols))
    col_list = ', '.join(userprofile_cols)
    try:
        cursor.execute(
            f"INSERT INTO USERPROFILE ({col_list}) VALUES ({placeholders})",
            values
        )
    except Exception as e:
        if len(skipped_profiles) < 5:
            print(f"[EXCEPTION] Insert failed for USERPROFILE row {idx}: {row}\n  Error: {e}")
        elif len(skipped_profiles) == 5:
            print("[EXCEPTION] ... (more skipped rows, see summary below)")
        skipped_profiles.append({'index': idx, 'row': row, 'error': str(e)})
conn.commit()
if skipped_profiles:
    print(f"[WARNING] Skipped {len(skipped_profiles)} USERPROFILE rows due to invalid ACCESSCATALYST, duplicates, or insert errors. Showing first 5 only.")
else:
    print(f"Inserted {len(profile_seen)} USERPROFILE rows.")

# 8. USERPROFILE_HISTORY
print('Inserting USERPROFILE_HISTORY...')
history = load_json('userprofile_history_data.json')
userprofile_history_cols = [col for col in table_columns_dict['USERPROFILE_HISTORY'] if col.upper() not in ('USERPROFILE_HISTORY_ID', 'RECORD_NUMBER')]  # ekskluder identity
error_messages = {}
error_count = 0
for idx, row in enumerate(history):
    # Map old field name to new if present
    if 'USERPROFILE_FIELD_ID' not in row and 'USERPROFILE_FIELD' in row:
        row['USERPROFILE_FIELD_ID'] = row['USERPROFILE_FIELD']
    # Remove identity columns if present
    if 'USERPROFILE_HISTORY_ID' in row:
        del row['USERPROFILE_HISTORY_ID']
    if 'RECORD_NUMBER' in row:
        del row['RECORD_NUMBER']
    try:
        values = [row.get(col) for col in userprofile_history_cols]
        placeholders = ', '.join(['?'] * len(userprofile_history_cols))
        col_list = ', '.join(userprofile_history_cols)
        cursor.execute(
            f"INSERT INTO USERPROFILE_HISTORY ({col_list}) VALUES ({placeholders})",
            values
        )
    except Exception as e:
        msg = str(e)
        if msg not in error_messages:
            if len(error_messages) < 5:
                print(f"[ERROR] USERPROFILE_HISTORY row {idx} failed: {e}\n  Row: {row}")
            elif len(error_messages) == 5:
                print("[ERROR] ... (more unique error types, see summary below)")
        error_messages[msg] = error_messages.get(msg, 0) + 1
        error_count += 1
conn.commit()
if error_messages:
    print(f"[SUMMARY] {error_count} USERPROFILE_HISTORY rows failed to insert. Unique error types:")
    for msg, count in error_messages.items():
        print(f"  {count} rows: {msg}")

print('✅ All data loaded successfully!')
cursor.close()
conn.close()
