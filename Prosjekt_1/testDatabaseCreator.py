import json
import os
import pyodbc

def read_json(path):
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def sql_type(col):
    t = col['type'].lower()
    if t == 'int':
        return 'INT'
    if t == 'date':
        return 'DATE'
    if t == 'nvarchar':
        return f"NVARCHAR({col.get('len', 255)})"
    if t == 'text':
        return 'NVARCHAR(MAX)'
    return 'NVARCHAR(255)'

def get_connection(server=None, database=None, username=None, password=None, driver=None):
    # Allow env vars to override or provide defaults
    server = server or os.getenv("DB_SERVER", "DESKTOP-R9S4CFK")
    database = database or os.getenv("DB_NAME", "TestDatabase")
    username = username or os.getenv("DB_UID", "sa")
    password = password or os.getenv("DB_PWD", "(catalystone123)")
    driver = driver or os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
    # Use Trusted_Connection if no username/password, else SQL auth
    if username and password:
        connection_string = f"DRIVER={{{driver}}};SERVER={server};DATABASE=master;UID={username};PWD={password};TrustServerCertificate=yes;Encrypt=no;"
    else:
        connection_string = f"DRIVER={{{driver}}};SERVER={server};DATABASE=master;Trusted_Connection=yes;TrustServerCertificate=yes;Encrypt=no;"
    return pyodbc.connect(connection_string, autocommit=True)

def main():
    schema = read_json('metaData.json')
    data = read_json('cleanDataQcChecked.json')
    if not schema or 'tables' not in schema or not schema['tables']:
        print('No tables found in metaData.json')
        return
    table = schema['tables'][0]
    table_name = table['table_name']
    columns = table['columns']
    col_defs = []
    for col in columns:
        col_defs.append(f"[{col['name']}] {sql_type(col)}")
    create_stmt = f"CREATE TABLE {table_name} (\n  {',\n  '.join(col_defs)}\n);"

    with get_connection() as conn:
        cursor = conn.cursor()
        # Create database if not exists
        cursor.execute(f"IF DB_ID('TestDatabase') IS NULL CREATE DATABASE TestDatabase;")
        cursor.execute(f"USE TestDatabase;")
        # Drop table if exists
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name};")
        # Create table
        cursor.execute(create_stmt)
        # Insert data
        for row in data:
            fields = ', '.join(f"[{k}]" for k in row.keys())
            placeholders = ', '.join('?' for _ in row.values())
            values = tuple(row.values())
            insert_sql = f"INSERT INTO {table_name} ({fields}) VALUES ({placeholders});"
            cursor.execute(insert_sql, values)
    print(f"Test database and table '{table_name}' created and populated in SQL Server.")

if __name__ == "__main__":
    main()
