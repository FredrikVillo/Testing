import pyodbc
import os

def get_connection(server=None, database=None, username=None, password=None, driver=None):
    # Allow env vars to override or provide defaults
    server = server or os.getenv("DB_SERVER", "localhost")
    database = database or os.getenv("DB_NAME", "")
    username = username or os.getenv("DB_UID", "sa")
    password = password or os.getenv("DB_PWD", "")
    driver = driver or os.getenv("DB_DRIVER", "ODBC Driver 18 for SQL Server")
    connection_string = f"""
        DRIVER={{{driver}}};
        SERVER={server};
        DATABASE={database};
        UID={username};
        PWD={password};
        TrustServerCertificate=yes;
        Encrypt=no;
    """
    return pyodbc.connect(connection_string)

def fetch_schema(cursor):
    query = """
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        c.name AS column_name,
        ty.name AS data_type,
        c.max_length,
        c.is_nullable,
        c.is_identity
    FROM sys.schemas s
    JOIN sys.tables t ON t.schema_id = s.schema_id
    JOIN sys.columns c ON c.object_id = t.object_id
    JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    ORDER BY s.name, t.name, c.column_id;
    """
    cursor.execute(query)
    return cursor.fetchall()

def fetch_schema_with_keys(cursor):
    query = """
    SELECT
        s.name AS schema_name,
        t.name AS table_name,
        c.name AS column_name,
        ty.name AS data_type,
        c.max_length,
        c.is_nullable,
        c.is_identity,
        CASE WHEN pk.column_id IS NOT NULL THEN 1 ELSE 0 END AS is_pk,
        fk_info.fk_name,
        fk_info.ref_schema,
        fk_info.ref_table,
        fk_info.ref_column
    FROM sys.schemas s
    JOIN sys.tables t ON t.schema_id = s.schema_id
    JOIN sys.columns c ON c.object_id = t.object_id
    JOIN sys.types ty ON c.user_type_id = ty.user_type_id
    LEFT JOIN (
        SELECT ic.object_id, ic.column_id
        FROM sys.indexes i
        JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        WHERE i.is_primary_key = 1
    ) pk ON c.object_id = pk.object_id AND c.column_id = pk.column_id
    LEFT JOIN (
        SELECT 
            fkc.parent_object_id, fkc.parent_column_id,
            fk.name AS fk_name,
            sch2.name AS ref_schema,
            t2.name AS ref_table,
            c2.name AS ref_column
        FROM sys.foreign_key_columns fkc
        JOIN sys.foreign_keys fk ON fkc.constraint_object_id = fk.object_id
        JOIN sys.tables t2 ON fkc.referenced_object_id = t2.object_id
        JOIN sys.schemas sch2 ON t2.schema_id = sch2.schema_id
        JOIN sys.columns c2 ON fkc.referenced_object_id = c2.object_id AND fkc.referenced_column_id = c2.column_id
    ) fk_info ON c.object_id = fk_info.parent_object_id AND c.column_id = fk_info.parent_column_id
    ORDER BY s.name, t.name, c.column_id;
    """
    cursor.execute(query)
    return cursor.fetchall()

def write_schema_to_file(rows, output_path):
    with open(output_path, 'w', encoding='utf-8') as f:
        current_table = None
        for row in rows:
            (schema, table, column, dtype, maxlen, nullable, identity, is_pk,
             fk_name, ref_schema, ref_table, ref_column) = row
            full_table = f"{schema}.{table}"
            if full_table != current_table:
                if current_table is not None:
                    f.write("\n")
                f.write(f"Table: {full_table}\n")
                f.write("-" * 40 + "\n")
                current_table = full_table
            col_info = f"  {column} ({dtype}, len={maxlen}, nullable={nullable}, identity={identity}"
            if is_pk:
                col_info += ", PK"
            if fk_name:
                col_info += f", FK→{ref_schema}.{ref_table}.{ref_column}"
            col_info += ")\n"
            f.write(col_info)

if __name__ == "__main__":
    # Use env vars if present, else fallback to hardcoded/test values
    server = os.getenv("DB_SERVER", "DESKTOP-R9S4CFK")
    database = os.getenv("DB_NAME", "analyticsroutinedev")
    username = os.getenv("DB_UID", "sa")
    password = os.getenv("DB_PWD", "(catalystone123)")
    output_file = "schema_output.txt"

    conn = get_connection(server, database, username, password)
    cursor = conn.cursor()
    schema_rows = fetch_schema_with_keys(cursor)
    write_schema_to_file(schema_rows, output_file)
    print(f"Schema with PK/FK info written to {output_file}")
