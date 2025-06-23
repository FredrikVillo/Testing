import pyodbc

def test_db_connection_and_create_table():
    print("Script started")
    conn_str = (
        'DRIVER={ODBC Driver 18 for SQL Server};'
        'SERVER=DESKTOP-R9S4CFK;'
        'DATABASE=HR_Synthetic;'
        'UID=sa;'
        'PWD=(catalystone123);'
        'TrustServerCertificate=yes;'
        'Encrypt=no;'
    )
    try:
        conn = pyodbc.connect(conn_str)
        print("✅ Connection successful!")
        cursor = conn.cursor()
        # Try to create the table if it doesn't exist
        create_table_sql = '''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='TestTable' AND xtype='U')
        CREATE TABLE TestTable (
            ID INT PRIMARY KEY IDENTITY(1,1),
            Name NVARCHAR(100),
            CreatedAt DATETIME DEFAULT GETDATE()
        )
        '''
        cursor.execute(create_table_sql)
        conn.commit()
        print("✅ TestTable created or already exists.")
        cursor.close()
        conn.close()
    except Exception as e:
        print("❌ Connection or table creation failed:", e)

if __name__ == "__main__":
    test_db_connection_and_create_table()
