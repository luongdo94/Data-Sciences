import pandas as pd
from sqlalchemy import create_engine
import os

# Path to your MDB file
MDB_FILE_PATH = r'"C:\Users\gia.luongdo\Desktop\ERP-Importer\db_Artikel_Export2.mdb"'

# Connection string for Access MDB (using Access ODBC driver)
conn_str = f"access+pyodbc:///?odbc_connect={{" \
    "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};" \
    f"DBQ={MDB_FILE_PATH};" \
    "}}"

# Create SQLAlchemy engine
engine = create_engine(conn_str)

# List all tables in the MDB file
table_names = engine.table_names()
print('Tables found:', table_names)

# Read a table into a DataFrame (replace 'YourTable' with actual table name)
table_name = table_names[0] if table_names else None
if table_name:
    df = pd.read_sql_table(table_name, engine)
    print(df.head())
else:
    print('No tables found in the MDB file.')