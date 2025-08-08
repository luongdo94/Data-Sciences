from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
MDB_FILE = DATA_DIR / "db_Artikel_Export2.mdb"
MDB_DATA = DATA_DIR / "DATEN.MDB"
OUTPUT_DIR = DATA_DIR / "output"
SQL_DIR = BASE_DIR / "sql"

for directory in [OUTPUT_DIR, SQL_DIR, DATA_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

def get_connection_string(mdb_path):
    """Create a connection string for any .mdb or .accdb file."""
    return f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={mdb_path};"

# Default connection strings
CONN_STR = get_connection_string(MDB_FILE)
CONN_STR_DATA = get_connection_string(MDB_DATA)
