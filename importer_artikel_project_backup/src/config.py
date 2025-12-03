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
    # Try with the newer driver first, fall back to older if needed
    drivers = [
        "Microsoft Access Driver (*.mdb, *.accdb)",
        "Microsoft Access Driver (*.mdb)",
        "Microsoft Access Driver (*.mdb, *.accdb)"
    ]
    
    import pyodbc
    available_drivers = [d for d in drivers if any(d in x for x in pyodbc.drivers())]
    
    if not available_drivers:
        raise RuntimeError("No suitable Microsoft Access ODBC driver found. Please install 'Microsoft Access Database Engine 2016 Redistributable'")
        
    return f"DRIVER={{{available_drivers[0]}}};DBQ={mdb_path};"

# Default connection strings
CONN_STR = get_connection_string(MDB_FILE)
CONN_STR_DATA = get_connection_string(MDB_DATA)
