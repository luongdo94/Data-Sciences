from pathlib import Path

# Project Base Directory
BASE_DIR = Path(__file__).resolve().parent.parent

# Database file path
# TODO: Update this path to your actual .mdb file
MDB_FILE = r"C:\Users\gia.luongdo\Python\python_project\data\DATEN.MDB"

# Data directories
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = DATA_DIR / "output"

# SQL directory
SQL_DIR = BASE_DIR / "sql"

# Ensure output directory exists
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
