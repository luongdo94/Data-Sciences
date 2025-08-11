# My Python Project

This project extracts data from an MDB database, transforms it, and saves it as a CSV file.

## Project Structure

```
python_project/
├── data/
│   └── output/         # Stores the output CSV files
├── sql/                # Stores .sql query files
│   └── get_data.sql
├── src/                # Contains all Python source code
│   ├── config.py       # Configuration like file paths
│   └── main.py         # Main script to run the process
├── requirements.txt    # Project dependencies
└── README.md           # This file
```

## How to Run

1.  **Update Configuration:** Open `src/config.py` and set the `MDB_FILE` path to your database file.
2.  **Edit SQL:** Modify `sql/get_data.sql` with your actual query.
3.  **Install Dependencies:** `pip install -r requirements.txt` (you will need to add `pyodbc` and `pandas` to this file).
4.  **Run the script:** `python src/main.py`

