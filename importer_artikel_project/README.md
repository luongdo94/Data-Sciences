# Article Importer

A tool for importing article data from Microsoft Access database to CSV files.

## Requirements

- Python 3.7+
- Microsoft Access ODBC Driver
- Python libraries (see `requirements.txt`)

## Installation

1. Create a virtual environment (recommended):
   ```bash
   python -m venv venv
   .\venv\Scripts\activate
   ```

2. Install required libraries:
   ```bash
   pip install -r requirements.txt
   ```

3. Copy your Access database file to the `data` directory or update the path in `src/config.py`

## Usage

Run the program:
```bash
python -m src.main
```

The resulting CSV files will be saved in the `data/output/` directory.

## Project Structure

```
importer_artikel_project/
├── data/                   # Data directory
│   └── output/             # Output CSV files
├── sql/                    # SQL files
├── src/                    # Source code
│   ├── __init__.py         # Package initialization
│   ├── config.py           # Configuration
│   ├── database.py         # Database connection
│   └── main.py             # Program entry point
├── requirements.txt        # Dependencies
└── README.md               # Documentation
```

## Notes

- The program will create CSV files with UTF-8 encoding and semicolon (;) as the delimiter
- Log file is saved in the root directory as `importer.log`
