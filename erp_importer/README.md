# ERP Importer

A Python application for importing and processing data from ERP systems.

## Features

- Export article data to CSV files
- Filter data based on article IDs from a CSV file
- Support for different export formats and encodings
- Configurable logging
- Modular architecture for easy maintenance and extension

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd erp_importer
   ```

2. Create a virtual environment (recommended):
   ```bash
   python -m venv venv
   .\venv\Scripts\activate  # On Windows
   source venv/bin/activate  # On Unix or MacOS
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Basic Usage

```bash
# Run with default settings
python -m erp_importer.main

# Specify a custom filter CSV
python -m erp_importer.main --filter-csv path/to/your/aid_list.csv

# Specify an output directory
python -m erp_importer.main --output-dir path/to/output/directory
```

### Configuration

You can configure the application by creating a `.env` file in the project root:

```ini
# Database configuration
DB_PATH=C:\\path\\to\\your\\database.mdb

# Logging configuration
LOG_LEVEL=INFO
LOG_FILE=erp_importer.log

# Output configuration
DEFAULT_OUTPUT_DIR=output
```

## Project Structure

```
erp_importer/
├── config/               # Configuration files
│   ├── __init__.py
│   └── database.py       # Database connection settings
├── models/              # Data models
├── repositories/        # Data access layer
│   ├── __init__.py
│   ├── base_repository.py
│   ├── article_repository.py
│   └── sql_queries.py
├── services/           # Business logic
│   ├── __init__.py
│   └── article_service.py
├── utils/              # Utility functions
│   ├── __init__.py
│   └── logger.py
├── main.py            # Main application entry point
├── requirements.txt   # Project dependencies
└── README.md          # This file
```

## Development

### Running Tests

```bash
pytest tests/
```

### Code Style

This project uses:
- Black for code formatting
- Flake8 for linting
- isort for import sorting
- mypy for type checking

Run all code quality checks:

```bash
black .
flake8
iSort .
mypy .
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
