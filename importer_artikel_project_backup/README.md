# Article Importer

A comprehensive tool for importing, comparing, and managing article data between different systems. The application supports importing from Microsoft Access databases, comparing data with ERP systems, and generating import-ready CSV files.

## 🚀 Latest Updates

- **Stock Management**:
  - Added `import_stock_lager` function for warehouse stock management
  - Support for filtering stock by areas with case-insensitive matching
  - Generate stock-related CSV files (STOCK - Lager.csv, STOCKARTICLE_PRIORITY_AREA, etc.)
  - Handle large datasets with optimized SQL queries

- **Data Processing Improvements**:
  - Enhanced error handling for database operations
  - Added support for case-insensitive area code matching
  - Improved handling of special characters in product data
  - Optimized memory usage for large datasets

## Features

- **Data Import/Export**:
  - Import/export SKUs, articles, classifications, keywords, and pricing information
  - Support for multiple data sources (MS Access, SQL Server, CSV, Excel)
  - Generate ERP-ready CSV files with proper formatting
  - Export stock and warehouse management data

- **Data Processing**:
  - Automatic data validation and cleaning
  - Handle special characters and encodings (UTF-8, Windows-1252)
  - Process color information and variants
  - Manage price calculations and conversions

- **Data Comparison**:
  - Compare data between different sources (Access, ERP, etc.)
  - Generate detailed comparison reports
  - Identify and highlight differences in SKUs, EANs, and article data

- **Modular Design**:
  - Separate modules for different data types (SKUs, articles, prices, etc.)
  - Configurable through environment variables
  - Comprehensive error handling and logging

## Requirements

- Python 3.7+
- Microsoft Access ODBC Driver
- SQL Server ODBC Driver
- Required Python packages (see `requirements.txt`)
  - pandas
  - pyodbc
  - python-dotenv
  - openpyxl (for Excel support)
  - tqdm (for progress bars)

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd importer_artikel_project
   ```

2. Create and activate a virtual environment (recommended):
   ```bash
   python -m venv venv
   # On Windows:
   .\venv\Scripts\activate
   # On macOS/Linux:
   # source venv/bin/activate
   ```

3. Install required libraries:
   ```bash
   pip install -r requirements.txt
   ```

4. Set up environment variables:
   - Copy `.env.example` to `.env`
   - Update the database connection strings and other settings
   pip install -r requirements.txt
   ```

4. Set up environment variables:
   - Copy `.env.example` to `.env`
   - Update the database connection details in `.env`

## Configuration

Create a `.env` file in the project root with the following variables:

```
# Database Configuration
SQL_SERVER=your_sql_server
SQL_DATABASE=your_database
SQL_USERNAME=your_username
SQL_PASSWORD=your_password

# File Paths
DATA_DIR=./data
OUTPUT_DIR=./data/output

# File Encodings (optional)
DEFAULT_ENCODING=utf-8-sig
CSV_DELIMITER=;
```

## Usage

### Data Comparison

To compare data between systems and generate difference reports:

```bash
python run_comparison_standalone.py
```
python -m src.main

# Or run specific functions directly
python -c "from src.simple_article_importer import import_stock_lager; import_stock_lager()"

# Run with specific area filters
python -c "from src.simple_article_importer import import_stock_lager; import_stock_lager(diff_areas=['area1', 'area2'])"

Or run individual import functions:

```python
from src.simple_article_importer import import_sku_basis, import_artikel_basis

# Import SKU data
sku_file = import_sku_basis()

# Import article data
artikel_file = import_artikel_basis()
```

### Available Import/Export Functions

#### SKU Operations

- **`import_sku_basis()`**  
  Imports basic SKU data including article information and basic attributes.  
  - **Input**: Queries database for SKU data filtered by diff list  
  - **Output**: `sku_basis.csv` with columns: aid, company, automatic_batch_numbering_pattern, batch_management, etc.  
  - **Handling**: Sets default values for required fields, handles data formatting  

- **`import_sku_classification()`**  
  Imports SKU classification data with product attributes and features.  
  - **Input**: Queries database for SKU classifications  
  - **Output**: `sku_classification.csv` with feature mappings  
  - **Features**: Handles Grammatur, Oeko_MadeInGreen, Partnerlook, and other product attributes  

- **`import_sku_keyword()`**  
  Imports and processes SKU keywords for search functionality.  
  - **Input**: Queries database for SKU keywords  
  - **Output**: `sku_keyword.csv` with keyword mappings  
  - **Processing**: Handles special characters and formatting  

- **`import_sku_text()`**  
  Imports various text descriptions for SKUs including webshop, catalog, and invoice texts.  
  - **Input**: Queries database for SKU text data  
  - **Output**: Multiple CSV files (e.g., `sku_text_webshoptext.csv`, `sku_text_artikeltext.csv`)  
  - **Text Types**: Webshop text, article text, catalog text, sales text, invoice text, care instructions  

- **`import_sku_variant()`**  
  Imports SKU variant data including size, color, and country of origin.  
  - **Input**: Queries database for SKU variants  
  - **Output**: `VARIANT_IMPORT - SKU-Variantenverknüpfung Import.csv`  
  - **Attributes**: Size, color, country of origin with proper encoding handling  

- **`import_sku_EAN()`**  
  Imports EAN (barcode) data for SKUs.  
  - **Input**: Queries database for EAN codes  
  - **Output**: `sku_ean.csv` with EAN mappings  
  - **Validation**: Checks for valid EAN formats  

- **`import_sku_gebinde()`**  
  Imports and processes SKU packaging data including dimensions and packaging units.  
  - **Input**: Queries database for packaging data  
  - **Output**:  
    - `artikel_gebinde.csv`: Standard packaging data  
    - `ARTICLE_PACKAGING_IMPORT - SKU-Gebindedaten_VE.csv`: Packaging data with Verpackungseinheit  
  - **Processing**:  
    - Converts dimensions from cm to mm  
    - Formats decimal separators (comma for German locale)  
    - Handles packaging unit formatting (adds 'er' suffix)  
    - Sets default values for missing dimensions

#### Article Operations

- **`import_artikel_basis()`**  
  Imports basic article data including core attributes.  
  - **Input**: Queries database for article data  
  - **Output**: `artikel_basis.csv` with article details  
  - **Handling**: Sets default values, handles special characters  

- **`import_artikel_classification()`**  
  Imports article classification data with detailed product attributes.  
  - **Input**: Queries database for article classifications  
  - **Output**: `artikel_classification.csv`  
  - **Features**: Includes product group, material, composition, and various product attributes  

- **`import_artikel_zuordnung()`**  
  Handles article assignments and mappings.  
  - **Input**: Queries database for article assignments  
  - **Output**: `artikel_zuordnung.csv`  
  - **Processing**: Manages article relationships and hierarchies  

- **`import_artikel_keyword()`**  
  Imports and processes article keywords.  
  - **Input**: Queries database for article keywords  
  - **Output**: `artikel_keyword.csv`  
  - **Features**: Handles keyword mappings and associations  

- **`import_artikel_text()`**  
  Imports various text descriptions for articles.  
  - **Input**: Queries database for article texts  
  - **Output**: Multiple CSV files for different text types  
  - **Text Types**: Description, technical details, marketing texts  

- **`import_artikel_variant()`**  
  Imports article variant data.  
  - **Input**: Queries database for article variants  
  - **Output**: `variant_export.csv`  
  - **Attributes**: Size, color, and other variant-specific data

#### Price Operations

- **`import_artikel_pricestaffeln()`**  
  Imports price scales for articles.  
  - **Input**: Queries database for price scales  
  - **Output**: `artikel_pricestaffeln.csv`  
  - **Features**: Handles different price tiers and quantity breaks  

- **`import_artikel_preisstufe_3_7()`**  
  Imports specific price levels (3-7) for articles.  
  - **Input**: Queries database for price levels  
  - **Output**: `artikel_preisstufe_3_7.csv`  
  - **Processing**: Converts and formats price data  

- **`import_artikel_basicprice()`**  
  Imports base prices for articles.  
  - **Input**: Queries database for base prices  
  - **Output**: `artikel_basicprice.csv`  
  - **Handling**: Manages currency and price formatting

#### Order Operations

- **`import_order()`**  
  Imports basic order data.  
  - **Input**: Queries database for order information  
  - **Output**: `order_import.csv`  
  - **Data**: Order headers and basic information  

- **`import_order_are_15()`**  
  Handles special order type ARE 15.  
  - **Input**: Queries database for ARE 15 orders  
  - **Output**: `order_are_15_import.csv`  
  - **Special Handling**: Specific formatting for ARE 15 order type  

- **`import_order_pos()`**  
  Imports order position data.  
  - **Input**: Queries database for order items  
  - **Output**: `order_pos_import.csv`  
  - **Details**: Includes article numbers, quantities, prices  

- **`import_order_pos_are_15()`**  
  Handles order positions for ARE 15 orders.  
  - **Input**: Queries database for ARE 15 order items  
  - **Output**: `order_pos_are_15_import.csv`  
  - **Special Handling**: Specific formatting for ARE 15 positions  

- **`import_order_classification()`**  
  Imports order classification data.  
  - **Input**: Queries database for order classifications  
  - **Output**: `order_classification_import.csv`  
  - **Features**: Manages order categories and types

## Project Structure

project-root/
├── data/                           # Data files (input/output)
│   ├── input/                      # Input data files
│   └── output/                     # Generated output files
│       ├── STOCK - Lager.csv       # Main stock data
│       ├── STOCKARTICLE_PRIORITY_AREA - Prioritätsplätze.csv  # Priority areas
│       └── Stockarticle_LocDef-Stellplatzdefinitionen.csv     # Location definitions
├── sql/                            # SQL query files
│   ├── get_lager.sql               # Stock data query
│   └── get_skus.sql                # SKU data query
├── src/                            # Source code
│   ├── database.py                 # Database connection and utilities
│   ├── fix_column_name.py          # Column name utilities
│   ├── main.py                     # Main application entry point
│   ├── simple_article_importer.py  # Core import/export functions
│   └── sku_color_processor.py      # SKU color processing
├── .env.example                    # Example environment variables
├── README.md                # This file
├── requirements.txt         # Python dependencies
└── run_comparison_standalone.py  # Standalone comparison script
importer_artikel_project/
├── data/                    # Data files and outputs
│   ├── output/              # Generated output files
│   │   └── comparison_results/  # Comparison reports
│   └── input/               # Input data files
├── sql/                     # SQL query files
│   ├── get_article_*.sql    # Article-related queries
│   ├── get_sku_*.sql        # SKU-related queries
│   └── get_*.sql            # Other queries
├── src/                     # Source code
│   ├── __init__.py
│   ├── comparison.py        # Data comparison functionality
│   ├── config.py            # Configuration settings
│   ├── database.py          # Database connection and query handling
│   └── simple_article_importer.py  # Main import functionality
├── .env.example             # Example environment variables
├── requirements.txt         # Python dependencies
├── run_comparison_standalone.py  # Comparison script
└── README.md                # This file
```

## Data Formats

### Input Files

Place the following files in the `data/` directory:
- `comparison.csv` - For SKU comparison
- `comparison_artbasis.csv` - For article basics comparison
- `comparison_EAN.csv` - For EAN comparison

### Output Files

Output files are generated in the `data/output/` directory with the following naming convention:
- `ARTICLE - *.csv`: Article-related data
- `SKU - *.csv`: SKU-related data
- `PRICELIST - *.csv`: Pricing information
- `comparison_results/*_differences.csv`: Comparison results

## Error Handling and Logging

The application includes comprehensive error handling and logging:
- Errors are logged to `importer.log`
- Detailed error messages are displayed in the console
- Data validation is performed before processing

## License

This project is proprietary software. All rights reserved.

## Support

For support or questions, please contact the development team.
