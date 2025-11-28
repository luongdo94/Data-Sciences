# Article Importer Project

Welcome! This tool helps you manage and convert product data from Microsoft Access databases into clean CSV files, ready for use in other systems like your online shop or ERP.

It handles everything from **Products (Articles)** and **Variants (SKUs)** to **Prices**, **Stock Levels**, and **Orders**.

---

## 🚀 Quick Start

If you have everything set up, here is how to run the tool:

1.  **Run the Full Import**:
    ```bash
    python -m src.main
    ```
    *This processes all data (SKUs, Articles, Prices, Orders) and saves it to `data/output`.*

2.  **Run Data Comparison**:
    ```bash
    python run_comparison_standalone.py
    ```
    *This compares your local data with external sources to find differences.*

---

## 📋 Requirements

Before you start, make sure you have:

1.  **Python 3.7+**: [Download here](https://www.python.org/downloads/)
2.  **Microsoft Access Database Engine**: Required to read `.mdb` files. [Download here](https://www.microsoft.com/en-us/download/details.aspx?id=54920)
3.  **ODBC Drivers**: Ensure you have the "Microsoft Access Driver (*.mdb, *.accdb)" installed.

---

## 🛠️ Installation & Setup

Follow these steps to set up the project on your computer:

### 1. Clone the Project
Open your terminal (Command Prompt or PowerShell) and run:
```bash
git clone <repository-url>
cd importer_artikel_project
```

### 2. Set Up Python Environment
It's best to use a virtual environment to keep things clean:
```bash
# Create the environment
python -m venv venv

# Activate it (Windows)
.\venv\Scripts\activate
```

### 3. Install Dependencies
Install the required Python libraries:
```bash
pip install -r requirements.txt
```

### 4. Configuration
The tool needs to know where your database and files are.
1.  Find the file named `.env.example`.
2.  Copy it and rename it to `.env`.
3.  Open `.env` in a text editor (like Notepad) and check the paths.
    *   `DATA_DIR`: Where your input files live.
    *   `OUTPUT_DIR`: Where the new CSV files will be saved.
    *   `SQL_SERVER`: Your database connection details (if used).

---

## 📖 How to Use

### 1. Full Import (Recommended)
The easiest way to use the tool is to run the main script. It runs all import functions in the correct order.
```bash
python -m src.main
```

### 2. Comparison Tool
If you have comparison files (like `comparison.csv`) in your `data/` folder, you can run this script to see what has changed:
```bash
python run_comparison_standalone.py
```
This script will create lists of differences between the Megaliste data and the data in the ERP.
*Results will be saved in `data/output/comparison_results`.*

### 3. Running Specific Parts
Advanced users can run specific functions directly from the command line:

**Example: Import only Stock Data**
```bash
python -c "from src.simple_article_importer import import_stock_lager; import_stock_lager()"
```

**Example: Import Stock for specific areas**
```bash
python -c "from src.simple_article_importer import import_stock_lager; import_stock_lager(diff_areas=['area1', 'area2'])"
```

---

## 🏗️ Project Structure

Here is a quick look at how the files are organized:

```text
importer_artikel_project/
├── data/                       # Where your data files live
│   ├── input/                  # Put your source files here
│   └── output/                 # The tool saves the generated CSV files here
├── sql/                        # SQL queries used to talk to the database
├── src/                        # The main code of the application
│   ├── main.py                 # The main script that runs the full import
│   ├── simple_article_importer.py  # Contains the logic for importing articles, SKUs, etc.
│   ├── comparison.py           # Logic for comparing data
│   └── config.py               # Settings and paths
├── .env                        # Your configuration file (passwords, paths)
├── requirements.txt            # List of required Python libraries
└── run_comparison_standalone.py  # Script for running data comparisons
```

---

## 📂 Output Files

After running the tool, check the `data/output` folder. You will see files like:

| Category | File Name Pattern | Description |
|----------|-------------------|-------------|
| **SKUs** | `SKU - *.csv` | Product variants, EANs, and keywords. |
| **Articles** | `ARTICLE - *.csv` | Main product details and descriptions. |
| **Prices** | `PRICELIST - *.csv` | Base prices, price scales, and validity dates. |
| **Stock** | `STOCK - *.csv` | Current warehouse stock levels. | Stock data come from fet_user.V_STOCK/fet_user.V_Promodoro_STOCK
| **Orders** | `CONTRACT - *.csv` | Order and contract information. |
    * Business_Partner data come from fet_user.FET_BUSINESSPARTNER
 

---

## ❓ Troubleshooting

**Error: "ODBC Driver not found"**
*   **Cause**: You are missing the Microsoft Access Database Engine.
*   **Fix**: Download and install the "Microsoft Access Database Engine 2016 Redistributable" (choose the 32-bit or 64-bit version matching your Python installation).

**Error: "File not found"**
*   **Cause**: The tool can't find your input files or database.
*   **Fix**: Check your `.env` file. Make sure `DATA_DIR` points to the correct folder and that your `.mdb` file is actually there.

---
*For further support, please contact the development team.*
