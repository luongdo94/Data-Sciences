import os
import sys
import io
import pandas as pd
import pyodbc
from pathlib import Path

# Configure stdout to support Unicode in Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Add root directory to path to import modules
sys.path.append(str(Path(__file__).parent.parent))

from src.config import DATA_DIR, MDB_DATA
from src.database import read_csv_file

# Read data from CSV file
csv_path = os.path.join(DATA_DIR, "output", "comparison_results", "sku_differences.csv")
try:
    df = read_csv_file(csv_path)
    print(f"Successfully read file: {csv_path}")
    
    # Print all columns to verify
    print("\nColumns in file:")
    for col in df.columns:
        print(f"- {col}")
        
    # Check if any column contains 'aid_ew', 'aid', or 'sku'
    sku_columns = [col for col in df.columns if 'aid' in col.lower() or 'sku' in col.lower()]
    if not sku_columns:
        print("\nCould not find any column containing SKU. Please verify the data.")
        sys.exit(1)
        
    # Use the first column containing 'aid' or 'sku' as the source column
    source_column = sku_columns[0]
    print(f"\nUsing column '{source_column}' to extract color codes")
    
except Exception as e:
    print(f"Error reading CSV file: {e}")
    sys.exit(1)

def extract_color(sku):
    """Extract color code from SKU string."""
    if not isinstance(sku, str):
        return None
    
    parts = sku.split('-')
    if len(parts) >= 3:
        color_part = parts[1].split('/')[0].strip()
        return color_part.lower()
    return None

# Extract color code from source column
print("\nExtracting color codes from data...")
df['color'] = df[source_column].apply(extract_color)

# Connect to the database and retrieve color list from ERP
print("\nConnecting to the database...")
conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={MDB_DATA};"
try:
    with pyodbc.connect(conn_str) as conn:
        print("Connected successfully!")
        color_query = """
            SELECT a.ERP_Farben
            FROM tArtFarben AS a
            WHERE a.isExport = TRUE
        """
        print("Retrieving color list from ERP...")
        erp_colors_df = pd.read_sql(color_query, conn)
        # Convert ERP_Farben column to a lowercase list
        erp_colors = [str(color).lower() for color in erp_colors_df['ERP_Farben']]
        print(f"Loaded {len(erp_colors)} color codes from ERP")

except Exception as e:
    print(f"Error connecting or querying database: {e}")
    sys.exit(1)

# Function to check if a color exists in the ERP list
def check_color_in_erp(color):
    if pd.isna(color) or color is None:
        return False
    return color.lower() in erp_colors

# Add a new column for validation results
print("\nValidating color codes...")
df['is_valid_color'] = df['color'].apply(check_color_in_erp)

# Calculate validation statistics
valid_count = df['is_valid_color'].sum()
invalid_count = (~df['is_valid_color']).sum()
total_count = len(df)

# Display validation results
print("\n" + "="*50)
print("COLOR VALIDATION RESULTS")
print("="*50)
print(f"Total data rows: {total_count:,}")
print(f"Valid color code count: {valid_count:,} ({valid_count/total_count:.1%})")
print(f"Invalid color code count: {invalid_count:,} ({invalid_count/total_count:.1%})")

# Save the validation results to a new file
output_dir = os.path.join(DATA_DIR, "output")
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "color_validation_results.csv")
try:
    df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
    print(f"\nValidation results saved to: {output_file}")
except Exception as e:
    print(f"Error saving results file: {e}")
    import traceback
    traceback.print_exc()

if __name__ == "__main__":
    main()
