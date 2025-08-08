import pandas as pd
import pyodbc
import re

def extract_color(sku):
    """Extracts the color from the SKU string, removing anything after / in the color part."""
    if not isinstance(sku, str):
        return None
    
    # Split by '-'
    parts = sku.split('-')
    if len(parts) >= 3:  # At least 3 parts: [number, color, size...]
        # Process only the color part (second part)
        color_part = parts[1].split('/')[0].strip()
        return color_part.lower()
    return None

def process_colors(csv_file_path=None, sku_column=None):
    """
    Main function to process SKU colors.
    
    Args:
        csv_file_path (str, optional): Path to the CSV file. Defaults to None.
        sku_column (str, optional): Name of the column containing SKUs. If None, uses first column.
    """
    # --- Configuration ---
    if csv_file_path is None:
        csv_file_path = r"C:\Users\gia.luongdo\Desktop\ERP-Importer\IMPORTER_SKU_BASIS_notinERP.csv"
        
    mdb_file = r"C:\Users\gia.luongdo\Desktop\ERP-Importer\DATEN.MDB"
    conn_str = (
        r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
        f"DBQ={mdb_file};"
    )

    try:
        # --- Database Connection and Color Map Retrieval ---
        print("Connecting to the database...")
        conn = pyodbc.connect(conn_str)
        print("Database connection successful.")

        color_query = """
            SELECT a.ERP_Farben, MID(a.FarbName, 4) AS ewFarben
            FROM tArtFarben AS a
            WHERE a.isExport = TRUE
        """
        print("Fetching color map from the database...")
        color_map_df = pd.read_sql(color_query, conn)
        conn.close()
        print(f"Successfully fetched {len(color_map_df)} color mappings.")

        # Create a dictionary for faster lookups: {ewFarben: ERP_Farben}
        color_map = color_map_df.set_index('ewFarben')['ERP_Farben'].to_dict()

        # Read and process the CSV file
        print(f"Reading SKU data from {csv_file_path}...")
        sku_df = pd.read_csv(csv_file_path, encoding='utf-8-sig', sep=';')
        
        # Use the first column if sku_column is not specified
        sku_column_name = sku_df.columns[0] if sku_column is None else sku_column
        
        # Check if the specified column exists
        if sku_column_name not in sku_df.columns:
            print(f"Error: Column '{sku_column_name}' not found in the CSV file.")
            print(f"Available columns: {', '.join(sku_df.columns)}")
            return
            
        print(f"Processing column: {sku_column_name}")

        # Process the SKUs
        sku_df['temp_color'] = sku_df[sku_column_name].apply(extract_color)
        sku_df['new_color'] = sku_df['temp_color'].map(color_map).fillna(sku_df['temp_color'])
        
        # Function to process each SKU
        def process_sku(row):
            if not isinstance(row[sku_column_name], str) or pd.isna(row['temp_color']) or pd.isna(row['new_color']):
                return row[sku_column_name]
                
            # Split the SKU into parts
            parts = row[sku_column_name].split('-')
            if len(parts) < 3:
                return row[sku_column_name]
                
            # Clean the color part (remove anything after /)
            color_part = parts[1].split('/')[0].strip()
            
            # Only replace if the color actually changed
            if row['temp_color'] != row['new_color']:
                color_part = row['new_color']
                
            # Rebuild the SKU
            parts[1] = color_part
            return '-'.join(parts)
            
        # Update the original column with processed values
        sku_df[sku_column_name] = sku_df.apply(process_sku, axis=1)
        
        # --- Save Results ---
        print(f"\nSaving updated data to {csv_file_path}...")
        sku_df[[sku_column_name]].to_csv(csv_file_path, index=False, encoding='utf-8-sig', sep=';')
        
        # Count how many colors were changed
        changed_count = (sku_df['temp_color'] != sku_df['new_color']).sum()
        
        print("\n--- Summary ---")
        print(f"File saved successfully: {csv_file_path}")
        print(f"Total SKUs processed: {len(sku_df)}")
        print(f"SKUs with updated colors: {changed_count}")
        
        # Show a preview of the changes
        print("\n--- First few rows of updated data ---")
        print(sku_df[sku_column_name].head().to_string())
        
        # Check for duplicates in the updated SKUs
        duplicates = sku_df[sku_column_name].duplicated(keep=False)
        if duplicates.any():
            print(f"\nWarning: Found {duplicates.sum()} duplicate SKUs after processing.")
            print("First few duplicates:")
            print(sku_df[sku_column_name][duplicates].head().to_string())
        else:
            print("\nNo duplicate SKUs found after processing.")

    except FileNotFoundError:
        print(f"ERROR: The file was not found at {csv_file_path}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    import argparse
    
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Process SKU colors in a CSV file.')
    parser.add_argument('--file', type=str, help='Path to the CSV file')
    parser.add_argument('--column', type=str, help='Name of the column containing SKUs')
    
    args = parser.parse_args()
    
    # Call the main function with provided arguments
    process_colors(csv_file_path=args.file, sku_column=args.column)
