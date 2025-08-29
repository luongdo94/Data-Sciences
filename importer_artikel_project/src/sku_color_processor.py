import pandas as pd
import pyodbc
import re
from src.config import OUTPUT_DIR, MDB_DATA

def extract_color(sku):
    if not isinstance(sku, str):
        return None
    
    parts = sku.split('-')
    if len(parts) >= 3:
        color_part = parts[1].split('/')[0].strip()
        return color_part.lower()
    return None

def process_colors(csv_file_path=None, sku_column='aid'):
    if csv_file_path is None:
        csv_file_path = OUTPUT_DIR / "skus.csv"
        
    conn_str = f"DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={MDB_DATA};"

    try:
        # Read original data
        original_df = pd.read_csv(csv_file_path, encoding='utf-8-sig', sep=';', dtype=str)
        
        # Get original column order
        original_columns = original_df.columns.tolist()
        
        with pyodbc.connect(conn_str) as conn:
            color_query = """
                SELECT a.ERP_Farben, MID(a.FarbName, 4) AS ewFarben
                FROM tArtFarben AS a
                WHERE a.isExport = TRUE
            """
            color_map_df = pd.read_sql(color_query, conn)

        color_map = color_map_df.set_index('ewFarben')['ERP_Farben'].to_dict()
        
        sku_column_name = sku_column if sku_column else 'aid'
        
        if sku_column_name not in original_df.columns:
            raise ValueError(f"Column '{sku_column_name}' not found in the CSV file")

        # Create a copy for processing, the color will be replaced with the ERP color if it not in ew_Farben
        sku_df = original_df.copy()
        sku_df['temp_color'] = sku_df[sku_column_name].apply(extract_color)
        sku_df['new_color'] = sku_df['temp_color'].map(color_map).fillna(sku_df['temp_color'])
        
        def process_sku(row):
            if not isinstance(row[sku_column_name], str) or pd.isna(row['temp_color']) or pd.isna(row['new_color']):
                return row[sku_column_name]
                
            parts = row[sku_column_name].split('-')
            if len(parts) < 3:
                return row[sku_column_name]
                
            color_part = parts[1].split('/')[0].strip()
            
            if row['temp_color'] != row['new_color']:
                color_part = row['new_color']
                
            # Remove all hyphens from the color part
            parts[1] = color_part.replace('-', '')
            return '-'.join(parts)
            
        # Update SKU column
        sku_df[sku_column_name] = sku_df.apply(process_sku, axis=1)
        
        # Remove temporary columns
        result_df = sku_df.drop(columns=['temp_color', 'new_color'], errors='ignore')
        
        # Maintain original column order
        result_df = result_df[original_columns]
        
        # Save the file
        result_df.to_csv(csv_file_path, index=False, encoding='utf-8-sig', sep=';')
        
        return result_df

    except Exception as e:
        raise Exception(f"Error processing SKU colors: {str(e)}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process SKU colors in a CSV file.')
    parser.add_argument('--file', type=str, help='Path to the CSV file')
    parser.add_argument('--column', type=str, help='Name of the column containing SKUs')
    
    args = parser.parse_args()
    process_colors(csv_file_path=args.file, sku_column=args.column)

