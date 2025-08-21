import pandas as pd
from pathlib import Path

def fix_column_names(file_path):
    try:
        # Convert to Path object if it's a string
        file_path = Path(file_path)
        
        # Read the file
        df = pd.read_csv(file_path, sep=';', encoding='utf-8-sig')
        
        # Check if column exists and needs renaming
        if 'is_mandatory.1' in df.columns:
            df = df.rename(columns={'is_mandatory.1': 'is_mandatory'})
            
            # Save the file with the corrected column name
            df.to_csv(file_path, index=False, sep=';', encoding='utf-8-sig')
            print(f"Successfully renamed 'is_mandatory.1' to 'is_mandatory' in {file_path}")
            return True
        else:
            print(f"No column named 'is_mandatory.1' found in {file_path}")
            return False
            
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        return False
