import pandas as pd
import sys
from pathlib import Path

# Add the parent directory to the path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from src.database import execute_sql_file
from src.config import OUTPUT_DIR

def import_auftrag():
    try:
        df = execute_sql_file('tAuftrag.sql')
        
        column_mapping = {
            'AuftragID': 'order_id',
            'LiefID': 'customer_id',
            'erfaßt_am': 'order_date',
            'RText': 'txdef'
        }
        
        transformed_df = df[list(column_mapping.keys())].rename(columns=column_mapping)
        transformed_df['login'] = 0
        transformed_df['company'] = 0
        transformed_df['order_date'] = pd.to_datetime(transformed_df['order_date'], errors='coerce').dt.strftime('%Y-%m-%d')
        
        output_file = OUTPUT_DIR / 'importer_order_head.csv'
        transformed_df[['order_id', 'customer_id', 'login', 'order_date', 'txdef', 'company']]\
            .to_csv(output_file, index=False, sep=';')
        
    except Exception as e:
        print(f"Error: {e}")
        raise

def import_auftrag_position():
    try:
        df = execute_sql_file('tAuftragPos.sql')
        
        if 'quantity' not in df.columns:
            raise ValueError("Quantity column not found")
        
        column_mapping = {
            'AuftragID': 'order_id',
            'PosID': 'position_no',
            'quantity': 'quantity',
            'login': 'login',
            'factory': 'factory'
        }
        
        existing_columns = [col for col in column_mapping if col in df.columns]
        transformed_df = df[existing_columns].rename(columns=column_mapping)
        
        required_columns = ['order_id', 'position_no', 'quantity']
        for col in required_columns:
            if col not in transformed_df.columns:
                raise ValueError(f"Required column '{col}' not found")
        
        if 'login' not in transformed_df.columns:
            transformed_df['login'] = ''
        transformed_df['factory'] = 'Düsseldorf'
        
        output_file = OUTPUT_DIR / 'importer_order_position.csv'
        transformed_df[['order_id', 'position_no', 'quantity', 'login', 'factory']]\
            .to_csv(output_file, index=False, sep=';', encoding='utf-8-sig')
        
    except Exception as e:
        print(f"Error: {e}")
        raise

