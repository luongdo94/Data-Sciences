import pandas as pd
from database import execute_sql_file
from config import OUTPUT_DIR

def main():
    try:
        # Process order heads
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
        
        final_columns = ['order_id', 'customer_id', 'login', 'order_date', 'txdef', 'company']
        transformed_df = transformed_df[final_columns]
        
        output_file = OUTPUT_DIR / 'importer_order_head.csv'
        transformed_df.to_csv(output_file, index=False, sep=';')
        print(f"Exported to: {output_file}")
        
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main()
