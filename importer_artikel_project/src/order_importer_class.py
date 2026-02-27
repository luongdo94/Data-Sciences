
import pandas as pd
from datetime import datetime
from pathlib import Path
from src.database import execute_query, save_fetcsv
from src.config import OUTPUT_DIR, SQL_DIR

class OrderImporter:
    """
    Importer class for handling Order data.
    Based on the logic from import_order functions in simple_article_importer.py.
    """

    def __init__(self):
        self.output_dir = OUTPUT_DIR
        self.sql_dir = SQL_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _load_query(self, filename):
        """Helper to safely load SQL query from file"""
        sql_path = self.sql_dir / filename
        if not sql_path.exists():
            print(f"Warning: SQL file not found at {sql_path}")
            return None
        return sql_path.read_text(encoding='utf-8-sig').strip() # Using utf-8-sig as in original code

    def _save_csv(self, df, filename, data_type="CONTRACT"):
        """Standardized CSV export with FETCSV header"""
        if df is not None and not df.empty:
            out_path = self.output_dir / filename
            save_fetcsv(df, out_path, data_type)
            print(f"Exported {len(df)} records to: {out_path}")
            return out_path
        return None

    def _decode_clerk(self, val):
        return val.decode('utf-16-le') if isinstance(val, bytes) else str(val)

    def _create_empty_csv(self, filename, columns, data_type="CONTRACT"):
        """Creates an empty CSV file with headers if no data is found (as per original logic)"""
        out_path = self.output_dir / filename
        save_fetcsv(pd.DataFrame(columns=columns), out_path, data_type)
        print(f"Created empty file: {out_path}")
        return out_path

    # --- MAIN METHODS ---

    def import_order(self):
        query = self._load_query('get_order.sql')
        if not query: return None
        
        df = pd.DataFrame(execute_query(query))
        if df.empty: return None

        # Rename
        rename_map = {
            'OrderNr_Lang': 'txId', 'POCode': 'txIdExternal',
            'AdrId': 'supplier_id', 'Name': 'clerk',
            'erfasst_am': 'txDate', 'OrgDatum': 'ex_txDate'
        }
        df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)

        # Defaults
        df['order_auto'] = 1
        df['currency'] = 'EUR' # Note: Original code used USD, but logic suggests EUR/USD based on context. Keeping USD as per original line 2586 or changing? 
        # Wait, original line 2586 said: order_data['currency'] = 'USD'
        # But import_business_partner used EUR. Let's stick to original behavior: USD
        df['currency'] = 'USD'

        # Date handling
        for date_col in ['ex_txDate', 'txDate']:
            if date_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[date_col]):
                df[date_col] = df[date_col].dt.strftime("%Y%m%d")
            else:
                df[date_col] = datetime.now().strftime("%Y%m%d")

        df['supplier_id'] = df['txId'].str[:5]
        df['txDef'] = 'Kontrakt_EWOrder'
        df['company'] = 1

        if 'clerk' in df.columns:
            df['clerk'] = df['clerk'].apply(self._decode_clerk)

        cols = ['txId', 'txIdExternal', 'supplier_id', 'clerk', 'txDate', 
                'ex_txDate', 'order_auto', 'currency', 'txDef', 'company']
        
        df = df[[c for c in cols if c in df.columns]]
        return self._save_csv(df, "order_data.csv")

    def import_order_pos(self):
        query = self._load_query('get_orderpos.sql')
        if not query: return None
        
        df = pd.DataFrame(execute_query(query))
        if df.empty: return None

        rename_map = {
            'OrderNr_Lang': 'txId', 'Menge': 'quantity',
            'OPreis': 'price', 'ArtikelCode': 'aid',
            'erfaßt_am': 'valid_from'
        }
        df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)

        # Defaults
        defaults = {
            'company': 1, 'priceUnit': 'Stk', 'factory': 'Düsseldorf',
            'commodity_group_path': '', 'unit': 'Stk', 
            'use_proc_unit_for_purchase': '0', 'supplierAid': '', 'pos_text': ''            
        }
        for k, v in defaults.items():
            df[k] = v

        df['supplier_id'] = df['txId'].str[:5]
        
        if 'valid_from' in df.columns:
            df['valid_from'] = df['valid_from'].dt.strftime("%Y%m%d")
            
        if 'price' in df.columns:
            df['price'] = df['price'].astype(str).str.replace('.', ',', regex=False)

        if 'clerk' in df.columns:
            df['clerk'] = df['clerk'].apply(self._decode_clerk)

        cols = ['txId', 'quantity', 'price', 'aid', 'company', 'priceUnit', 
                'supplier_id', 'factory', 'commodity_group_path', 'unit', 
                'use_proc_unit_for_purchase', 'supplierAid', 'valid_from', 'pos_text']
        
        df = df[[c for c in cols if c in df.columns]]
        return self._save_csv(df, "order_pos_data.csv")

    def import_order_are_15(self):
        # Almost identical to import_order but different SQL and file
        query = self._load_query('get_order_are_15.sql')
        if not query: return None
        
        # Original code printed raw query but we skip that
        
        df = pd.DataFrame(execute_query(query))
        if df.empty: return None # Original code doesn't create empty file for this one, just returns None

        # Logic is very similar to import_order
        rename_map = {
            'OrderNr_Lang': 'txId', 'POCode': 'txIdExternal',
            'AdrId': 'supplier_id', 'Name': 'clerk',
            'erfasst_am': 'txDate', 'OrgDatum': 'ex_txDate'
        }
        df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)

        df['order_auto'] = 1
        df['currency'] = 'USD'
        
        for date_col in ['ex_txDate', 'txDate']:
            if date_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[date_col]):
                df[date_col] = df[date_col].dt.strftime("%Y%m%d")
            else:
                df[date_col] = datetime.now().strftime("%Y%m%d")

        df['supplier_id'] = df['txId'].str[:5]
        df['txDef'] = 'Kontrakt_EWOrder'
        df['company'] = 1

        if 'clerk' in df.columns:
            df['clerk'] = df['clerk'].apply(self._decode_clerk)

        cols = ['txId', 'txIdExternal', 'supplier_id', 'clerk', 'txDate', 
                'ex_txDate', 'order_auto', 'currency', 'txDef', 'company']
        
        df = df[[c for c in cols if c in df.columns]]
        return self._save_csv(df, "order_are_15_data.csv")

    def import_order_pos_are_15(self):
        cols = ['txId', 'quantity', 'price', 'aid', 'company', 'priceUnit', 
                'supplier_id', 'factory', 'commodity_group_path', 'unit', 
                'use_proc_unit_for_purchase', 'supplierAid', 'valid_from', 'pos_text']
        
        query = self._load_query('get_orderpos_are_15.sql')
        if not query: 
            return self._create_empty_csv("order_pos_are_15_data.csv", cols)
            
        df = pd.DataFrame(execute_query(query))
        
        if df.empty:
            return self._create_empty_csv("order_pos_are_15_data.csv", cols)

        rename_map = {
            'OrderNr_Lang': 'txId', 'Menge': 'quantity',
            'OPreis': 'price', 'ArtikelCode': 'aid',
            'erfaßt_am': 'valid_from'
        }
        df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)
        
        # Defaults
        defaults = {
            'company': 1, 'priceUnit': 'Stk', 'factory': 'Düsseldorf',
            'commodity_group_path': '', 'unit': 'Stk', 
            'use_proc_unit_for_purchase': '0', 'supplierAid': '', 'pos_text': ''
        }
        for k, v in defaults.items():
            df[k] = v
            
        df['supplier_id'] = df['txId'].str[:5]
        
        if 'price' in df.columns:
            df['price'] = df['price'].astype(str).str.replace('.', ',', regex=False)
            
        if 'valid_from' in df.columns:
            df['valid_from'] = df['valid_from'].dt.strftime("%Y%m%d")
            
        if 'clerk' in df.columns:
             df['clerk'] = df['clerk'].apply(self._decode_clerk)
             
        df = df[[c for c in cols if c in df.columns]]
        return self._save_csv(df, "order_pos_are_15_data.csv")

    def import_order_classification(self):
        # 1. Export Pos Data (Reusing import_order_pos logic partially but query might be different? 
        # Original code reads get_orderpos.sql AGAIN in this function. 
        # But wait, original code exports to "orderpos_data.csv" then proceeds to read get_order.sql
        # This seems redundant if import_order_pos() is already called.
        # But for strict refactoring we just implement logic)
        
        # Let's call our own method for the pos part to avoid duplicate code
        # self.import_order_pos() # Redundant call removed
        # Note: Original code executed get_orderpos.sql and saved it. calling self.import_order_pos() does exactly that.

        # 2. Main Classification Query
        query = self._load_query('get_order.sql')
        if not query: return None
        
        df = pd.DataFrame(execute_query(query))
        if df.empty: return None

        rename_map = {
            'OrderNr_Lang': 'txId', 'POCode': 'txIdExternal',
            'AdrId': 'supplier_id', 'Name': 'clerk',
            'erfasst_am': 'txDate', 'OrgDatum': 'ex_txDate'
        }
        df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)
        
        df['order_auto'] = 1
        df['currency'] = 'USD'
        
        for date_col in ['ex_txDate', 'txDate']:
            if date_col in df.columns and pd.api.types.is_datetime64_any_dtype(df[date_col]):
                df[date_col] = df[date_col].dt.strftime("%Y%m%d")
            else:
                df[date_col] = datetime.now().strftime("%Y%m%d")
                
        df['supplier_id'] = df['txId'].str[:5]
        
        # Classification specific fields
        df['txDef'] = 'Kontrakt_EWOrder'
        df['company'] = 1
        df['classification_system'] = 'Version'
        df['feature[0]'] = 'CUT'
        df['feature_value[0]'] = '1'
        df['feature[1]'] = 'SpecSheet'
        df['feature_value[1]'] = ''
        df['K_Typ'] = '1'
        
        if 'clerk' in df.columns:
            df['clerk'] = df['clerk'].apply(self._decode_clerk)
        
        cols = ['txId', 'K_Typ', 'classification_system', 'feature[0]', 'feature_value[0]', 'feature[1]', 'feature_value[1]']
        df = df[[c for c in cols if c in df.columns]]
        
        return self._save_csv(df, "order_classification.csv")
