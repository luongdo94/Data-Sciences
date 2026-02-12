
import pandas as pd
from pathlib import Path
from src.database import execute_query
from src.config import OUTPUT_DIR, SQL_DIR

class BusinessPartnerImporter:
    """
    Importer class for handling Business Partners, Suppliers, Communications, and Addresses.
    Refactoring the previous 4 standalone functions into a unified class structure.
    """

    def __init__(self, diff_partner_ids=None):
        self.output_dir = OUTPUT_DIR
        self.sql_dir = SQL_DIR
        self.diff_ids = self._resolve_diff_ids(diff_partner_ids)
        self.eu_countries = {
            'BE', 'BG', 'CZ', 'DK', 'EE', 'IE', 'EL', 'ES', 'FR', 'HR', 'IT', 
            'CY', 'LV', 'LT', 'LU', 'HU', 'MT', 'NL', 'AT', 'PL', 'PT', 'RO', 
            'SI', 'SK', 'FI', 'SE'
        }

    def _resolve_diff_ids(self, ids):
        """Internal helper to load diff ids if not provided"""
        if ids is None:
            try:
                from run_comparison_standalone import diff_partner_ids
                return {str(pid) for pid in diff_partner_ids} if diff_partner_ids else None
            except (ImportError, AttributeError):
                return None
        return {str(pid) for pid in ids} if ids else None

    def _load_query(self, filename):
        """Helper to safely load SQL query from file"""
        sql_path = self.sql_dir / filename
        if not sql_path.exists():
            print(f"Warning: SQL file not found at {sql_path}")
            # Create placeholder if missing (replicating original logic)
            with open(sql_path, 'w', encoding='utf-8') as f:
                f.write("SELECT * FROM tAdressen")
            print(f"Created placeholder SQL file at {sql_path}")
            
        return sql_path.read_text(encoding='utf-8')

    def _fetch_data(self, sql_filename, filter_col='AdrId'):
        """Common logic to execute query and filter by IDs"""
        query = self._load_query(sql_filename)
        df = pd.DataFrame(execute_query(query))
        
        if df.empty:
            return df

        if self.diff_ids and filter_col in df.columns:
            print(f"Filtering {len(self.diff_ids)} records")
            df[filter_col] = df[filter_col].astype(str)
            df = df[df[filter_col].isin(self.diff_ids)]
            
        return df

    def _save_csv(self, df, filename):
        """Standardized CSV export"""
        if df is not None and not df.empty:
            out_path = self.output_dir / filename
            df.to_csv(out_path, index=False, encoding='utf-8-sig', sep=';')
            print(f"Exported {len(df)} records to: {out_path}")
            return out_path
        return None

    def _normalize_common_fields(self, df):
        """Apply common transformations like language and customer_id"""
        # Language map
        if 'CodeLang' in df.columns:
            df['language'] = df['CodeLang'].apply(
                lambda x: 'en' if str(x) in ['826'] else 'de'
            )
        else:
            df['language'] = 'de'

        # Tax definition
        if 'country' in df.columns:
            df['tax_def'] = df['country'].apply(self._get_tax_def)
        else:
            df['tax_def'] = 'Inland'
            
        # Standard constants
        if 'company' not in df.columns: df['company'] = 1
        if 'is_company' not in df.columns: df['is_company'] = 1

        # Customer ID padding
        if 'customer_id' in df.columns:
            df['customer_id'] = df['customer_id'].astype(str).str.zfill(5)
            
        return df

    def _get_tax_def(self, country_code):
        if not country_code or pd.isna(country_code):
            return 'Inland'
        code = str(country_code).upper()
        if code == 'DE': return 'Inland'
        if code in self.eu_countries: return 'EU'
        return 'Drittland'

    # --- MAIN METHODS ---

    def import_business_partner(self):
        df = self._fetch_data('get_business_partner.sql')
        if df.empty: return None

        # Rename
        rename_map = {
            'Name2': 'company_name1', 'Name3': 'company_name2',
            'SteuerNr': 'Tax_Number', 'UStID': 'vat_id',
            'LandKfz': 'country', 'KNummer': 'customer_id'
        }
        df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)
        
        # Normalize
        if 'currency' not in df.columns: df['currency'] = 'EUR'
        df = self._normalize_common_fields(df)

        # Export columns
        cols = ['customer_id', 'company', 'is_company', 'company_name1', 'company_name2', 
                'Tax_Number', 'vat_id', 'country', 'language', 'currency', 'tax_def']
        
        # Logic to split MA vs Others
        df_ma = pd.DataFrame()
        if 'KGruppe' in df.columns:
            df_ma = df[df['KGruppe'] == 'MA'].copy()
            df = df[df['KGruppe'] != 'MA'].copy()
        
        # Process MA
        if not df_ma.empty:
            df_ma['is_company'] = 0
            df_ma.rename(columns={'company_name1': 'last_Name', 'company_name2': 'first_Name'}, inplace=True)
            # Split name logic here if needed...
            self._save_csv(df_ma, "BUSINESS_PARTNER_MA.csv")

        # Process Others
        df = df[[c for c in cols if c in df.columns]].drop_duplicates()
        return self._save_csv(df, "BUSINESS_PARTNER.csv")

    def import_business_supplier(self):
        df = self._fetch_data('get_business_supplier.sql')
        if df.empty: return None

        rename_map = {
            'AdrNr': 'supplier_id', 'AdrName1': 'company_name1',
            'AdrBem': 'company_name2', 'LandKfz' : 'country',
            'CurrCode' : 'currency', 'SteuerNr' : 'Tax_Number',
            'UStIdNr' : 'vat_id'
        }
        df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)
        df = self._normalize_common_fields(df)

        cols = ['supplier_id', 'company', 'is_company', 'company_name1', 
                'Tax_Number', 'country', 'language', 'currency', 'tax_def']
        
        df = df[[c for c in cols if c in df.columns]].drop_duplicates()
        return self._save_csv(df, "BUSINESS_SUPPLIER.csv")

    def import_communication(self):
        df = self._fetch_data('get_bp_communication.sql')
        if df.empty: return None
        
        # Common prep
        df['company'] = 1
        rename_base = {'Name2': 'name1', 'KNummer': 'customer_id'}
        df.rename(columns={k:v for k,v in rename_base.items() if k in df.columns}, inplace=True)
        if 'customer_id' in df.columns:
            df['customer_id'] = df['customer_id'].astype(str).str.zfill(5)

        # Config for splitting
        comm_configs = [
            (1, 'Email', 'EMail', "BUSINESS_PARTNER_EMAIL.csv"),
            (2, 'Fax', 'Fax', "BUSINESS_PARTNER_FAX.csv"),
            (3, 'Tel', 'Telefon', "BUSINESS_PARTNER_TELEFON.csv"),
            (4, 'Funk', 'Mobil', "BUSINESS_PARTNER_MOBIL.csv"),
            (5, 'Homepage', 'URL', "BUSINESS_PARTNER_URL.csv"),
        ]

        results = []
        for type_id, src_col, dest_col_name, filename in comm_configs: # dest_col_name unused in logic but good for metadata
             if src_col in df.columns:
                sub_df = df.copy()
                sub_df['communication_type'] = type_id
                sub_df['communication_string'] = sub_df[src_col]
                
                cols = ['customer_id', 'company', 'name1', 'communication_type', 'communication_string']
                sub_df = sub_df[[c for c in cols if c in sub_df.columns]].drop_duplicates()
                results.append(self._save_csv(sub_df, filename))
        
        return results

    def import_address(self):
        df = self._fetch_data('get_bp_address.sql')
        if df.empty: return None

        rename_map = {
            'Name2': 'name1', 'LandKfz': 'country', 'KNummer': 'customer_id',
            'Straße': 'street', 'PLZ': 'post_code', 'Ort': 'city',
            'EMail': 'email', 'Fax': 'fax', 'Tel': 'telephone'
        }
        df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)
        df = self._normalize_common_fields(df) # Handles padded customer_id

        cols = ['customer_id', 'company', 'name1', 'street', 'post_code', 
                'city', 'country', 'email', 'fax', 'telephone']
        
        df = df[[c for c in cols if c in df.columns]].drop_duplicates()
        return self._save_csv(df, "BUSINESS_PARTNER_ADDRESS.csv")

