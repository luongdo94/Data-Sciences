
import pandas as pd
from pathlib import Path
from src.database import execute_query, save_fetcsv
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

    def _save_csv(self, df, filename, data_type="BUSINESS_PARTNER"):
        """Standardized CSV export with FETCSV header"""
        if df is not None and not df.empty:
            out_path = self.output_dir / filename
            save_fetcsv(df, out_path, data_type)
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

    @staticmethod
    def calculate_german_iban(blz, kontonummer):
        """Calculate German IBAN from BLZ (8-digit bank code) and Kontonummer."""
        import re
        if pd.isna(blz) or pd.isna(kontonummer):
            return ""
        blz_str = re.sub(r'\D', '', str(blz).strip())
        konto_str = re.sub(r'\D', '', str(kontonummer).strip())
        if len(blz_str) != 8 or not konto_str:
            return ""
        konto_str = konto_str.zfill(10)
        calc_number = int(blz_str + konto_str + "131400")
        check_digit = str(98 - (calc_number % 97)).zfill(2)
        return f"DE{check_digit}{blz_str}{konto_str}"

    @staticmethod
    def calculate_bic_from_iban(iban_str):
        """Look up BIC from a valid IBAN string using the schwifty library.
        
        Cleans common formatting issues (spaces, dashes) before lookup.
        Accepts any valid IBAN (not just German DE IBANs).
        Returns empty string if IBAN is invalid or BIC is not found.
        """
        if not iban_str or not str(iban_str).strip():
            return ""
        import re
        # Clean IBAN: remove all non-alphanumeric characters
        cleaned = re.sub(r'[^A-Za-z0-9]', '', str(iban_str).strip()).upper()
        # Valid IBAN: starts with 2 letters (country code) + 2 digits (check digits) + min 10 chars
        if not re.match(r'^[A-Z]{2}[0-9]{2}[A-Z0-9]{10,}$', cleaned):
            return ""
        try:
            from schwifty import IBAN
            iban = IBAN(cleaned)
            bic = iban.bic
            return str(bic) if bic else ""
        except Exception:
            return ""

    def import_business_customer(self):
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

    def import_business_customer_accounting(self):
        df = self._fetch_data('get_business_partner_accounting.sql')
        if df.empty: return None

        # Rename
        rename_map = {
            'Name2': 'name1', 'Name3': 'name2',
            'LandKfz': 'country', 'KNummer': 'customer_id',
            'Bank': 'bankname', 'BLZ': 'bankcode', 'Konto': 'accountno'
        }
        df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)
        
        # Normalize
        if 'currency' not in df.columns: df['currency'] = 'EUR'
        df = self._normalize_common_fields(df)

        # Export columns
        cols = ['customer_id', 'company', 'name1', 'name2',
                'bankname', 'bankcode', 'bankplace', 'bankcountry', 'iban', 'swiftbic', 'accountno']
        
        # --- Resolve IBAN from mixed BLZ/BIC and Kontonummer/IBAN fields ---
        #
        # Real-world data combinations:
        #   Case 1: BLZ (8 digits) + Kontonummer (digits)  → calculate IBAN
        #   Case 2: BLZ (8 digits) + any valid IBAN        → use IBAN from accountno
        #   Case 3: BIC (letters)  + any valid IBAN        → use IBAN from accountno
        #   Case 4: BIC (letters)  + Kontonummer (digits)  → cannot resolve, leave empty
        import re as _re
        _iban_pattern = _re.compile(r'^[A-Z]{2}[0-9]{2}[A-Z0-9]{10,}$')
        _bic_pattern  = _re.compile(r'^[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}([A-Z0-9]{3})?$')

        def _clean_alphanum(val):
            """Strip all non-alphanumeric characters and uppercase."""
            return _re.sub(r'[^A-Za-z0-9]', '', str(val).strip()).upper() if pd.notna(val) else ''

        def resolve_iban(bankcode_val, accountno_val):
            blz   = str(bankcode_val).strip() if pd.notna(bankcode_val) else ''
            konto = _clean_alphanum(accountno_val)  # Improvement 1: clean whitespace/dashes

            # Improvement 2: accept any valid IBAN (not just German DE) → covers Cases 2 & 3
            if _iban_pattern.match(konto):
                return konto

            # BLZ is an 8-digit bank code → calculate German IBAN (Case 1)
            if blz and blz[0].isdigit():
                return self.calculate_german_iban(blz, konto)

            # BIC + Kontonummer → cannot resolve IBAN (Case 4)
            return ''

        df['iban'] = df.apply(lambda row: resolve_iban(row.get('bankcode'), row.get('accountno')), axis=1)

        # --- Load Bundesbank BLZ registry (city + BIC + bank name from official source) ---
        blz_lookup_path = SQL_DIR.parent / 'data' / 'bundesbank_blz_lookup.csv'
        _required_cols = {'bic', 'bank_name'}
        if not blz_lookup_path.exists() or not _required_cols.issubset(pd.read_csv(blz_lookup_path, nrows=1).columns):
            import urllib.request
            url = ('https://www.bundesbank.de/resource/blob/602632/'
                   'bec25ca5df1eb62fefadd8325dafe67c/'
                   '472B63F073F071307366337C94F8C870/blz-aktuell-txt-data.txt')
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=30) as r:
                raw = r.read().decode('latin-1')
            records = []
            for line in raw.splitlines():
                if len(line) < 150: continue
                records.append({
                    'blz'      : line[0:8].strip(),
                    'bank_name': line[9:67].strip(),    # full bank name from Bundesbank
                    'city'     : line[72:107].strip(),
                    'bic'      : line[139:150].strip()
                })
            blz_df = pd.DataFrame(records).drop_duplicates(subset='blz', keep='first')
            blz_lookup_path.parent.mkdir(parents=True, exist_ok=True)
            blz_df.to_csv(blz_lookup_path, index=False, encoding='utf-8')
        else:
            blz_df = pd.read_csv(blz_lookup_path, dtype=str)

        blz_city_map = dict(zip(blz_df['blz'], blz_df['city']))
        blz_bic_map  = dict(zip(blz_df['blz'], blz_df['bic']))       if 'bic'       in blz_df.columns else {}
        blz_name_map = dict(zip(blz_df['blz'], blz_df['bank_name'])) if 'bank_name' in blz_df.columns else {}

        df['bankplace'] = df['bankcode'].apply(
            lambda v: blz_city_map.get(str(v).strip(), '') if pd.notna(v) else ''
        )

        # Fill bankname using a 3-level fallback chain (defined here, applied after swiftbic is computed)
        _bic_name_cache = {}

        def resolve_bankname(name_val, bankcode_val, bic_val):
            # Priority 1: use source bankname if available
            name = str(name_val).strip() if pd.notna(name_val) else ''
            if name and name.lower() not in ('nan', 'none', ''):
                return name

            # Priority 2: look up bank name from Bundesbank BLZ registry (numeric BLZ only)
            blz = str(bankcode_val).strip() if pd.notna(bankcode_val) else ''
            bb_name = blz_name_map.get(blz, '')
            if bb_name:
                return bb_name

            # Priority 3: get bank name from BIC via schwifty (cached per unique BIC)
            # NOTE: uses swiftbic column which must be computed before calling this
            bic_key = str(bic_val).strip().upper() if pd.notna(bic_val) else ''
            if bic_key:
                if bic_key not in _bic_name_cache:
                    try:
                        from schwifty import BIC as _BIC
                        _bic_name_cache[bic_key] = _BIC(bic_key).bank_name or ''
                    except Exception:
                        _bic_name_cache[bic_key] = ''
                if _bic_name_cache[bic_key]:
                    return _bic_name_cache[bic_key]

            return ''


        # --- Resolve BIC with improved priority chain + caching ---
        # Improvement 3: cache schwifty calls (same IBAN looked up only once)
        # Improvement 5: reordered priorities for speed and coverage
        _bic_cache = {}

        def resolve_bic(iban_val, bankcode_val):
            blz = str(bankcode_val).strip().upper() if pd.notna(bankcode_val) else ''

            # Priority 1: bankcode is already a valid BIC → instant, no lookup needed
            if blz and blz[0].isalpha() and _bic_pattern.match(blz):
                return blz

            # Priority 2: BIC from Bundesbank BLZ registry (offline, authoritative)
            if blz and blz[0].isdigit():
                bb_bic = blz_bic_map.get(blz, '')
                if bb_bic:
                    return bb_bic

            # Priority 3: schwifty IBAN→BIC lookup (cached per unique IBAN value)
            iban_key = _clean_alphanum(iban_val)
            if iban_key:
                if iban_key not in _bic_cache:
                    _bic_cache[iban_key] = self.calculate_bic_from_iban(iban_key)
                if _bic_cache[iban_key]:
                    return _bic_cache[iban_key]

            # Priority 4: no BIC resolvable (defunct bank or missing data)
            return ''

        df['swiftbic'] = df.apply(lambda row: resolve_bic(row.get('iban'), row.get('bankcode')), axis=1)

        # Now apply bankname fallback (after swiftbic is computed so Priority 3 can use it)
        df['bankname'] = df.apply(
            lambda row: resolve_bankname(row.get('bankname'), row.get('bankcode'), row.get('swiftbic')), axis=1
        )

        # Derive bank country from IBAN (first 2 chars) with fallback to BIC (chars 4-5)
        def get_bank_country(iban_val, bic_val):
            iban = str(iban_val).strip().upper() if iban_val else ''
            if len(iban) >= 2 and iban[:2].isalpha():
                return iban[:2]
            bic = str(bic_val).strip().upper() if bic_val else ''
            if len(bic) >= 6 and bic[4:6].isalpha():
                return bic[4:6]
            return ''

        df['bankcountry'] = df.apply(lambda row: get_bank_country(row.get('iban'), row.get('swiftbic')), axis=1)

        # Export all records into a single file
        out_cols = [c for c in cols if c in df.columns]
        return self._save_csv(df[out_cols].drop_duplicates(), "BUSINESS_PARTNER_ACCOUNTING.csv")

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

    def import_customer_communication(self):
        df = self._fetch_data('get_bp_communication.sql')
        if df.empty: return None
        
        # Common prep
        df['company'] = 1
        rename_base = {'Name2': 'name1', 'KNummer': 'customer_id'}
        df.rename(columns={k:v for k,v in rename_base.items() if k in df.columns}, inplace=True)
        if 'customer_id' in df.columns:
            df['customer_id'] = df['customer_id'].astype(str).str.zfill(5)

        # Config for splitting
        # (type_id, db_column, filename)
        comm_configs = [
            (1, 'EMail', "BUSINESS_PARTNER_EMAIL.csv"),
            (2, 'Fax', "BUSINESS_PARTNER_FAX.csv"),
            (3, 'Tel', "BUSINESS_PARTNER_TELEFON.csv"),
            (4, 'Tel', "BUSINESS_PARTNER_MOBIL.csv"),
            (5, 'Homepage', "BUSINESS_PARTNER_URL.csv"),
        ]

        results = []
        for type_id, src_col, filename in comm_configs: 
             if src_col in df.columns:
                sub_df = df.copy()
                
                # Convert to string and handle actual None - keeping all records as requested
                sub_df['communication_string'] = sub_df[src_col].apply(lambda x: str(x).strip() if pd.notna(x) else "")
                sub_df['communication_type'] = type_id
                
                cols = ['customer_id', 'company', 'name1', 'communication_type', 'communication_string']
                sub_df = sub_df[[c for c in cols if c in sub_df.columns]].drop_duplicates()
                results.append(self._save_csv(sub_df, filename))
        
        return results

    def import_customer_contact_communication(self):
        df = self._fetch_data('get_customer_contact_communication.sql')
        if df.empty: return None
        
        # Common prep
        df['company'] = 1
        rename_map = {
            'KNummer': 'customer_id', 
            'PAdrID': 'employee_id',
            'PVorName': 'first_name', 
            'PName': 'last_name',
            'Name1': 'name1',
            'DWahl': 'Tel',
            'EMail': 'EMail'
        }
        df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)
        if 'customer_id' in df.columns:
            df['customer_id'] = df['customer_id'].astype(str).str.zfill(5)

        # Config for splitting
        # (type_id, db_column, filename)
        comm_configs = [
            (1, 'EMail', "BUSINESS_PARTNER_CONTACT_EMAIL.csv"),
            (3, 'Tel', "BUSINESS_PARTNER_CONTACT_TELEFON.csv"),
            (4, 'Tel', "BUSINESS_PARTNER_CONTACT_MOBIL.csv"),
        ]

        results = []
        for type_id, src_col, filename in comm_configs: 
             if src_col in df.columns:
                sub_df = df.copy()
                
                # Convert to string and handle actual None - keeping all records as requested
                sub_df['communication_string'] = sub_df[src_col].apply(lambda x: str(x).strip() if pd.notna(x) else "")
                sub_df['communication_type'] = type_id
                
                cols = ['customer_id', 'employee_id', 'company', 'name1', 'first_name', 'last_name', 'communication_type', 'communication_string']
                sub_df = sub_df[[c for c in cols if c in sub_df.columns]].drop_duplicates()
                results.append(self._save_csv(sub_df, filename))
        
        return results

    def import_customer_employee_role(self):
        df = self._fetch_data('get_customer_employee_role.sql')
        if df.empty: return None
        
        # Common prep
        df['company'] = 1
        df['add_replace_mode'] = 0
        rename_map = {
            'KNummer': 'customer_id', 
            'PAdrID': 'employee_id',
            'PVorName': 'first_name', 
            'PName': 'last_name',
            'Name1': 'name1',
            'PBereich': 'role'  
        }
        df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)
        if 'customer_id' in df.columns:
            df['customer_id'] = df['customer_id'].astype(str).str.zfill(5)

        # Config for splitting (Similar to communication but for roles)
        # Using type_id=1 for Role as a placeholder/standard
        comm_configs = [
            (1, 'role', "BUSINESS_PARTNER_CONTACT_ROLE.csv"),
        ]

        results = []
        for type_id, src_col, filename in comm_configs: 
             if src_col in df.columns:
                sub_df = df.copy()
                
                # Convert to string and handle actual None
                role_raw = sub_df[src_col].apply(lambda x: str(x).strip() if pd.notna(x) else "")
                
                # Role Mapping
                role_map = {
                    'GF': 'Geschäftsleitung',
                    'Fax-GF': 'Geschäftsleitung',
                    'AL-Eink.': 'Leitung Einkauf',
                    'AL-Verk.': 'Leitung Verkauf',
                    'Buchhalt.': 'Buchhaltung',
                    'Fax-BH': 'Buchhaltung',
                    'Einkauf': 'Einkäufer',
                    'Fax-VK': 'Verkauf',
                    'Gesell.': 'Gesellschafter',
                    'Marketing': 'Marketing',
                    'Inhaber': 'Inhaber',
                    'Verkauf': 'Verkauf',
                    'W-Annahm.': 'Warenannahme',
                    'Zentrale': 'Zentrale'
                }
                sub_df['role'] = role_raw.apply(lambda x: role_map.get(x, ""))
                
                cols = ['customer_id', 'employee_id', 'company', 'name1', 'first_name', 'last_name', 'role', 'add_replace_mode']
                sub_df = sub_df[[c for c in cols if c in sub_df.columns]].drop_duplicates()
                results.append(self._save_csv(sub_df, filename))
        
        return results

    def import_customer_address(self):
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
                'city', 'country', 'email', 'fax', 'telephone', 'isRechnungPDF']
        
        df = df[[c for c in cols if c in df.columns]].drop_duplicates()
        
        dfs = []
        import pandas as pd
        for i in range(1, 9):
            sub_df = df.copy()
            sub_df['address_type'] = i
            if i == 2:
                sub_df['defaultRoleDef'] = 1
            elif i == 3 and 'isRechnungPDF' in sub_df.columns:
                def get_role_def(val):
                    try:
                        # address_type=3 and abs(isRechnungPDF)=1 -> 0
                        # address_type=3 and abs(isRechnungPDF)=0 -> 1
                        val_num = abs(float(val)) if not pd.isna(val) and str(val).strip() != '' else -1
                        return 0 if val_num == 1 else 1
                    except (ValueError, TypeError):
                        return 0
                sub_df['defaultRoleDef'] = sub_df['isRechnungPDF'].apply(get_role_def)
            else:
                sub_df['defaultRoleDef'] = 0

            dfs.append(sub_df)
            
        final_df = pd.concat(dfs, ignore_index=True)
        
        # Ensure correct column order
        final_cols = ['customer_id', 'company', 'name1', 'address_type', 'defaultRoleDef', 'street', 'post_code', 
                      'city', 'country', 'email', 'fax', 'telephone']
        final_df = final_df[[c for c in final_cols if c in final_df.columns]]
        
        return self._save_csv(final_df, "BUSINESS_PARTNER_ADDRESS.csv")

    def import_customer_keyword(self):
        df = self._fetch_data('get_customer_keyword.sql')
        if df.empty: return None

        # Common prep
        df['company'] = 1
        df['isprimary'] = 1
        rename_map = {'KCode': 'keyword', 'Name2': 'name1', 'KNummer': 'customer_id'} # Base rename, update as needed
        df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)
        
        if 'customer_id' in df.columns:
            df['customer_id'] = df['customer_id'].astype(str).str.zfill(5)

        cols = ['customer_id', 'company', 'name1', 'keyword', 'isprimary']
        
        # Logic to split MA vs Others (Same as import_business_customer)
        df_ma = pd.DataFrame()
        if 'KGruppe' in df.columns:
            df_ma = df[df['KGruppe'] == 'MA'].copy()
            df = df[df['KGruppe'] != 'MA'].copy()

        # Process MA Keywords
        if not df_ma.empty:
            df_ma = df_ma[[c for c in cols if c in df_ma.columns]].drop_duplicates()
            self._save_csv(df_ma, "BUSINESS_PARTNER_KEYWORD_MA.csv")

        # Process Customer Keywords
        df = df[[c for c in cols if c in df.columns]].drop_duplicates()
        return self._save_csv(df, "BUSINESS_PARTNER_KEYWORD.csv")

    def import_customer_contact(self):
        df = self._fetch_data('get_customer_contact.sql')
        if df.empty: return None

        # Common prep
        df['company'] = 1
        # Map SQL columns to internal standard names
        rename_map = {
            'AdrID': 'customer_id', 
            'PVorName': 'first_name', 
            'PName': 'last_name',
            'PAnrede': 'address',
            'PAdrID': 'employee_id',
            'WebUserName': 'login',
            'PUserPw': 'password'
        }
        df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)
        
        if 'customer_id' in df.columns:
            df['customer_id'] = df['customer_id'].astype(str).str.zfill(5)

        # Normalize (Standardize salutations, etc.)
        df = self._normalize_common_fields(df)

        # Output columns
        cols = ['customer_id', 'employee_id', 'company', 'address', 'first_name', 'last_name', 'login', 'password', 'language']
        
        df = df[[c for c in cols if c in df.columns]].drop_duplicates()
        return self._save_csv(df, "BUSINESS_PARTNER_CONTACT.csv")
    def import_supplier_communication(self):
        df = self._fetch_data('get_supplier_communication.sql')
        if df.empty: return None
        
        # Common prep
        df['company'] = 1
        rename_base = {'Name1': 'name1', 'SupplierID': 'supplier_id'}
        df.rename(columns={k:v for k,v in rename_base.items() if k in df.columns}, inplace=True)
        
        # Config for splitting (Files named BUSINESS_SUPPLIER_...)
        # (type_id, db_column, filename)
        comm_configs = [
            (1, 'EMail', "BUSINESS_SUPPLIER_EMAIL.csv"),
            (2, 'Fax', "BUSINESS_SUPPLIER_FAX.csv"),
            (3, 'Telefon', "BUSINESS_SUPPLIER_TELEFON.csv"),
            (4, 'Funk', "BUSINESS_SUPPLIER_MOBIL.csv"),
            (5, 'URL', "BUSINESS_SUPPLIER_URL.csv"),
        ]

        results = []
        for type_id, src_col, filename in comm_configs: 
             if src_col in df.columns:
                sub_df = df.copy()
                
                # Convert to string and handle actual None - keeping all records as requested
                sub_df['communication_string'] = sub_df[src_col].apply(lambda x: str(x).strip() if pd.notna(x) else "")
                sub_df['communication_type'] = type_id
                
                cols = ['supplier_id', 'company', 'name1', 'communication_type', 'communication_string']
                sub_df = sub_df[[c for c in cols if c in sub_df.columns]].drop_duplicates()
                results.append(self._save_csv(sub_df, filename, data_type="BUSINESS_SUPPLIER"))
        
        return results

    def import_supplier_address(self):
        df = self._fetch_data('get_supplier_address.sql')
        if df.empty: return None

        rename_map = {
            'AdrName1': 'name1', 'LandKfz': 'country', 'AdrNr': 'supplier_id',
            'AdrStraße': 'street', 'AdrPLZ': 'post_code', 'AdrOrt': 'city',
            'AdrEMail': 'email', 'AdrFax': 'fax', 'AdrTel': 'telephone'
        }
        df.rename(columns={k:v for k,v in rename_map.items() if k in df.columns}, inplace=True)
        df = self._normalize_common_fields(df)

        cols = ['supplier_id', 'company', 'name1', 'street', 'post_code', 
                'city', 'country', 'email', 'fax', 'telephone']
        
        df = df[[c for c in cols if c in df.columns]].drop_duplicates()
        return self._save_csv(df, "BUSINESS_SUPPLIER_ADDRESS.csv", data_type="BUSINESS_SUPPLIER")

