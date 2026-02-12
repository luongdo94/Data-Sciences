
import pandas as pd
import re
from datetime import datetime, timedelta
from pathlib import Path
from src.database import execute_query, read_sql_query
from src.config import OUTPUT_DIR, SQL_DIR

class ArticleImporter:
    """
    Importer class for handling Article/SKU data.
    Encapsulates logic for article basis, classification, text, variants, pricing, EAN, and packaging.
    """

    def __init__(self, diff=None, diff1=None):
        self.output_dir = OUTPUT_DIR
        self.sql_dir = SQL_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # diff: typically for SKU level differences
        # diff1: typically for Article (Basis) level differences
        self.diff = self._resolve_diff(diff, 'diff')
        self.diff1 = self._resolve_diff(diff1, 'diff1')

    def _resolve_diff(self, diff_val, diff_name):
        """Internal helper to load diff lists if not provided"""
        if diff_val is None:
            try:
                import run_comparison_standalone
                return getattr(run_comparison_standalone, diff_name, None)
            except (ImportError, AttributeError):
                return None
        if diff_val is not None and len(diff_val) == 0:
            return None
        return diff_val

    def _load_query(self, filename):
        """Helper to safely load SQL query from file"""
        sql_path = self.sql_dir / filename
        if not sql_path.exists():
            print(f"Warning: SQL file not found at {sql_path}")
            return None
        return sql_path.read_text(encoding='utf-8')

    def _save_csv(self, df, filename):
        """Standardized CSV export"""
        if df is not None and not df.empty:
            out_path = self.output_dir / filename
            df.to_csv(out_path, index=False, encoding='utf-8-sig', sep=';')
            print(f"Exported {len(df)} records to: {out_path}")
            return out_path
        return None

    # --- HELPERS ---

    def _process_text_df(self, df, id_col, lang, delete_texts, filename_prefix):
        """Common logic for processing text DataFrames"""
        # Replace NaN values with empty string and strip whitespace
        df = df.fillna('').apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        
        # Apply text processing to Pflegekennzeichnung
        if 'Pflegekennzeichnung' in df.columns:
            df['Pflegekennzeichnung'] = df['Pflegekennzeichnung'].str.split(';').str.join('\n')
            df['Pflegekennzeichnung'] = df['Pflegekennzeichnung'].apply(
                lambda x: x[:2] + '°C' + x[2:] if len(x) > 2 and '°C' not in x else x
            )

        # Define text classifications
        text_configs = [
            ('Webshoptext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Artikeltext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Katalogtext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Vertriebstext', df['ArtBem'] + ' ' + df['ArtText'] + ' ' + df['VEText'] + ' ' + df['VEText2'] + ' ' + df['VEText_SP']),
            ('Rechnungstext', df['ArtBem']),
            ('Pflegehinweise', df['Pflegekennzeichnung'])
        ]

        output_files = []
        for classification, text_content in text_configs:
            df_result = df[[id_col]].copy()
            df_result.rename(columns={id_col: 'aid'}, inplace=True)
            
            df_result['company'] = 0
            df_result['language'] = lang
            df_result['textClassification'] = classification
            df_result['text'] = text_content.str.strip()
            df_result['deleteTexts'] = delete_texts
            df_result['valid_from_text'] = datetime.now().strftime('%Y%m%d')
            df_result['valid_to_text'] = ''
            
            df_result = df_result[df_result['text'].str.len() > 0]
            if df_result.empty: continue
                
            df_result = df_result.drop_duplicates(subset=['aid', 'textClassification', 'text'])
            df_result = df_result.drop_duplicates(subset=['aid', 'textClassification'])
            
            df_result['text'] = df_result['text'].str.replace(r'\s+', ' ', regex=True)
            df_result['text'] = df_result['text'].str.replace('\r\n', '||')
            
            cols = ['aid', 'company', 'textClassification', 'text', 'language', 'deleteTexts', 'valid_from_text', 'valid_to_text']
            filename = f"{filename_prefix}_{classification.lower()}.csv"
            out = self._save_csv(df_result[cols], filename)
            if out:
                output_files.append(out)
                if classification == 'Pflegehinweise':
                    self._add_care_instruction_keywords(out)
        
        return output_files

    def _add_care_instruction_keywords(self, file_path):
        """Adds keywords to care instruction CSVs"""
        if not file_path.exists(): return
        
        df = pd.read_csv(file_path, sep=';', encoding='utf-8-sig')
        if 'text' not in df.columns: return

        df['text'] = df['text'].astype(str)
        df['text'] = 'Waschen: || ' + df['text']
        
        df['text'] = df['text'].apply(
            lambda x: x.replace('Keine chemische', '|| Reinigung: || Keine chemische')
            if 'Keine chemische' in x and 'Reinigung:' not in x else x
        )
        
        # Keywords mapping
        keywords = {'Trocknen': '||Trocknen', 'mäßig': '||Bügeln', 'mässig': '||Bügeln', 'Reinigen': '||Reinigen'}
        
        for pattern, keyword in keywords.items():
            if keyword == '||Bügeln':
                df['text'] = df['text'].apply(lambda x: x.replace('nicht heiß bügeln', ' ||Bügeln: || nicht heiß bügeln') if 'nicht heiß bügeln' in x and 'Bügeln:' not in x else x)
                df['text'] = df['text'].apply(lambda x: x.replace('nicht bügeln', ' ||Bügeln: || nicht bügeln') if 'nicht bügeln' in x and 'Bügeln:' not in x else x)
                df['text'] = df['text'].apply(lambda x: x.replace('Bügeln:', ' ||Bügeln: ||') if 'Bügeln:' in x and '||Bügeln: ||' not in x else x)
                df['text'] = df['text'].apply(lambda x: x.replace(' Bügeln ', ' ||Bügeln: || ') if ' Bügeln ' in x and 'Bügeln:' not in x else x)
                df['text'] = df['text'].apply(lambda x: x.replace(pattern, '||Bügeln: || ' + pattern) if pattern in x and 'Bügeln:' not in x else x)
            else:
                df['text'] = df['text'].apply(lambda x: x.replace(f' {pattern}', f' {keyword[2:]}: || {pattern}') if f' {keyword[2:]}:' not in x and f' {pattern}' in x else x)

        df['text'] = df['text'].str.replace(r'\s+', ' ', regex=True)
        df['text'] = df['text'].str.replace('\|\|', ' || ').str.replace('\s+', ' ').str.strip()
        df['text'] = df['text'].str.replace(' : ', ': ')
        
        df.to_csv(file_path, index=False, encoding='utf-8-sig', sep=';')

    def _generate_validity_csv(self, type_filter, output_filename, is_staffel=False):
        """Helper to generate validity CSV from Price_ERP.csv"""
        valid_path = Path("C:/Users/gia.luongdo/Python/importer_artikel_project/data/Price_ERP.csv")
        if not valid_path.exists():
            print(f"Warning: Validity file not found at {valid_path}")
            return None
            
        try:
            val_df = pd.read_csv(valid_path, sep=';', header=0, dtype=str)
            val_df.columns = [col.lower() for col in val_df.columns]
            if 'data' in val_df.columns:
                val_df = val_df.rename(columns={'data': 'pricelist'})
            
            if 'aid' in val_df.columns:
                val_df = val_df[~val_df['aid'].str.contains('_obsolet', case=False, na=False)]
            
            val_df = val_df[val_df['aktiv'] == 'ja']
            val_df = val_df[val_df['pricelist'].str.contains(type_filter, regex=True, na=False)]
            
            val_df['company'] = '1'; val_df['currency'] = 'EUR'; val_df['unit'] = 'Stk'; val_df['limitValidity'] = '0'
            val_df['valid_to'] = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
            try:
                val_df['valid_from'] = pd.to_datetime(val_df['valid_from']).dt.strftime('%Y%m%d')
            except:
                val_df['valid_from'] = datetime.now().strftime('%Y%m%d')

            if is_staffel:
                price_cols = ['price[0]', 'price[1]', 'price[2]']
                amounts = ['1', '100', '1000']
                
                def process_staffel(group):
                    prices = sorted([float(str(p).replace(',', '.')) for p in group['price']], reverse=True)
                    res = group.iloc[0].to_dict()
                    for i in range(3):
                        res[price_cols[i]] = str(prices[i]).replace('.', ',') if i < len(prices) else (str(prices[-1]).replace('.', ',') if prices else '')
                        res[f'amountFrom[{i}]'] = amounts[i]
                        res[f'discountable_idx[{i}]'] = 'J'
                        res[f'surchargeable_idx[{i}]'] = 'J'
                    return pd.Series(res)

                val_df = val_df.groupby(['aid', 'pricelist']).apply(process_staffel).reset_index(drop=True)
                cols = ['aid', 'company', 'pricelist', 'valid_from', 'valid_to', 'currency', 'unit', 'limitValidity']
                for i in range(3): cols.extend([f'price[{i}]', f'amountFrom[{i}]', f'discountable_idx[{i}]', f'surchargeable_idx[{i}]'])
                return self._save_csv(val_df[cols], output_filename)
            else:
                if 'price' in val_df.columns:
                    val_df['price'] = val_df['price'].astype(str).str.replace('.', ',', regex=False)
                if 'basicprice' in val_df.columns:
                    val_df['basicPrice'] = val_df['basicprice'].astype(str).str.replace('.', ',', regex=False)
                
                val_df['discountable_idx'] = 'J'; val_df['surchargeable_idx'] = 'J'; val_df['amountFrom'] = '1'
                val_df['discountable'] = 'J'; val_df['surchargeable'] = 'J'; val_df['use_default_sales_unit'] = 1
                
                if 'basicPrice' in val_df.columns:
                    cols = ['aid', 'company', 'basicPrice', 'currency', 'valid_from', 'valid_to', 'limitValidity', 'discountable', 'surchargeable', 'unit', 'use_default_sales_unit']
                else:
                    cols = ['aid', 'company', 'price', 'currency', 'unit', 'pricelist', 'valid_from', 'valid_to', 'limitValidity', 'amountFrom', 'discountable_idx', 'surchargeable_idx']
                
                return self._save_csv(val_df[[c for c in cols if c in val_df.columns]], output_filename)
        except Exception as e:
            print(f"Error generating validity CSV {output_filename}: {e}")
            return None

    # --- MAIN METHODS ---

    def update_sku(self):
        query = self._load_query("getall_aid_ew.sql")
        if not query: return None
        df = pd.DataFrame(execute_query(query))
        if df.empty: return None
        if 'aid1' in df.columns: df['aid'] = df['aid1'].astype(str)
        
        defaults = {
            'company': 0, 'automatic_batch_numbering_pattern': '{No,000000000}',
            'batch_management': 2, 'batch_number_range': 'Chargen',
            'batch_numbering_type': 3, 'date_requirement': 1,
            'discountable': 'ja', 'factory': 'Düsseldorf',
            'isPi': 'ja', 'isSl': 'ja', 'isSt': 'ja',
            'isShopArticle': 'ja', 'isVerifiedArticle': 'ja', 'isCatalogArticle': 'ja',
            'unitPi': 'Stk', 'unitSl': 'Stk', 'unitSt': 'Stk',
            'replacement_time': 1, 'taxPi': 'Waren', 'taxSl': 'Waren',
            'valid_to': datetime.now().strftime("%Y%m%d")
        }
        for k, v in defaults.items(): df[k] = v
        cols = ['aid', 'company', 'automatic_batch_numbering_pattern', 'batch_management', 'batch_number_range', 'batch_numbering_type', 'date_requirement', 'discountable', 'factory', 'isPi', 'isShopArticle', 'isSl', 'isSt', 'isVerifiedArticle', 'isCatalogArticle', 'unitPi', 'unitSl', 'unitSt', 'name', 'replacement_time', 'taxPi', 'taxSl', 'valid_to']
        return self._save_csv(df[[col for col in cols if col in df.columns]], "sku_update.csv")

    def import_sku_basis(self):
        sql = read_sql_query("get_skus.sql", self.diff)
        df = pd.DataFrame(execute_query(sql))
        if df.empty: return None
        
        defaults = {
            'company': 0, 'automatic_batch_numbering_pattern': '{No,000000000}',
            'batch_management': 2, 'batch_number_range': 'Chargen',
            'batch_numbering_type': 3, 'date_requirement': 1,
            'discountable': 'ja', 'factory': 'Düsseldorf',
            'isPi': 'ja', 'isSl': 'ja', 'isSt': 'ja',
            'isShopArticle': 'ja', 'isVerifiedArticle': 'ja', 'isCatalogArticle': 'ja',
            'unitPi': 'Stk', 'unitSl': 'Stk', 'unitSt': 'Stk',
            'replacement_time': 1, 'taxPi': 'Waren', 'taxSl': 'Waren',
            'valid_from': datetime.now().strftime("%Y%m%d")
        }
        for k, v in defaults.items(): df[k] = v
        df['country_of_origin'] = df['Ursprungsland'].str[:2] if 'Ursprungsland' in df.columns else ''
        cols = ['aid', 'company', 'country_of_origin', 'automatic_batch_numbering_pattern', 'batch_management', 'batch_number_range', 'batch_numbering_type', 'date_requirement', 'discountable', 'factory', 'isPi', 'isShopArticle', 'isSl', 'isSt', 'isVerifiedArticle', 'isCatalogArticle', 'unitPi', 'unitSl', 'unitSt', 'name', 'replacement_time', 'taxPi', 'taxSl', 'valid_from']
        return self._save_csv(df[[c for c in cols if c in df.columns]], "sku_basis.csv")

    def import_sku_classification(self):
        if self.diff is None: return None
        sql = read_sql_query("get_skus.sql", self.diff)
        df = pd.DataFrame(execute_query(sql))
        if df.empty: return None

        # Similar logic as artikel_classification but for SKU
        results = []
        for _, row in df.iterrows():
            def bint(c): return int(abs(row[c])) if c in row and pd.notna(row[c]) else 0
            feats = [
                ('Grammatur', row.get('Grammatur', '')),
                ('Oeko_MadeInGreen', row.get('Oeko_MadeInGreen', '')),
                ('Partnerlook', str(row.get('Artikel_Partner', ''))[:4] if pd.notna(row.get('Artikel_Partner')) else ''),
                ('Sortierung', row.get('sku_ArtSort', '')),
                ('Fabric_Herstellung', row.get('Fabric_Herstellung', '')),
                ('Material', row.get('Zusammensetzung', '')),
                ('Workwear', bint('workwear')),
                ('Produktlinie_Veredelung', bint('veredelung')),
                ('Produktlinie_Veredelungsart_Discharge', bint('discharge')),
                ('Produktlinie_Veredelungsart_DTG', bint('dtg')),
                ('Produktlinie_Veredelungsart_DYOJ', bint('dyoj')),
                ('Produktlinie_Veredelungsart_DYOP', bint('dyop')),
                ('Produktlinie_Veredelungsart_Flock', bint('flock')),
                ('Produktlinie_Veredelungsart_Siebdruck', bint('siebdruck')),
                ('Produktlinie_Veredelungsart_Stick', bint('stick')),
                ('Produktlinie_Veredelungsart_Sublimationsdruck', bint('sublimation')),
                ('Produktlinie_Veredelungsart_Transferdruck', bint('transfer')),
                ('Brand_Premium_Item', bint('premium')),
                ('Extras', bint('extras')),
                ('Kids', 1 - bint('erw') if 'erw' in row else 0),
                ('Outdoor', bint('outdoor')),
                ('Size_Oversize', bint('oversize')),
                ('Geschlecht', str(row.get('Gender', '')).replace('Kinder', '')),
                ('No_Label', bint('No_Label')),
                ('Grad_60', bint('Grad_60')),
                ('Colour_Farbe', row.get('Farbe', '')),
                ('Colour_Farbgruppe', row.get('Farbgruppe', '')),
                ('Size_Größe', row.get('Größe', '')),
                ('Size_Größenspiegel', row.get('Größenspiegel', '')),
                ('Colour_zweifarbig', bint('zweifarbig')),
                ('Ursprungsland', str(row.get('Ursprungsland', ''))[:2]),
                ('Fabric_Melange', bint('ColorMelange')),
                ('Zolltext_VZTA_aktiv_bis', pd.to_datetime(row.get('VZTA aktiv bis')).strftime('%Y%m%d') if pd.notna(row.get('VZTA aktiv bis')) else ''),
                ('Zolltext_VZTA_aktiv_von', pd.to_datetime(row.get('VZTA aktiv von')).strftime('%Y%m%d') if pd.notna(row.get('VZTA aktiv von')) else ''),
                ('New_Year', row.get('newyear', '')),
                ('Special_Offer', bint('specialoffer')),
            ]
            base = {'aid': row['aid'], 'company': 0, 'classification_system': 'Warengruppensystem', 'product_group': row.get('product_group', ''), 'product_group_superior': f"{row.get('Marke', '')}||Produktlinie||ROOT"}
            for i, (fname, fval) in enumerate(feats):
                base[f'feature[{i}]'] = fname
                base[f'feature_value[{i}]'] = fval
            results.append(base)

        res_df = pd.DataFrame(results)
        return self._save_csv(res_df, "sku_classification.csv")

    def import_sku_text(self):
        sql = read_sql_query("get_sku_text_DE.sql", self.diff)
        df = pd.DataFrame(execute_query(sql))
        if df.empty: return []
        return self._process_text_df(df, 'ArtikelCode', 'DE', 0, "sku_text")

    def import_sku_text_en(self):
        sql = read_sql_query("get_sku_text_EN.sql", self.diff)
        df = pd.DataFrame(execute_query(sql))
        if df.empty: return []
        return self._process_text_df(df, 'ArtikelCode', 'EN', 1, "sku_text_en")

    def import_sku_variant(self):
        if not self.diff: return None
        sql = read_sql_query("get_variant_sku.sql", self.diff)
        df = pd.DataFrame(execute_query(sql))
        if df.empty: return None

        groesse_col = next((col for col in df.columns if col in ['Größe', 'GrÃ¶ÃŸe', 'GrÃƒÂ¶ÃƒÅ¸e']), 'Größe')
        attrs = [('Size_Größe', df[groesse_col].fillna('')), ('Colour_Farbe', df['Farbe'].fillna('') if 'Farbe' in df.columns else ''), ('Ursprungsland', df['Ursprungsland'].str[:2].fillna('') if 'Ursprungsland' in df.columns else '')]
        
        res = pd.DataFrame({'aid': df['aid'], 'variant_aid': df['variant_aid'], 'company': 0, 'classification_system': 'Warengruppensystem'})
        for i, (name, values) in enumerate(attrs):
            res[f'attribute[{i}]'] = name
            res[f'attribute_value[{i}]'] = values
            res[f'is_mandatory[{i}]'] = 1
        
        return self._save_csv(res, "VARIANT_IMPORT - SKU-Variantenverknüpfung Import.csv")

    def import_sku_keyword(self):
        sql = read_sql_query("get_sku_keywords.sql", self.diff)
        df = pd.DataFrame(execute_query(sql))
        if df.empty: return None
        return self._save_csv(df, "sku_keyword.csv")

    def import_sku_ean(self):
        query = self._load_query("get_EAN.sql")
        df = pd.DataFrame(execute_query(query))
        if df.empty: return None
        df['QtyId'] = pd.to_numeric(df['QtyId'], errors='coerce').fillna(0).astype(int)
        df['Verpackungseinheit'] = df['Verpackungseinheit'].astype(str)
        df = df[~((df['Verpackungseinheit'] == '1') & (df['QtyId'] == 2))]
        df.loc[df['QtyId'] == 1, 'Verpackungseinheit'] = 'Stk'
        df.loc[df['IsEndsWithS'] == 1, 'Verpackungseinheit'] = 'SP'
        df.loc[df['QtyId'] != 1, 'Verpackungseinheit'] = df.loc[df['QtyId'] != 1, 'Verpackungseinheit'].replace({'1': 'Stk', '5': '5er', '10': '10er'})
        
        res = pd.DataFrame({'aid': df['ArtikelCode'], 'company': 0, 'EAN': df['EAN13'].astype(str), 'numbertype': '2', 'valid_from': datetime.now().strftime("%Y%m%d"), 'unit': df['Verpackungseinheit'], 'purpose': '1'})
        return self._save_csv(res, "article_ean.csv")

    def import_sku_gebinde(self):
        sql = read_sql_query("get_sku_gebinde.sql", self.diff)
        df = pd.DataFrame(execute_query(sql))
        if df.empty: return None
        df.rename(columns={'ArtikelCode': 'aid', 'Karton_Länge': 'length', 'Karton_Breite': 'width', 'Karton_Höhe': 'height', 'Produktgewicht': 'weight', 'Kartoneinheit': 'packaging_unit'}, inplace=True)
        for c in ['length', 'width', 'height']: 
            if c in df.columns:
                df[c] = pd.to_numeric(df[c].astype(str).str.replace(',', '.'), errors='coerce').fillna(0) * 10
                df[c] = df[c].apply(lambda x: f"{x:.1f}".replace('.', ','))
        if 'weight' in df.columns: df['weight'] = df['weight'].fillna('0').astype(str).str.replace('.', ',')
        df['length_unit'] = 'mm'; df['width_unit'] = 'mm'; df['height_unit'] = 'mm'; df['weight_unit'] = 'g'; df['is_packing_unit'] = 1; df['company'] = 1; df['content_unit'] = 'Stk'; df['packaging_factor'] = df['packaging_unit']
        if 'packaging_unit' in df.columns: df['packaging_unit'] = 'K' + df['packaging_unit'].astype(str)
        
        file1 = self._save_csv(df[['aid', 'company', 'packaging_unit', 'packaging_factor', 'length', 'width', 'height', 'is_packing_unit', 'content_unit', 'length_unit', 'width_unit', 'height_unit']], "artikel_gebinde.csv")
        if 'Verpackungseinheit' in df.columns:
            df2 = df.copy()
            df2.rename(columns={'Verpackungseinheit': 'packaging_unit_ve'}, inplace=True)
            df2['packaging_unit'] = df2['packaging_unit_ve'].astype(str) + 'er'
            df2['is_packing_unit'] = 0
            df2 = df2[~df2['packaging_unit'].isin(['1er', '1'])]
            df2['packaging_factor'] = df2['packaging_unit'].str.replace('er', '').astype(int)
            df2['length'] = 0; df2['width'] = 0; df2['height'] = 0
            self._save_csv(df2[['aid', 'packaging_unit', 'packaging_factor', 'is_packing_unit', 'company', 'content_unit', 'length_unit', 'width_unit', 'height_unit', 'length', 'width', 'height']], "ARTICLE_PACKAGING_IMPORT - SKU-Gebindedaten_VE.csv")
        return file1

    # --- ARTIKEL (BASIS) ---

    def import_artikel_basis(self):
        sql = read_sql_query("get_articles.sql", self.diff1)
        df = pd.DataFrame(execute_query(sql))
        if df.empty: return None
        defaults = {'company': 0, 'automatic_batch_numbering_pattern': '{No,000000000}', 'batch_management': 2, 'batch_number_range': 'Chargen', 'batch_numbering_type': 3, 'date_requirement': 1, 'discountable': 'ja', 'factory': 'Düsseldorf', 'isPi': 'ja', 'isSl': 'ja', 'isSt': 'ja', 'isShopArticle': 'ja', 'isVerifiedArticle': 'ja', 'isCatalogArticle': 'ja', 'unitPi': 'Stk', 'unitSl': 'Stk', 'unitSt': 'Stk', 'replacement_time': 1, 'taxPi': 'Waren', 'taxSl': 'Waren', 'valid_from': datetime.now().strftime("%Y%m%d")}
        for k, v in defaults.items(): df[k] = v
        cols = ['aid', 'company', 'automatic_batch_numbering_pattern', 'batch_management', 'batch_number_range', 'batch_numbering_type', 'date_requirement', 'discountable', 'factory', 'isPi', 'isShopArticle', 'isSl', 'isSt', 'isVerifiedArticle', 'isCatalogArticle', 'unitPi', 'unitSl', 'unitSt', 'name', 'replacement_time', 'taxPi', 'taxSl', 'valid_from']
        return self._save_csv(df[[c for c in cols if c in df.columns]], "artikel_basis.csv")

    def import_artikel_classification(self):
        sql = read_sql_query("get_article_classification.sql", self.diff1)
        df = pd.DataFrame(execute_query(sql))
        if df.empty: return None
        results = []
        for _, row in df.iterrows():
            def bint(c): return int(abs(row[c])) if c in row and pd.notna(row[c]) else 0
            feats = [('Grammatur', row.get('Grammatur', '')), ('Oeko_MadeInGreen', ''), ('Partnerlook', str(row.get('Artikel_Partner', ''))[:4] if pd.notna(row.get('Artikel_Partner')) else ''), ('Sortierung', row.get('ArtSort', '')), ('Fabric_Herstellung', row.get('Materialart', '')), ('Material', row.get('Zusammensetzung', '')), ('Workwear', bint('workwear')), ('Produktlinie_Veredelung', bint('veredelung')), ('Produktlinie_Veredelungsart_Discharge', bint('discharge')), ('Produktlinie_Veredelungsart_DTG', bint('dtg')), ('Produktlinie_Veredelungsart_DYOJ', bint('dyoj')), ('Produktlinie_Veredelungsart_DYOP', bint('dyop')), ('Produktlinie_Veredelungsart_Flock', bint('flock')), ('Produktlinie_Veredelungsart_Siebdruck', bint('siebdruck')), ('Produktlinie_Veredelungsart_Stick', bint('stick')), ('Produktlinie_Veredelungsart_Sublimationsdruck', bint('sublimation')), ('Produktlinie_Veredelungsart_Transferdruck', bint('transfer')), ('Brand_Premium_Item', bint('premium')), ('Extras', bint('extras')), ('Kids', 1 - bint('erw') if 'erw' in row else 0), ('Outdoor', bint('outdoor')), ('Size_Oversize', bint('oversize')), ('Geschlecht', row.get('Gender', '')), ('No_Label', bint('No_Label')), ('Grad_60', bint('Grad_60')), ('New_Year', row.get('New_Year', '')), ('Special_Offer', bint('specialoffer'))]
            base = {'aid': row.get('aid', ''), 'company': 0, 'classification_system': 'Warengruppensystem', 'product_group': row.get('product_group', ''), 'product_group_superior': f"{row.get('Marke', '')}||Produktlinie||ROOT"}
            for i, (fname, fval) in enumerate(feats):
                base[f'feature[{i}]'] = fname
                base[f'feature_value[{i}]'] = fval
            results.append(base)
        return self._save_csv(pd.DataFrame(results), "artikel_classification.csv")

    def import_artikel_zuordnung(self):
        sql = read_sql_query("get_article_zuordnung.sql", self.diff1)
        df = pd.DataFrame(execute_query(sql))
        if df.empty: return None
        df['aid_assigned'] = df['aid_assigned'].fillna('') + df['aid_alternativen'].fillna('')
        df_short = df[['aid', 'aid_assigned']].copy()
        df_short['aid_assigned'] = df_short['aid_assigned'].str.split(';').apply(lambda x: [i for i in x if i] if isinstance(x, list) else [])
        df_final = df_short.explode('aid_assigned')
        df_final = df_final[df_final['aid_assigned'].notna() & (df_final['aid_assigned'] != '')]
        df_final['company'] = 0; df_final['remove_assocs'] = 0; df_final['type'] = 3
        return self._save_csv(df_final[['aid', 'aid_assigned', 'company', 'remove_assocs', 'type']], "artikel_zuordnung.csv")

    def import_artikel_keyword(self):
        sql = read_sql_query("get_article_keyword.sql", self.diff1)
        df = pd.DataFrame(execute_query(sql))
        if df.empty: return None
        df['keyword'] = df['keyword'].fillna('kein Schlüsselwort').replace('', 'kein Schlüsselwort')
        df['company'] = 0
        return self._save_csv(df, "artikel_keyword.csv")

    def import_artikel_text(self):
        sql = read_sql_query("get_article_text_DE.sql", self.diff1)
        df = pd.DataFrame(execute_query(sql))
        if df.empty: return []
        return self._process_text_df(df, 'ArtikelNeu', 'DE', 0, "article_text")

    def import_artikel_text_en(self):
        sql = read_sql_query("get_article_text_EN.sql", self.diff1)
        df = pd.DataFrame(execute_query(sql))
        if df.empty: return []
        return self._process_text_df(df, 'ArtikelNeu', 'EN', 1, "article_text_en")

    def import_artikel_variant(self):
        if not self.diff1: return None
        sql = read_sql_query("get_variant.sql", self.diff1)
        df = pd.DataFrame(execute_query(sql))
        if df.empty: return None
        attrs = [('Size_Größe', df['Größe']), ('Colour_Farbe', df['Farbe'])]
        res = pd.DataFrame({'aid': df['aid'], 'variant_aid': df['sku'], 'company': 0, 'classification_system': 'Warengruppensystem'})
        for i, (name, values) in enumerate(attrs):
            res[f'attribute[{i}]'] = name
            res[f'attribute_value[{i}]'] = values
            res[f'is_mandatory[{i}]'] = 1
        return self._save_csv(res, "variant_export.csv")

    # --- PRICING ---

    def import_artikel_pricestaffeln(self):
        sql = read_sql_query("get_article_price.sql", None)
        df = pd.DataFrame(execute_query(sql)).rename(columns={'ArtikelCode': 'aid', 'Preis': 'price', 'Menge_von': 'quantity_from', 'Menge_bis': 'quantity_to'})
        if df.empty: return None
        df_pivot = df.pivot_table(index='aid', columns='Staffel', values='price', aggfunc='first').reset_index().rename(columns={1: 'p1', 2: 'p2', 3: 'p3'})
        
        results = []
        for pricelist in ['Preisstaffel 1-3', 'Preisstaffel 2-3']:
            for _, row in df_pivot.iterrows():
                p_cols = ['p1', 'p2', 'p3'] if pricelist == 'Preisstaffel 1-3' else ['p2', 'p3']
                if any(pd.notna(row.get(c)) for c in p_cols):
                    d = row.to_dict(); d['pricelist'] = pricelist; results.append(d)
        
        fdf = pd.DataFrame(results)
        fdf['company'] = '1'; fdf['currency'] = 'EUR'; fdf['unit'] = 'Stk'; fdf['valid_from'] = datetime.now().strftime("%Y%m%d"); fdf['limitValidity'] = '0'
        
        fdf['price[0]'] = fdf['p1']
        mask = fdf['pricelist'] == 'Preisstaffel 2-3'
        if not fdf[mask].empty:
            fdf.loc[mask, 'price[0]'] = fdf.loc[mask, 'p2']
            fdf.loc[mask, 'price[1]'] = fdf.loc[mask, 'p2']
            fdf.loc[mask, 'price[2]'] = fdf.loc[mask, 'p3']
        if not fdf[~mask].empty:
            fdf.loc[~mask, 'price[1]'] = fdf.loc[~mask, 'p2']
            fdf.loc[~mask, 'price[2]'] = fdf.loc[~mask, 'p3']
        
        for i in range(3):
            fdf[f'price[{i}]'] = fdf[f'price[{i}]'].astype(str).str.replace('.', ',')
            fdf[f'amountFrom[{i}]'] = ['1', '100', '1000'][i]
            fdf[f'discountable_idx[{i}]'] = 'J'
            fdf[f'surchargeable_idx[{i}]'] = 'J'
            
        cols = ['aid', 'company', 'currency', 'unit', 'pricelist', 'valid_from', 'limitValidity']
        for i in range(3): cols.extend([f'price[{i}]', f'amountFrom[{i}]', f'discountable_idx[{i}]', f'surchargeable_idx[{i}]'])
        out = self._save_csv(fdf[cols], "PRICELIST- Artikel-Preisstafeln.csv")
        self._generate_validity_csv("Preisstaffel", "PRICELIST_pricestaffeln_validity.csv", is_staffel=True)
        return out

    def import_artikel_preisstufe_3_7(self):
        sql = read_sql_query("get_article_price.sql", None)
        df = pd.DataFrame(execute_query(sql)).rename(columns={'ArtikelCode': 'aid', 'Preis': 'price'})
        if df.empty: return None
        df_pivot = df.pivot_table(index='aid', columns='Staffel', values='price', aggfunc='first').reset_index()
        
        results = []
        for i in range(3, 8):
            for _, row in df_pivot.iterrows():
                if i in row and pd.notna(row.get(i)):
                    results.append({'aid': row['aid'], 'price': str(row[i]).replace('.', ','), 'pricelist': f'Preisstufe {i}'})
                    
        fdf = pd.DataFrame(results)
        if fdf.empty: return None
        fdf['company'] = '1'; fdf['currency'] = 'EUR'; fdf['unit'] = 'Stk'; fdf['valid_from'] = datetime.now().strftime("%Y%m%d"); fdf['limitValidity'] = '0'; fdf['discountable_idx'] = 'J'; fdf['surchargeable_idx'] = 'J'; fdf['amountFrom'] = '1'
        out = self._save_csv(fdf[['aid', 'company', 'price', 'currency', 'unit', 'pricelist', 'valid_from', 'limitValidity', 'amountFrom', 'discountable_idx', 'surchargeable_idx']], "PRICELIST- Artikel-Preisstufe_3_7.csv")
        self._generate_validity_csv("Preisstufe", "PRICELIST_preisstufe3_7_validity.csv")
        return out

    def import_artikel_basicprice(self):
        sql = self._load_query("get_article_price.sql")
        df = pd.DataFrame(execute_query(sql))
        if df.empty: return None
        df['aid'] = df['ArtikelCode'].astype(str).str.strip()
        df = df.drop_duplicates(subset=['aid'], keep='first')
        df['company'] = '1'; df['currency'] = 'EUR'; df['valid_from'] = datetime.now().strftime("%Y%m%d"); df['limitValidity'] = '0'; df['discountable'] = 'J'; df['surchargeable'] = 'J'; df['unit'] = 'Stk'; df['use_default_sales_unit'] = 1
        df['basicPrice'] = df['Preis'].astype(str).str.replace('.', ',')
        cols = ['aid', 'company', 'basicPrice', 'currency', 'valid_from', 'limitValidity', 'discountable', 'surchargeable', 'unit', 'use_default_sales_unit']
        out = self._save_csv(df[cols], "PRICELIST - Artikel-Basispreis.csv")
        self._generate_validity_csv("Private_", "PRICELIST_basicprice_validity.csv")
        return out
