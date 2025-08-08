import sys
import io
from datetime import datetime
import pandas as pd
from pathlib import Path

# Define OUTPUT_DIR directly to avoid import issues
OUTPUT_DIR = Path(__file__).parent.parent / 'data' / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.database import execute_query
from run_comparison_standalone import diff, diff1

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

def read_sql_query(sql_file, aids=None):
    """Read and format SQL query with optional AIDs"""
    sql_path = project_root / "sql" / sql_file
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    
    sql_query = sql_path.read_text(encoding='utf-8').strip()
    sql_query = '\n'.join(line for line in sql_query.split('\n') 
                          if not line.strip().startswith('--'))
    
    if aids:
        formatted_aids = ["'" + str(aid).replace("'", "''") + "'" for aid in aids]
        sql_query = sql_query.replace("{aid_placeholders}", ", ".join(formatted_aids))
    
    return sql_query

def import_sku_basis():
    try:
        if not diff:
            raise ValueError("No AIDs found")
            
        print(f"Processing {len(diff)} AIDs...")
        
        df = pd.DataFrame(execute_query(read_sql_query("get_skus.sql", diff)))
        if df.empty:
            print("No data returned")
            return None

        # Add required columns
        df['company'] = 0
        df['automatic_batch_numbering_pattern'] = '{No,000000000}'
        df['batch_management'] = 2
        df['batch_number_range'] = 'Chargen'
        df['batch_numbering_type'] = 3
        df['date_requirement'] = 1
        df['discountable'] = 'ja'
        df['factory'] = 'Düsseldorf'
        df['isPi'] = df['isSl'] = df['isSt'] = 'ja'
        df['isShopArticle'] = df['isVerifiedArticle'] = df['isCatalogArticle'] = 'ja'
        df['unitPi'] = df['unitSl'] = df['unitSt'] = 'Stk'
        df['replacement_time'] = 1
        df['taxPi'] = df['taxSl'] = 'Waren'
        df['valid_from'] = datetime.now().strftime("%Y%m%d")
        df['country_of_origin'] = df['Ursprungsland'].str[:2] if 'Ursprungsland' in df else ''

        columns = ['aid', 'company', 'country_of_origin', 'automatic_batch_numbering_pattern',
                 'batch_management', 'batch_number_range', 'batch_numbering_type', 'date_requirement',
                 'discountable', 'factory', 'isPi', 'isShopArticle', 'isSl', 'isSt', 'isVerifiedArticle',
                 'isCatalogArticle', 'unitPi', 'unitSl', 'unitSt', 'name', 'replacement_time',
                 'taxPi', 'taxSl', 'valid_from']
        
        output_file = OUTPUT_DIR / "sku_basis.csv"
        df[[col for col in columns if col in df.columns]].to_csv(
            output_file, index=False, encoding='utf-8-sig', sep=';'
        )
        
        return output_file if output_file.exists() else None
            
    except Exception as e:
        print(f"Error in import_sku_basis: {e}")
        raise

def import_sku_classification():
    try:
        if not diff:
            raise ValueError("No AIDs found")
            
        print(f"Processing {len(diff)} AIDs...")
        
        df = pd.DataFrame(execute_query(read_sql_query("get_skus.sql", diff)))
        if df.empty:
            print("No data returned")
            return None
            
        # Add classification columns
        df['company'] = 0
        df['classification_system'] = 'Warengruppensystem'
        df['product_group'] = df['Marke']
        df['product_group_superior'] = df['Marke'] + '||Produktlinie||ROOT'
        df['ArtikelCode'] = df['aid']
        
        # Define feature mappings
        features = [
            ('Grammatur', df['Grammatur'].str.extract(r'(\d+)')[0]),
            ('Oeko_MadeInGreen', ''),
            ('Partnerlook', df['Artikel_Partner'].str[:4]),
            ('Sortierung', df['sku_ArtSort']),
            ('Fabric_Herstellung', df['Fabric_Herstellung']),
            ('Material', df['Zusammensetzung']),
            ('Workwear', abs(df['workwear'])),
            ('Produktlinie_Veredelung', abs(df['veredelung'])),
            ('Produktlinie_Veredelungsart_Discharge', abs(df['discharge'])),
            ('Produktlinie_Veredelungsart_DTG', abs(df['dtg'])),
            ('Produktlinie_Veredelungsart_DYOJ', abs(df['dyoj'])),
            ('Produktlinie_Veredelungsart_DYOP', abs(df['dyop'])),
            ('Produktlinie_Veredelungsart_Flock', abs(df['flock'])),
            ('Produktlinie_Veredelungsart_Siebdruck', abs(df['siebdruck'])),
            ('Produktlinie_Veredelungsart_Stick', abs(df['stick'])),
            ('Produktlinie_Veredelungsart_Sublimationsdruck', abs(df['sublimation'])),
            ('Produktlinie_Veredelungsart_Transferdruck', abs(df['transfer'])),
            ('Brand_Premium_Item', abs(df['premium'])),
            ('Extras', abs(df['extras'])),
            ('Kids', 1 - abs(df['erw'])),
            ('Outdoor', abs(df['outdoor'])),
            ('Size_Oversize', abs(df['oversize'])),
            ('Geschlecht', df['Gender'].replace('Kinder', '')),
            ('Brand_Label', abs(df['label'])),
            ('Colour_Farbe', df['Farbe']),
            ('Colour_Farbgruppe', df['Farbgruppe']),
            ('Size_Größe', df['Größe']),
            ('Size_Größenspiegel', df['Größenspiegel']),
            ('Colour_zweifarbig', df['zweifarbig']),
            ('Ursprungsland', df['Ursprungsland'].str[:2]),
            ('Fabric_Melange', df['ColorMelange']),
            ('Zolltext_VZTA_aktiv_bis', pd.to_datetime(df['VZTA aktiv bis']).dt.strftime('%Y%m%d')),
            ('Zolltext_VZTA_aktiv_von', pd.to_datetime(df['VZTA aktiv von']).dt.strftime('%Y%m%d'))
        ]
        
        # Add features to dataframe
        for i, (name, value) in enumerate(features):
            df[f'feature[{i}]'] = name
            df[f'feature_value[{i}]'] = value
        
        # Select and reorder columns
        feature_cols = [f'{x}[{i}]' for i in range(33) for x in ('feature', 'feature_value')]
        columns = ['aid', 'company', 'classification_system', 'product_group', 'product_group_superior'] + feature_cols
        
        output_file = OUTPUT_DIR / "sku_classification.csv"
        df[columns].to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_sku_classification: {e}")
        raise
def import_sku_keyword():
    try:
        if not diff:
            raise ValueError("No AIDs found")
            
        print(f"Processing {len(diff)} AIDs for keywords...")
        
        df = pd.DataFrame(execute_query(read_sql_query("get_sku_keywords.sql", diff)))
        if df.empty:
            print("No data returned")
            return None
            
        output_file = OUTPUT_DIR / "sku_keyword.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_sku_keyword: {e}")
        raise
def import_artikel_basis():
    try:
        if not diff:
            raise ValueError("No AIDs found")
            
        print(f"Processing {len(diff)} AIDs...")
        
        # Debug: Print the SQL query
        sql_query = read_sql_query("get_articles.sql", diff1)  
        # Execute the query
        df = pd.DataFrame(execute_query(sql_query))
        # Add columns with specified values
        df['company'] = 0
        df['automatic_batch_numbering_pattern'] = '{No,000000000}'
        df['batch_management'] = 2
        df['batch_number_range'] = 'Chargen'
        df['batch_numbering_type'] = 3
        df['date_requirement'] = 1
        df['discountable'] = 'ja'
        df['factory'] = 'Düsseldorf'
        df['isPi'] = 'ja'
        df['isShopArticle'] = 'ja'
        df['isSl'] = 'ja'
        df['isSt'] = 'ja'
        df['isVerifiedArticle'] = 'ja'
        df['isCatalogArticle'] = 'ja'
        df['unitPi'] = 'Stk'
        df['unitSl'] = 'Stk'
        df['unitSt'] = 'Stk'
        df['name'] = df['name']
        df['replacement_time'] = 1
        df['taxPi'] = 'Waren'
        df['taxSl'] = 'Waren'
        df['valid_from'] = datetime.now().strftime("%Y%m%d")
        # Take only first 2 characters from Ursprungsland and rename to 'country', reorder columns
        df['country_of_origin'] = df['Ursprungsland'].str[:2]
        df = df[['aid', 'company', 'country_of_origin', 'automatic_batch_numbering_pattern', 
                'batch_management', 'batch_number_range', 'batch_numbering_type', 'date_requirement', 
                'discountable', 'factory', 'isPi', 'isShopArticle', 'isSl', 'isSt', 'isVerifiedArticle', 
                'isCatalogArticle', 'unitPi', 'unitSl', 'unitSt', 'name', 'replacement_time', 'taxPi', 
                'taxSl', 'valid_from']]
        
        # Debug: Print query results
        print(f"Query executed successfully. Rows returned: {len(df)}")
        if not df.empty:
            print("data available")
        else:
            print("No data returned")
            return None

        output_file = OUTPUT_DIR / "artikel_basis.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        print(f"\nData exported to: {output_file}")
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_artikel_basis: {e}")
        raise

def extract_numbers(value):
    """Extract numbers from a string"""
    if pd.isna(value):
        return ''
    import re
    numbers = re.findall(r'\d+', str(value))
    return ''.join(numbers) if numbers else ''

def import_artikel_classification():
    """Import article classification data for AIDs not in ERP"""
    try:
        from run_comparison_standalone import diff1
        
        if not diff1:
            raise ValueError("No AIDs found")
            
        print(f"Processing {len(diff1)} AIDs for classification...")
        
        # Read and execute the query from SQL file
        sql_query = read_sql_query("get_article_classification.sql", diff1)
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No classification data returned")
            return None
        
        # Process the data
        df['Grammatur'] = df['Grammatur'].apply(extract_numbers)
        df['Gender'] = df['Gender'].replace('Kinder', '')
        
        # Define features mapping
        features = [
            ('Grammatur', 'Grammatur'),
            ('Oeko_MadeInGreen', ''),
            ('Partnerlook', lambda x: str(x['Artikel_Partner'])[:4] if pd.notna(x.get('Artikel_Partner')) else ''),
            ('Sortierung', 'ArtSort'),
            ('Fabric_Herstellung', 'Materialart'),
            ('Material', 'Zusammensetzung'),
            ('Workwear', lambda x: abs(x['workwear'])),
            ('Produktlinie_Veredelung', lambda x: abs(x['veredelung'])),
            ('Produktlinie_Veredelungsart_Discharge', lambda x: abs(x['discharge'])),
            ('Produktlinie_Veredelungsart_DTG', lambda x: abs(x['dtg'])),
            ('Produktlinie_Veredelungsart_DYOJ', lambda x: abs(x['dyoj'])),
            ('Produktlinie_Veredelungsart_DYOP', lambda x: abs(x['dyop'])),
            ('Produktlinie_Veredelungsart_Flock', lambda x: abs(x['flock'])),
            ('Produktlinie_Veredelungsart_Siebdruck', lambda x: abs(x['siebdruck'])),
            ('Produktlinie_Veredelungsart_Stick', lambda x: abs(x['stick'])),
            ('Produktlinie_Veredelungsart_Sublimationsdruck', lambda x: abs(x['sublimation'])),
            ('Produktlinie_Veredelungsart_Transferdruck', lambda x: abs(x['transfer'])),
            ('Brand_Premium_Item', lambda x: abs(x['premium'])),
            ('Extras', lambda x: abs(x['extras'])),
            ('Kids', lambda x: 1 - abs(x['erw'])),
            ('Outdoor', lambda x: abs(x['outdoor'])),
            ('Size_Oversize', lambda x: abs(x['oversize'])),
            ('Geschlecht', 'Gender'),
            ('Brand_Label', lambda x: abs(x['label']))
        ]
        
        # Create result DataFrame with required structure
        result_rows = []
        for _, row in df.iterrows():
            base_data = {
                'aid': row.get('aid', ''),
                'company': 0,
                'classification_system': 'Warengruppensystem',
                'product_group': row.get('product_group', ''),
                'product_group_superior': str(row.get('Marke', '')) + '||Produktlinie||ROOT'
            }
            
            # Add features
            for i, (feature_name, feature_value) in enumerate(features):
                base_data[f'feature[{i}]'] = feature_name
                
                if callable(feature_value):
                    try:
                        value = feature_value(row)
                    except Exception as e:
                        print(f"Error processing feature {feature_name}: {e}")
                        value = ''
                elif feature_value == '':
                    value = ''
                elif feature_value in row:
                    value = row[feature_value]
                else:
                    value = ''
                    
                base_data[f'feature_value[{i}]'] = value
            
            result_rows.append(base_data)
        
        result_df = pd.DataFrame(result_rows)
        
        # Save to CSV with _notinERP suffix
        output_file = OUTPUT_DIR / "artikel_classification.csv"
        result_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        print(f"Classification data exported to: {output_file}")
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_artikel_classification: {e}")
        import traceback
        traceback.print_exc()
        raise

def import_artikel_zuordnung():
    """Import article association data (Zuordnung) for AIDs not in ERP"""
    try:
        from run_comparison_standalone import diff1
        
        if not diff1:
            raise ValueError("No AIDs found")
            
        print(f"Processing {len(diff1)} AIDs for article associations...")
        
        # Read and execute the query from SQL file
        sql_query = read_sql_query("get_article_zuordnung.sql", diff1)
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No article association data returned")
            return None
        
        # Process the data
        # Combine aid_assigned and aid_alternativen
        df['aid_assigned'] = df['aid_assigned'].fillna('') + df['aid_alternativen'].fillna('')
        
        # Create a new DataFrame with only necessary columns and split aid_assigned by ';'
        df_short = df[['aid', 'aid_assigned']].copy()
        df_short['aid_assigned'] = df_short['aid_assigned'].str.split(';')
        
        # Remove empty strings from the list
        df_short['aid_assigned'] = df_short['aid_assigned'].apply(lambda x: [i for i in x if i] if isinstance(x, list) else [])
        
        # Explode the list to create multiple rows
        df_final = df_short.explode('aid_assigned')
        
        # Filter out rows where aid_assigned is empty
        df_final = df_final[df_final['aid_assigned'].notna() & (df_final['aid_assigned'] != '')]
        
        # Add required columns with default values
        df_final['company'] = 0
        df_final['remove_assocs'] = 0
        df_final['type'] = 3
        
        # Reorder columns
        df_final = df_final[['aid', 'aid_assigned', 'company', 'remove_assocs', 'type']]
        
        # Save to CSV
        output_file = OUTPUT_DIR / "artikel_zuordnung.csv"
        df_final.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        print(f"Article association data exported to: {output_file}")
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_artikel_zuordnung: {e}")
        import traceback
        traceback.print_exc()
        raise

def import_artikel_keyword():
    """Import article keywords for AIDs not in ERP"""
    try:
        from run_comparison_standalone import diff1
        
        if not diff1:
            raise ValueError("No AIDs found")
            
        print(f"Processing {len(diff1)} AIDs for article keywords...")
        
        # Read and execute the query from SQL file
        sql_query = read_sql_query("get_article_keyword.sql", diff1)
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No article keyword data returned")
            return None
        
        # Process the data
        # Replace empty or NaN values with 'kein Schlüsselwort'
        df['keyword'] = df['keyword'].fillna('kein Schlüsselwort')
        df['keyword'] = df['keyword'].replace('', 'kein Schlüsselwort')
        
        # Add columns with specified values
        df['company'] = 0
        df['language'] = 'DE'
        df['separator'] = ','
        
        # Reorder columns
        df = df[['aid', 'company', 'keyword', 'language', 'separator']]
        
        # Save to CSV
        output_file = OUTPUT_DIR / "artikel_keyword.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        print(f"Article keyword data exported to: {output_file}")
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_artikel_keyword: {e}")
        import traceback
        traceback.print_exc()
        raise

# This allows the script to be run directly
if __name__ == "__main__":
    import_sku_basis()
    import_sku_classification()
    import_sku_keyword()
    import_artikel_basis()
    import_artikel_classification()
    import_artikel_zuordnung()
    import_artikel_keyword()