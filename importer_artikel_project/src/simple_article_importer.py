import io
import os
import re
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Union, Any
from datetime import timedelta

# Add project root to Python path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.database import get_sql_server_connection as get_connection, execute_query, read_sql_query

# Define OUTPUT_DIR
OUTPUT_DIR = project_root / 'data' / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Set up console encoding
# if sys.stdout.encoding != 'utf-8':
#     sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
# if sys.stderr.encoding != 'utf-8':
#     sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
def extract_numbers(value):
    #Extract numbers from a string
    if pd.isna(value):
        return ''
    import re
    numbers = re.findall(r'\d+', str(value))
    return ''.join(numbers) if numbers else ''

def update_sku():
    try:
        # Execute query without diff filter
        df = pd.DataFrame(execute_query(read_sql_query("getall_aid_ew.sql")))
        if df.empty:
            return None
    
        # Map aid1 to aid if it exists
        if 'aid1' in df.columns:
            df['aid'] = df['aid1'].astype(str)
        
        # Add required columns similar to import_sku_basis
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
        df['valid_to'] = datetime.now().strftime("%Y%m%d")
        #df['country_of_origin'] = df['Ursprungsland'].str[:2] if 'Ursprungsland' in df else ''

        
        
        # Define columns for the update file
        columns = [
            'aid', 'company', 'automatic_batch_numbering_pattern',
            'batch_management', 'batch_number_range', 'batch_numbering_type', 'date_requirement',
            'discountable', 'factory', 'isPi', 'isShopArticle', 'isSl', 'isSt', 'isVerifiedArticle',
            'isCatalogArticle', 'unitPi', 'unitSl', 'unitSt', 'name', 'replacement_time',
            'taxPi', 'taxSl', 'valid_to',
        ]
        
        # Only include columns that exist in the dataframe
        output_columns = [col for col in columns if col in df.columns]
        
        output_file = OUTPUT_DIR / "sku_update.csv"
        df[output_columns].to_csv(
            output_file, index=False, encoding='utf-8-sig', sep=';'
        )
        
        return output_file if output_file.exists() else None
            
    except Exception as e:
        print(f"Error in update_sku: {e}")
        import traceback
        traceback.print_exc()
        return None

def import_sku_basis(diff=None):
    try:
        # Allow diff to be passed in or imported lazily; if none, process all SKUs
        if diff is None:
            try:
                from run_comparison_standalone import diff as _diff
                if _diff:
                    diff = _diff
            except Exception:
                diff = None

        # If a diff list was provided but is empty, treat as None
        if diff is not None and len(diff) == 0:
            diff = None

        # If diff is provided, pass it to the SQL helper; otherwise let the SQL omit AID filtering
        df = pd.DataFrame(execute_query(read_sql_query("get_skus.sql", diff)))
        if df.empty:
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
        raise

def import_sku_classification(diff=None):
    try:
        # Allow diff to be passed in or imported lazily; if none, process all SKUs
        if diff is None:
            try:
                from run_comparison_standalone import diff as _diff
                if _diff:
                    diff = _diff
            except Exception:
                diff = None

        # If a diff list was provided but is empty, treat as None
        if diff is not None and len(diff) == 0:
            diff = None
            
        if diff is None:
            return None
        
        # If diff is provided, pass it to the SQL helper; otherwise let the SQL omit AID filtering
        df = pd.DataFrame(execute_query(read_sql_query("get_skus.sql", diff)))
        if df.empty:
            return None
            
        # Add classification columns
        df['company'] = 0
        df['classification_system'] = 'Warengruppensystem'
        df['product_group_superior'] = df['Marke'] + '||Produktlinie||ROOT'
        df['ArtikelCode'] = df['aid']
        
        # Define feature mappings
        features = [
            ('Grammatur', df['Grammatur'].str.extract(r'(\d+)')[0] if 'Grammatur' in df else ''),
            ('Oeko_MadeInGreen', df['Oeko_MadeInGreen'] if 'Oeko_MadeInGreen' in df else ''),
            ('Partnerlook', df['Artikel_Partner'].str[:4] if 'Artikel_Partner' in df else ''),
            ('Sortierung', df['sku_ArtSort'] if 'sku_ArtSort' in df else ''),
            ('Fabric_Herstellung', df['Fabric_Herstellung'] if 'Fabric_Herstellung' in df else ''),
            ('Material', df['Zusammensetzung'] if 'Zusammensetzung' in df else ''),
            ('Workwear', abs(df['workwear']) if 'workwear' in df else 0),
            ('Produktlinie_Veredelung', abs(df['veredelung']) if 'veredelung' in df else 0),
            ('Produktlinie_Veredelungsart_Discharge', abs(df['discharge']) if 'discharge' in df else 0),
            ('Produktlinie_Veredelungsart_DTG', abs(df['dtg']) if 'dtg' in df else 0),
            ('Produktlinie_Veredelungsart_DYOJ', abs(df['dyoj']) if 'dyoj' in df else 0),
            ('Produktlinie_Veredelungsart_DYOP', abs(df['dyop']) if 'dyop' in df else 0),
            ('Produktlinie_Veredelungsart_Flock', abs(df['flock']) if 'flock' in df else 0),
            ('Produktlinie_Veredelungsart_Siebdruck', abs(df['siebdruck']) if 'siebdruck' in df else 0),
            ('Produktlinie_Veredelungsart_Stick', abs(df['stick']) if 'stick' in df else 0),
            ('Produktlinie_Veredelungsart_Sublimationsdruck', abs(df['sublimation']) if 'sublimation' in df else 0),
            ('Produktlinie_Veredelungsart_Transferdruck', abs(df['transfer']) if 'transfer' in df else 0),
            ('Brand_Premium_Item', abs(df['premium']) if 'premium' in df else 0),
            ('Extras', abs(df['extras']) if 'extras' in df else 0),
            ('Kids', 1 - abs(df['erw']) if 'erw' in df else 0),
            ('Outdoor', abs(df['outdoor']) if 'outdoor' in df else 0),
            ('Size_Oversize', abs(df['oversize']) if 'oversize' in df else 0),
            ('Geschlecht', df['Gender'].replace('Kinder', '') if 'Gender' in df else ''),
            ('Brand_Label', abs(df['label']) if 'label' in df else 0),
            ('Colour_Farbe', df['Farbe'] if 'Farbe' in df else ''),
            ('Colour_Farbgruppe', df['Farbgruppe'] if 'Farbgruppe' in df else ''),
            ('Size_Größe', df['Größe'] if 'Größe' in df else ''),
            ('Size_Größenspiegel', df['Größenspiegel'] if 'Größenspiegel' in df else ''),
            ('Colour_zweifarbig', df['zweifarbig'] if 'zweifarbig' in df else ''),
            ('Ursprungsland', df['Ursprungsland'].str[:2] if 'Ursprungsland' in df else ''),
            ('Fabric_Melange', df['ColorMelange'] if 'ColorMelange' in df else ''),
            ('Zolltext_VZTA_aktiv_bis', pd.to_datetime(df['VZTA aktiv bis']).dt.strftime('%Y%m%d') if 'VZTA aktiv bis' in df else ''),
            ('Zolltext_VZTA_aktiv_von', pd.to_datetime(df['VZTA aktiv von']).dt.strftime('%Y%m%d') if 'VZTA aktiv von' in df else ''),
            ('New_Year', df['newyear'] if 'newyear' in df else ''),   
            ('Special_Offer', abs(df['specialoffer']) if 'specialoffer' in df else 0),
        ]
        
        # Add features to dataframe
        for i, (name, value) in enumerate(features):
            df[f'feature[{i}]'] = name
            df[f'feature_value[{i}]'] = value
        
        # Select and reorder columns
        feature_cols = [f'{x}[{i}]' for i in range(35) for x in ('feature', 'feature_value')]
        columns = ['aid', 'company', 'classification_system', 'product_group', 'product_group_superior'] + feature_cols
        
        output_file = OUTPUT_DIR / "sku_classification.csv"
        df[[col for col in columns if col in df.columns]].to_csv(
            output_file, index=False, encoding='utf-8-sig', sep=';'
        )
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        raise
def import_sku_keyword(diff=None):
    try:
        # Allow diff to be passed in or imported lazily; if none, process all SKUs
        if diff is None:
            try:
                from run_comparison_standalone import diff as _diff
                if _diff:
                    diff = _diff
            except Exception:
                diff = None

        # Read the SQL query directly
        with open(Path(__file__).parent.parent / 'sql' / 'get_sku_keywords.sql', 'r', encoding='utf-8') as f:
            sql_query = f.read()
        
        # Apply diff filtering if provided
        if diff is not None and len(diff) > 0:
            formatted_aids = ["'" + str(aid).replace("'", "''") + "'" for aid in diff]
            sql_query = sql_query.replace('{aid_placeholders}', ", ".join(formatted_aids))
        else:
            # Remove the AID filter if no diff is provided
            sql_query = sql_query.replace('AND sku.ArtikelCode IN ({aid_placeholders})', '')
        
        # Execute the query
        df = pd.DataFrame(execute_query(sql_query))
        if df.empty:
            return None
            
        output_file = OUTPUT_DIR / "sku_keyword.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        raise
def import_artikel_basis(diff=None):
    try:
        # Allow diff to be passed in or imported lazily; if none, process all articles
        if diff is None:
            try:
                from run_comparison_standalone import diff1 as _diff
                if _diff:
                    diff = _diff
            except (ImportError, FileNotFoundError):
                diff = None
        
        # If a diff list was provided but is empty, treat as None
        if diff is not None and len(diff) == 0:
            diff = None
        
        # Read the SQL query from file
        with open(Path(__file__).parent.parent / 'sql' / 'get_articles.sql', 'r', encoding='utf-8') as f:
            sql_query = f.read()
            
        # Apply diff filtering if provided
        if diff is not None and len(diff) > 0:
            formatted_aids = ["'" + str(aid).replace("'", "''") + "'" for aid in diff]
            sql_query = sql_query.replace('{aid_placeholders}', ", ".join(formatted_aids))
        else:
            # Remove the AID filter condition if no diff is provided
            sql_query = sql_query.replace('AND m.ArtBasis IN ({aid_placeholders})', '')
            sql_query = re.sub(r'\s*AND\s*m\.ArtBasis\s*IN\s*\(\{aid_placeholders\}\)', 
                             '', sql_query, flags=re.IGNORECASE)
        
        # Execute the query
        df = pd.DataFrame(execute_query(sql_query))
        if df.empty:
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
        
        # Define output columns
        columns = [
            'aid', 'company', 'automatic_batch_numbering_pattern',
            'batch_management', 'batch_number_range', 'batch_numbering_type', 'date_requirement',
            'discountable', 'factory', 'isPi', 'isShopArticle', 'isSl', 'isSt', 'isVerifiedArticle',
            'isCatalogArticle', 'unitPi', 'unitSl', 'unitSt', 'name', 'replacement_time',
            'taxPi', 'taxSl', 'valid_from'
        ]
        
        # Save to CSV
        output_file = OUTPUT_DIR / "artikel_basis.csv"
        df[[col for col in columns if col in df.columns]].to_csv(
            output_file, index=False, encoding='utf-8-sig', sep=';'
        )
        
        return output_file if output_file.exists() else None
            
    except Exception as e:
        raise
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
            return None

        output_file = OUTPUT_DIR / "artikel_basis.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        raise



def import_artikel_classification(diff1=None):
    # Import article classification data
    try:
        # Allow diff1 to be passed in or imported lazily; if none, process all articles
        if diff1 is None:
            try:
                from run_comparison_standalone import diff1 as _diff
                if _diff:
                    diff1 = _diff
            except (ImportError, FileNotFoundError):
                diff1 = None
        
        # If a diff1 list was provided but is empty, treat as None
        if diff1 is not None and len(diff1) == 0:
            diff1 = None
            
        # Read the SQL query from file
        with open(Path(__file__).parent.parent / 'sql' / 'get_article_classification.sql', 'r', encoding='utf-8') as f:
            sql_query = f.read()
            
        # If we have diff1, use it to filter the query
        if diff1 is not None and len(diff1) > 0:
            sql_query = read_sql_query("get_article_classification.sql", diff1)
        else:
            # Remove the AID filter if no diff1 is provided
            sql_query = sql_query.replace('AND m.ArtBasis IN ({aid_placeholders})', '')
            
        # Execute the query
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            return None
        
        # Process the data
        df['Grammatur'] = df['Grammatur'].apply(extract_numbers)
        
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
            ('Brand_Label', lambda x: abs(x['label'])),
            ('New_Year', df['New_Year'] if 'New_Year' in df else ''),   
            ('Special_Offer', lambda x: abs(x['specialoffer']) if 'specialoffer' in x else 0),
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
        
        # Save to CSV with proper encoding
        output_file = OUTPUT_DIR / "artikel_classification.csv"
        result_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise

def import_artikel_zuordnung(diff1=None):
    # Import article association data (Zuordnung)
    try:
        # Allow diff1 to be passed in or imported lazily; if none, process all articles
        if diff1 is None:
            try:
                from run_comparison_standalone import diff1 as _diff
                if _diff:
                    diff1 = _diff
            except (ImportError, FileNotFoundError):
                diff1 = None
        
        # If a diff1 list was provided but is empty, treat as None
        if diff1 is not None and len(diff1) == 0:
            diff1 = None
        
        # Read the SQL query from file
        with open(Path(__file__).parent.parent / 'sql' / 'get_article_zuordnung.sql', 'r', encoding='utf-8') as f:
            sql_query = f.read()
            
        # If we have diff1, use it to filter the query
        if diff1 is not None and len(diff1) > 0:
            sql_query = read_sql_query("get_article_zuordnung.sql", diff1)
        else:
            # Remove the entire AID filter line if no diff1 is provided
            sql_query = '\n'.join([line for line in sql_query.split('\n') if 'ArtBasis IN' not in line])
            
        # Execute the query
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
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

def import_artikel_keyword(diff1=None):
    # Import article keywords for AIDs not in ERP
    try:
        # Allow diff1 to be passed in or imported lazily; if none, process all articles
        if diff1 is None:
            try:
                from run_comparison_standalone import diff1 as _diff
                if _diff:
                    diff1 = _diff
            except (ImportError, FileNotFoundError):
                diff1 = None
        
        # If a diff1 list was provided but is empty, treat as None
        if diff1 is not None and len(diff1) == 0:
            diff1 = None
        
        # Read the SQL query from file
        with open(Path(__file__).parent.parent / 'sql' / 'get_article_keyword.sql', 'r', encoding='utf-8') as f:
            sql_query = f.read()
            
        # If we have diff1, use it to filter the query
        if diff1 is not None and len(diff1) > 0:
            print(f"Processing {len(diff1)} AIDs for article keywords...")
            sql_query = read_sql_query("get_article_keyword.sql", diff1)
        else:
            print("Processing all article data for keywords...")
            # Remove the entire AID filter line if no diff1 is provided
            sql_query = '\n'.join([line for line in sql_query.split('\n') if 'ArtBasis IN' not in line])
            
        # Execute the query
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

def import_artikel_text(diff1=None):
    try:
        # Allow diff1 to be passed in or imported lazily; if none, process all articles
        if diff1 is None:
            try:
                from run_comparison_standalone import diff1 as _diff
                if _diff:
                    diff1 = _diff
            except (ImportError, FileNotFoundError):
                diff1 = None
        
        # If a diff1 list was provided but is empty, treat as None
        if diff1 is not None and len(diff1) == 0:
            diff1 = None
        
        # Read the SQL query from file
        with open(Path(__file__).parent.parent / 'sql' / 'get_article_text_DE.sql', 'r', encoding='utf-8') as f:
            sql_query = f.read()
            
        # If we have diff1, use it to filter the query
        if diff1 is not None and len(diff1) > 0:
            print(f"Processing {len(diff1)} AIDs for article text...")
            sql_query = read_sql_query("get_article_text_DE.sql", diff1)
        else:
            print("Processing all article data for text...")
            # Remove the entire AID filter line if no diff1 is provided
            sql_query = '\n'.join([line for line in sql_query.split('\n') if 'ArtNr IN' not in line])
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No article text data returned")
            return []
            
        # Remove duplicate columns if any (e.g. ArtikelNeu from multiple tables)
        df = df.loc[:, ~df.columns.duplicated()]
        
        # Process the data
        # Replace NaN values with empty string and strip whitespace
        df = df.fillna('').apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        # Apply text processing to Pflegekennzeichnung
        df['Pflegekennzeichnung'] = df['Pflegekennzeichnung'].str.split(';').str.join('\n')
        df['Pflegekennzeichnung'] = df['Pflegekennzeichnung'].apply(lambda x: x[:2] + '°C' + x[2:] if len(x) > 2 and '°C' not in x else x)
        # Define text classifications and their corresponding text content
        text_classifications = [
            ('Webshoptext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Artikeltext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Katalogtext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Vertriebstext', df['ArtBem'] + ' ' + df['ArtText'] + ' ' + df['VEText'] + ' ' + df['VEText2'] + ' ' + df['VEText_SP']),
            ('Rechnungstext', df['ArtBem']),
            ('Pflegehinweise', df['Pflegekennzeichnung'])
        ]
        
        output_files = []
        
        for classification, text_content in text_classifications:
            # Create a copy of the base dataframe for this classification
            df_result = df[['ArtikelNeu']].copy()
            df_result.rename(columns={'ArtikelNeu': 'aid'}, inplace=True)
            
            # Add required columns
            df_result['company'] = 0
            df_result['language'] = 'DE'
            df_result['textClassification'] = classification
            df_result['text'] = text_content.str.strip()  # Remove extra whitespace
            df_result['deleteTexts'] = 0
            df_result['valid_from_text'] = datetime.now().strftime('%Y%m%d')
            df_result['valid_to_text'] = ''
            
            # Remove rows with empty text
            df_result = df_result[df_result['text'].str.len() > 0]
            
            if df_result.empty:
                print(f"No data for {classification} after removing empty text")
                continue
                
            # Remove duplicates based on aid and textClassification
            df_result = df_result.drop_duplicates(subset=['aid', 'textClassification', 'text'])
            
            # For each aid, keep only the first occurrence of each textClassification
            df_result = df_result.drop_duplicates(subset=['aid', 'textClassification'])
            
            # Clean up text: replace multiple spaces and line breaks
            df_result['text'] = df_result['text'].str.replace(r'\s+', ' ', regex=True)
            df_result['text'] = df_result['text'].str.replace('\r\n', '||')
            
            # Reorder columns
            df_result = df_result[['aid', 'company', 'textClassification', 'text', 'language', 'deleteTexts', 'valid_from_text', 'valid_to_text']]
            
            # Create output filename based on classification
            output_file = OUTPUT_DIR / f"article_text_{classification.lower()}.csv"
            df_result.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
            
            if output_file.exists():
                output_files.append(output_file)
                print(f"{classification} data exported with {len(df_result)} rows to: {output_file}")
        
        # Process Pflegehinweise file after all classifications are done
        pflegehinweise_file = OUTPUT_DIR / "article_text_pflegehinweise.csv"
        if pflegehinweise_file.exists():
            # Read with explicit UTF-8-SIG encoding to handle BOM
            df_pflegehinweise = pd.read_csv(pflegehinweise_file, sep=';', encoding='utf-8-sig')
            
            if 'text' in df_pflegehinweise.columns:
                # Ensure text is properly encoded
                df_pflegehinweise['text'] = df_pflegehinweise['text'].astype(str)
                
                # Add 'Waschen:' at the beginning with consistent formatting
                df_pflegehinweise['text'] = 'Waschen: || ' + df_pflegehinweise['text']
                
                # Handle 'Reinigung' section
                df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                    lambda x: x.replace('Keine chemische', '|| Reinigung: || Keine chemische')
                    if 'Keine chemische' in x and 'Reinigung:' not in x
                    else x
                )
                
                # Define the keywords to add with their patterns
                keywords = {
                    'Trocknen': '||Trocknen',
                    'mäßig': '||Bügeln',
                    'Reinigen': '||Reinigen'
                }
                
                # Add keywords before each section if they don't already have a colon
                for pattern, keyword in keywords.items():
                    # Special handling for BÃ¼geln to place it before 'nicht heiÃŸ bÃ¼geln', 'nicht bÃ¼geln', or 'mÃ¤ÃŸig'
                    if keyword == 'Bügeln':
                        # First check for 'nicht heiÃŸ bÃ¼geln'
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace('nicht heiß bügeln', ' ||Bügeln: || nicht heiß bügeln')
                            if 'nicht heiß bügeln' in x and 'Bügeln:' not in x
                            else x
                        )
                        # Then check for 'nicht bügeln' if 'nicht heiÃŸ bÃ¼geln' wasn't found
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace('nicht bügeln', ' ||Bügeln: || nicht bügeln')
                            if 'nicht bügeln' in x and 'Bügeln:' not in x
                            else x
                        )
                        # Handle standalone 'Bügeln' section
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace('Bügeln:', ' ||Bügeln: ||') if 'Bügeln:' in x and '||Bügeln: ||' not in x else x
                        )
                        # Handle case where 'Bügeln' appears without a colon
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace(' Bügeln ', ' ||Bügeln: || ') if ' Bügeln ' in x and 'Bügeln:' not in x and '||Bügeln: ||' not in x else x
                        )
                        # Finally check for 'mÃ¤ÃŸig' if neither of the above were found
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace('mäßig', '||Bügeln: || mäßig')
                            if 'mäßig' in x and f' {keyword}:' not in x
                            else x
                        )
                    else:
                        # Standard handling for other keywords
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace(f' {pattern}', f' {keyword}: || {pattern}') 
                            if f' {keyword}:' not in x and f' {pattern}' in x 
                            else x
                        )
                        
                        # Then ensure the format is consistent (in case only one separator was added)
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].str.replace(
                            f' {keyword}: {pattern}', 
                            f' {keyword}: || {pattern}'
                        )
                
                # Clean up any double spaces that might have been created
                df_pflegehinweise['text'] = df_pflegehinweise['text'].str.replace(r'\s+', ' ', regex=True)
                
                # Ensure consistent spacing around separators
                df_pflegehinweise['text'] = df_pflegehinweise['text'].str.replace('\|\|', ' || ').str.replace('\s+', ' ').str.strip()
                df_pflegehinweise['text'] = df_pflegehinweise['text'].str.replace(' : ', ': ')
                
                # Save with proper encoding
                df_pflegehinweise.to_csv(pflegehinweise_file, index=False, encoding='utf-8-sig', sep=';')
                print(f"Care instructions with keywords added and saved to: {pflegehinweise_file}")              
            else:
                print(f"Warning: 'text' column not found in {pflegehinweise_file}")
        
        return output_files if output_files else []
        

    except Exception as e:
        print(f"Error in import_artikel_text: {e}")
        import traceback
        traceback.print_exc()
        raise

def import_artikel_text_en(diff1=None):
    try:
        # Import diff1 lazily if not provided
        if diff1 is None:
            try:
                from run_comparison_standalone import diff1 as _diff
                if _diff:
                    diff1 = _diff
            except (ImportError, FileNotFoundError):
                diff1 = None
        
        # If a diff1 list was provided but is empty, treat as None
        if diff1 is not None and len(diff1) == 0:
            diff1 = None
        
        # Read the SQL query from file
        with open(Path(__file__).parent.parent / 'sql' / 'get_article_text_EN.sql', 'r', encoding='utf-8') as f:
            sql_query = f.read()
            
        # If we have diff1, use it to filter the query
        if diff1 is not None and len(diff1) > 0:
            print(f"Processing {len(diff1)} AIDs for English article text...")
            sql_query = read_sql_query("get_article_text_EN.sql", diff1)
        else:
            print("Processing all article data for English text...")
            # Remove the entire AID filter line if no diff1 is provided
            sql_query = '\n'.join([line for line in sql_query.split('\n') if 'ArtNr IN' not in line])
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No English article text data returned")
            return []
            
        # Remove duplicate columns if any (e.g. ArtikelNeu from multiple tables)
        df = df.loc[:, ~df.columns.duplicated()]
        
        # Process the data
        # Replace NaN values with empty string and strip whitespace
        df = df.fillna('').apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        # Apply text processing to Pflegekennzeichnung
        df['Pflegekennzeichnung'] = df['Pflegekennzeichnung'].str.split(';').str.join('\n')
        df['Pflegekennzeichnung'] = df['Pflegekennzeichnung'].apply(lambda x: x[:2] + '°C' + x[2:] if len(x) > 2 and '°C' not in x else x)
        # Define text classifications and their corresponding text content
        text_classifications = [
            ('Webshoptext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Artikeltext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Katalogtext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Vertriebstext', df['ArtBem'] + ' ' + df['ArtText'] + ' ' + df['VEText'] + ' ' + df['VEText2'] + ' ' + df['VEText_SP']),
            ('Rechnungstext', df['ArtBem']),
            ('Pflegehinweise', df['Pflegekennzeichnung'])
        ]
        
        output_files = []
        
        for classification, text_content in text_classifications:
            # Create a copy of the base dataframe for this classification
            df_result = df[['ArtikelNeu']].copy()
            df_result.rename(columns={'ArtikelNeu': 'aid'}, inplace=True)
            
            # Add required columns
            df_result['company'] = 0
            df_result['language'] = 'EN'
            df_result['textClassification'] = classification
            df_result['text'] = text_content.str.strip()  # Remove extra whitespace
            df_result['deleteTexts'] = 0
            df_result['valid_from_text'] = datetime.now().strftime('%Y%m%d')
            df_result['valid_to_text'] = ''
            
            # Remove rows with empty text
            df_result = df_result[df_result['text'].str.len() > 0]
            
            if df_result.empty:
                print(f"No English data for {classification} after removing empty text")
                continue
                
            # Remove duplicates based on aid and textClassification
            df_result = df_result.drop_duplicates(subset=['aid', 'textClassification', 'text'])
            
            # For each aid, keep only the first occurrence of each textClassification
            df_result = df_result.drop_duplicates(subset=['aid', 'textClassification'])
            
            # Clean up text: replace multiple spaces and line breaks
            df_result['text'] = df_result['text'].str.replace(r'\s+', ' ', regex=True)
            df_result['text'] = df_result['text'].str.replace('\r\n', '||')
            
            # Reorder columns
            df_result = df_result[['aid', 'company', 'textClassification', 'text', 'language', 'deleteTexts', 'valid_from_text', 'valid_to_text']]
            
            # Create output filename based on classification
            output_file = OUTPUT_DIR / f"article_text_en_{classification.lower()}.csv"
            df_result.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
            
            if output_file.exists():
                output_files.append(output_file)
                print(f"{classification} English data exported with {len(df_result)} rows to: {output_file}")
        
        # Process Pflegehinweise file after all classifications are done
        pflegehinweise_file = OUTPUT_DIR / "article_text_en_pflegehinweise.csv"
        if pflegehinweise_file.exists():
            # Read with explicit UTF-8-SIG encoding to handle BOM
            df_pflegehinweise = pd.read_csv(pflegehinweise_file, sep=';', encoding='utf-8-sig')
            
            if 'text' in df_pflegehinweise.columns:
                # Ensure text is properly encoded
                df_pflegehinweise['text'] = df_pflegehinweise['text'].astype(str)
                
                # Add 'Waschen:' at the beginning with consistent formatting
                df_pflegehinweise['text'] = 'Waschen: || ' + df_pflegehinweise['text']
                
                # Handle 'Reinigung' section
                df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                    lambda x: x.replace('Keine chemische', '|| Reinigung: || Keine chemische')
                    if 'Keine chemische' in x and 'Reinigung:' not in x
                    else x
                )
                
                # Define the keywords to add with their patterns
                keywords = {
                    'Trocknen': '||Trocknen',
                    'mäßig': '||Bügeln',
                    'Reinigen': '||Reinigen'
                }
                
                # Add keywords before each section if they don't already have a colon
                for pattern, keyword in keywords.items():
                    # Special handling for BÃ¼geln to place it before 'nicht heiÃŸ bÃ¼geln', 'nicht bÃ¼geln', or 'mÃ¤ÃŸig'
                    if keyword == 'BÃ¼geln':
                        # First check for 'nicht heiÃŸ bÃ¼geln'
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace('nicht heiß bügeln', ' ||Bügeln: || nicht heiß bügeln')
                            if 'nicht heiß bügeln' in x and 'Bügeln:' not in x
                            else x
                        )
                        # Then check for 'nicht bÃ¼geln' if 'nicht heiÃŸ bÃ¼geln' wasn't found
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace('nicht bügeln', ' ||Bügeln: || nicht bügeln')
                            if 'nicht bügeln' in x and 'Bügeln:' not in x
                            else x
                        )
                        # Handle standalone 'BÃ¼geln' section
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace('Bügeln:', ' ||Bügeln: ||') if 'Bügeln:' in x and '||Bügeln: ||' not in x else x
                        )
                        # Handle case where 'BÃ¼geln' appears without a colon
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace(' Bügeln ', ' ||Bügeln: || ') if ' Bügeln ' in x and 'Bügeln:' not in x and '||Bügeln: ||' not in x else x
                        )
                        # Finally check for 'mÃ¤ÃŸig' if neither of the above were found
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace('mässig', '||Bügeln: || mässig')
                            if 'mässig' in x and f' {keyword}:' not in x
                            else x
                        )
                    else:
                        # Standard handling for other keywords
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace(f' {pattern}', f' {keyword}: || {pattern}') 
                            if f' {keyword}:' not in x and f' {pattern}' in x 
                            else x
                        )
                        
                        # Then ensure the format is consistent (in case only one separator was added)
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].str.replace(
                            f' {keyword}: {pattern}', 
                            f' {keyword}: || {pattern}'
                        )
                
                # Clean up any double spaces that might have been created
                df_pflegehinweise['text'] = df_pflegehinweise['text'].str.replace(r'\s+', ' ', regex=True)
                
                # Ensure consistent spacing around separators
                df_pflegehinweise['text'] = df_pflegehinweise['text'].str.replace('\|\|', ' || ').str.replace('\s+', ' ').str.strip()
                df_pflegehinweise['text'] = df_pflegehinweise['text'].str.replace(' : ', ': ')
                
                # Save with proper encoding
                df_pflegehinweise.to_csv(pflegehinweise_file, index=False, encoding='utf-8-sig', sep=';')
                print(f"English care instructions with keywords added and saved to: {pflegehinweise_file}")              
            else:
                print(f"Warning: 'text' column not found in {pflegehinweise_file}")
        
        return output_files if output_files else []
        
    except Exception as e:
        print(f"Error in import_artikel_text_en: {e}")
        import traceback
        traceback.print_exc()
        raise

import re

def import_sku_text():
    try:
        # Try to import diff, but make it optional
        try:
            from run_comparison_standalone import diff
            if not diff:
                print("No AIDs found in diff. Processing all data...")
                diff = None
        except (ImportError, FileNotFoundError):
            print("Comparison module not found. Processing all data...")
            diff = None
        
        # Read the SQL query from file
        with open(Path(__file__).parent.parent / 'sql' / 'get_sku_text_DE.sql', 'r', encoding='utf-8') as f:
            sql_query = f.read()
            
        # If we have diff, use it to filter the query
        if diff is not None:
            sql_query = read_sql_query("get_sku_text_DE.sql", diff)
        else:
            # Remove the AID filter if no diff is provided
            sql_query = sql_query.replace('AND s.ArtikelCode IN ({aid_placeholders})', '')
            
        # Execute the query
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No article text data returned")
            return []
        
        # Process the data
        # Replace NaN values with empty string and strip whitespace
        df = df.fillna('').apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        
        # Apply text processing to Pflegekennzeichnung
        df['Pflegekennzeichnung'] = df['Pflegekennzeichnung'].str.split(';').str.join('\n')
        df['Pflegekennzeichnung'] = df['Pflegekennzeichnung'].apply(lambda x: x[:2] + '°C' + x[2:] if len(x) > 2 and '°C' not in x else x)

        # Define text classifications and their corresponding text content
        text_classifications = [
            ('Webshoptext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Artikeltext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Katalogtext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Vertriebstext', df['ArtBem'] + ' ' + df['ArtText'] + ' ' + df['VEText'] + ' ' + df['VEText2'] + ' ' + df['VEText_SP']),
            ('Rechnungstext', df['ArtBem']),
            ('Pflegehinweise', df['Pflegekennzeichnung'])
        ]
        
        output_files = []
        
        # Process Pflegehinweise separately
        for classification, text_content in text_classifications:
            # Create a copy of the base dataframe for this classification
            df_result = df[['ArtikelCode']].copy()
            df_result.rename(columns={'ArtikelCode': 'aid'}, inplace=True)
            
            # Add required columns
            df_result['company'] = 0
            df_result['language'] = 'DE'
            df_result['textClassification'] = classification
            df_result['text'] = text_content.str.strip()  # Remove extra whitespace
            df_result['deleteTexts'] = 0
            df_result['valid_from_text'] = datetime.now().strftime('%Y%m%d')
            df_result['valid_to_text'] = ''
            
            # Remove rows with empty text
            df_result = df_result[df_result['text'].str.len() > 0]
            
            if df_result.empty:
                print(f"No data for {classification} after removing empty text")
                continue
                
            # Remove duplicates based on aid and textClassification
            df_result = df_result.drop_duplicates(subset=['aid', 'textClassification', 'text'])
            
            # For each aid, keep only the first occurrence of each textClassification
            df_result = df_result.drop_duplicates(subset=['aid', 'textClassification'])
            
            # Clean up text: replace multiple spaces and line breaks
            df_result['text'] = df_result['text'].str.replace(r'\s+', ' ', regex=True)
            df_result['text'] = df_result['text'].str.replace('\r\n', '||')
            
            # Reorder columns
            df_result = df_result[['aid', 'company', 'textClassification', 'text', 'language', 'deleteTexts', 'valid_from_text', 'valid_to_text']]
            
            # Create output filename based on classification
            output_file = OUTPUT_DIR / f"sku_text_{classification.lower()}.csv"
            df_result.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
            
        # Process Pflegehinweise file after all classifications are done
        pflegehinweise_file = OUTPUT_DIR / "sku_text_pflegehinweise.csv"
        if pflegehinweise_file.exists():
            df_pflegehinweise = pd.read_csv(pflegehinweise_file, sep=';', encoding='utf-8-sig')
            
            # Add keywords to specific positions in the text if they're not already present
            if 'text' in df_pflegehinweise.columns:
                # Ensure text is properly encoded and clean
                df_pflegehinweise['text'] = df_pflegehinweise['text'].astype(str)
                
                # Add 'Waschen:' at the beginning of the text with consistent formatting
                df_pflegehinweise['text'] = 'Waschen: || ' + df_pflegehinweise['text']
            
            # Add 'Reinigung: ||' before 'Keine chemische' if it exists
            df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                lambda x: x.replace('Keine chemische', '|| Reinigung: || Keine chemische')
                if 'Keine chemische' in x and 'Reinigung:' not in x
                else x
            )
            
            # Define the keywords to add with their patterns
            keywords = {
                'Trocknen': '||Trocknen',
                'mäßig': '||Bügeln',
                'Reinigen': '||Reinigen'
            }
            
            # Add keywords before each section if they don't already have a colon
            for pattern, keyword in keywords.items():
                # Special handling for BÃ¼geln to place it before 'nicht heiÃŸ bÃ¼geln', 'nicht bÃ¼geln', or 'mÃ¤ÃŸig'
                if keyword == 'Bügeln':
                    # First check for 'nicht heiÃŸ bÃ¼geln'
                    df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                        lambda x: x.replace('nicht heiß bügeln', ' ||Bügeln: || nicht heiß bügeln')
                        if 'nicht heiß bügeln' in x and 'Bügeln:' not in x
                        else x
                    )
                    # Then check for 'nicht bügeln' if 'nicht heiÃŸ bÃ¼geln' wasn't found
                    df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                        lambda x: x.replace('nicht bügeln', ' ||Bügeln: || nicht bügeln')
                        if 'nicht bügeln' in x and 'Bügeln:' not in x
                        else x
                    )
                    # Handle standalone 'BÃ¼geln' section
                    df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                        lambda x: x.replace('Bügeln:', ' ||Bügeln: ||') if 'Bügeln:' in x and '||Bügeln: ||' not in x else x
                    )
                    # Handle case where 'BÃ¼geln' appears without a colon
                    df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                        lambda x: x.replace(' Bügeln ', ' ||Bügeln: || ') if 'Bügeln' in x and 'Bügeln:' not in x and '||Bügeln: ||' not in x else x
                    )
                    # Finally check for 'mÃ¤ÃŸig' if neither of the above were found
                    df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                        lambda x: x.replace('mässig', '||Bügeln: || mäßig')
                        if 'mäßig' in x and f' {keyword}:' not in x
                        else x
                    )
                else:
                    # Standard handling for other keywords
                    df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                        lambda x: x.replace(f' {pattern}', f' {keyword}: || {pattern}') 
                        if f' {keyword}:' not in x and f' {pattern}' in x 
                        else x
                    )
                    
                    # Then ensure the format is consistent (in case only one separator was added)
                    df_pflegehinweise['text'] = df_pflegehinweise['text'].str.replace(
                        f' {keyword}: {pattern}', 
                        f' {keyword}: || {pattern}'
                    )
            
            # Clean up any double spaces that might have been created
            df_pflegehinweise['text'] = df_pflegehinweise['text'].str.replace(r'\s+', ' ', regex=True)
            
            # Save the updated file
            df_pflegehinweise.to_csv(pflegehinweise_file, index=False, encoding='utf-8-sig', sep=';')
            print(f"Care instructions with keywords added and saved to: {pflegehinweise_file}")
        else:
            print(f"Warning: 'text' column not found in {pflegehinweise_file}")
        
        return output_files if output_files else None
        
    except Exception as e:
        print(f"Error in import_sku_text: {e}")
        import traceback
        traceback.print_exc()
        raise

def import_sku_text_en():
    try:
        # Make diff optional
        try:
            from run_comparison_standalone import diff
            if not diff:
                print("No AIDs found in diff. Processing all data...")
                diff = None
        except (ImportError, FileNotFoundError):
            print("Comparison module not found. Processing all data...")
            diff = None
        
        # Read the SQL query from file
        with open(Path(__file__).parent.parent / 'sql' / 'get_sku_text_EN.sql', 'r', encoding='utf-8') as f:
            sql_query = f.read()
            
        # If we have diff, use it to filter the query
        if diff is not None:
            sql_query = read_sql_query("get_sku_text_EN.sql", diff)
        else:
            # Remove the AID filter if no diff is provided
            sql_query = sql_query.replace('AND s.ArtikelCode IN ({aid_placeholders})', '')
            
        # Execute the query
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No English article text data returned")
            return []
        
        # Process the data
        # Replace NaN values with empty string and strip whitespace
        df = df.fillna('').apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        
        # Apply text processing to Pflegekennzeichnung
        df['Pflegekennzeichnung'] = df['Pflegekennzeichnung'].str.split(';').str.join('\n')
        df['Pflegekennzeichnung'] = df['Pflegekennzeichnung'].apply(lambda x: x[:2] + '°C' + x[2:] if len(x) > 2 and '°C' not in x else x)

        # Define text classifications and their corresponding text content
        text_classifications = [
            ('Webshoptext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Artikeltext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Katalogtext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Vertriebstext', df['ArtBem'] + ' ' + df['ArtText'] + ' ' + df['VEText'] + ' ' + df['VEText2'] + ' ' + df['VEText_SP']),
            ('Rechnungstext', df['ArtBem']),
            ('Pflegehinweise', df['Pflegekennzeichnung'])
        ]
        
        output_files = []
        
        # Process Pflegehinweise separately
        for classification, text_content in text_classifications:
            # Create a copy of the base dataframe for this classification
            df_result = df[['ArtikelCode']].copy()
            df_result.rename(columns={'ArtikelCode': 'aid'}, inplace=True)
            
            # Add required columns
            df_result['company'] = 0
            df_result['language'] = 'EN'
            df_result['textClassification'] = classification
            df_result['text'] = text_content.str.strip()  # Remove extra whitespace
            df_result['deleteTexts'] = 0
            df_result['valid_from_text'] = datetime.now().strftime('%Y%m%d')
            df_result['valid_to_text'] = ''
            
            # Remove rows with empty text
            df_result = df_result[df_result['text'].str.len() > 0]
            
            if df_result.empty:
                print(f"No English data for {classification} after removing empty text")
                continue
                
            # Remove duplicates based on aid and textClassification
            df_result = df_result.drop_duplicates(subset=['aid', 'textClassification', 'text'])
            
            # For each aid, keep only the first occurrence of each textClassification
            df_result = df_result.drop_duplicates(subset=['aid', 'textClassification'])
            
            # Clean up text: replace multiple spaces and line breaks
            df_result['text'] = df_result['text'].str.replace(r'\s+', ' ', regex=True)
            df_result['text'] = df_result['text'].str.replace('\r\n', '||')
            
            # Reorder columns
            df_result = df_result[['aid', 'company', 'textClassification', 'text', 'language', 'deleteTexts', 'valid_from_text', 'valid_to_text']]
            
            # Create output filename based on classification
            output_file = OUTPUT_DIR / f"sku_text_en_{classification.lower()}.csv"
            df_result.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
            output_files.append(output_file)
            
        # Process Pflegehinweise file after all classifications are done
        pflegehinweise_file = OUTPUT_DIR / "sku_text_en_pflegehinweise.csv"
        if pflegehinweise_file.exists():
            df_pflegehinweise = pd.read_csv(pflegehinweise_file, sep=';', encoding='utf-8-sig')
            
            # Add keywords to specific positions in the text if they're not already present
            if 'text' in df_pflegehinweise.columns:
                # Ensure text is properly encoded and clean
                df_pflegehinweise['text'] = df_pflegehinweise['text'].astype(str)
                
                # Add 'Waschen:' at the beginning of the text with consistent formatting
                df_pflegehinweise['text'] = 'Waschen: || ' + df_pflegehinweise['text']
            
                # Add 'Reinigung: ||' before 'Keine chemische' if it exists
                df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                    lambda x: x.replace('Keine chemische', '|| Reinigung: || Keine chemische')
                    if 'Keine chemische' in x and 'Reinigung:' not in x
                    else x
                )
                
                # Define the keywords to add with their patterns
                keywords = {
                    'Trocknen': '||Trocknen',
                    'mäßig': '||Bügeln',
                    'Reinigen': '||Reinigen'
                }
                
                # Add keywords before each section if they don't already have a colon
                for pattern, keyword in keywords.items():
                    # Special handling for BÃ¼geln to place it before 'nicht heiÃŸ bÃ¼geln', 'nicht bÃ¼geln', or 'mÃ¤ÃŸig'
                    if keyword == 'Bügeln':
                        # First check for 'nicht heiÃŸ bÃ¼geln'
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace('nicht heiß bügeln', ' ||Bügeln: || nicht heiß bügeln')
                            if 'nicht heiß bügeln' in x and 'Bügeln:' not in x
                            else x
                        )
                        # Then check for 'nicht bügeln' if 'nicht heiÃŸ bÃ¼geln' wasn't found
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace('nicht bügeln', ' ||Bügeln: || nicht bügeln')
                            if 'nicht bügeln' in x and 'Bügeln:' not in x
                            else x
                        )
                        # Handle standalone 'BÃ¼geln' section
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace('Bügeln:', ' ||Bügeln: ||') if 'Bügeln:' in x and '||Bügeln: ||' not in x else x
                        )
                        # Handle case where 'BÃ¼geln' appears without a colon
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace(' Bügeln ', ' ||Bügeln: || ') if 'Bügeln' in x and 'Bügeln:' not in x and '||Bügeln: ||' not in x else x
                        )
                        # Finally check for 'mÃ¤ÃŸig' if neither of the above were found
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace('mässig', '||Bügeln: || mäßig')
                            if 'mäßig' in x and f' {keyword}:' not in x
                            else x
                        )
                    else:
                        # Standard handling for other keywords
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].apply(
                            lambda x: x.replace(f' {pattern}', f' {keyword}: || {pattern}') 
                            if f' {keyword}:' not in x and f' {pattern}' in x 
                            else x
                        )
                        
                        # Then ensure the format is consistent (in case only one separator was added)
                        df_pflegehinweise['text'] = df_pflegehinweise['text'].str.replace(
                            f' {keyword}: {pattern}', 
                            f' {keyword}: || {pattern}'
                        )
                
                # Clean up any double spaces that might have been created
                df_pflegehinweise['text'] = df_pflegehinweise['text'].str.replace(r'\s+', ' ', regex=True)
                
                # Save the updated file
                df_pflegehinweise.to_csv(pflegehinweise_file, index=False, encoding='utf-8-sig', sep=';')
                print(f"English care instructions with keywords added and saved to: {pflegehinweise_file}")
            else:
                print(f"Warning: 'text' column not found in {pflegehinweise_file}")
        
        return output_files if output_files else None
        
    except Exception as e:
        print(f"Error in import_sku_text_en: {e}")
        import traceback
        traceback.print_exc()
        raise

def import_artikel_variant():
    try:
        from run_comparison_standalone import diff1
        
        if not diff1:
            raise ValueError("No AIDs found in diff")
            
        print(f"Processing {len(diff1)} AIDs for variants...")
        
        # Read and execute the query from SQL file
        sql_query = read_sql_query("get_variant.sql", diff1)
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No variant data returned from database")
            return None
        
        # Process the data
        #df['company'] = 0
        #df['classification_system'] = 'Warengruppensystem'
        
        # Handle encoding for 'GrÃ¶ÃŸe' column - check both possible encodings
        groesse_col = 'GrÃ¶ÃŸe' if 'GrÃ¶ÃŸe' in df.columns else 'GrÃƒÂ¶ÃƒÅ¸e'
        farbe_col = 'Farbe' if 'Farbe' in df.columns else 'Farbe'  # Keep as is, but check if exists
        
        if groesse_col not in df.columns:
            print(f"Error: Could not find size column in: {df.columns.tolist()}")
            return None
            
        if farbe_col not in df.columns:
            print(f"Error: Could not find color column in: {df.columns.tolist()}")
            return None
        
        # Define attributes mapping with proper values
        attributes = [
            ('Size_GrÃ¶ÃŸe', df[groesse_col]),
            ('Colour_Farbe', df[farbe_col]),
        ]
        
        # Add attributes to dataframe and create is_mandatory  columns
        for i, (name, values) in enumerate(attributes):
            df[f'attribute[{i}]'] = name
            df[f'attribute_value[{i}]'] = values
            df[f'is_mandatory[{i}]'] = 1  # Temporary unique names
        
        # Create a list of columns for the final output
        output_columns = ['aid', 'variant_aid', 'company', 'classification_system']
        for i in range(len(attributes)):
            output_columns.extend([f'attribute[{i}]', f'attribute_value[{i}]', f'is_mandatory[{i}]'])
        
        # Create result DataFrame with all required columns
        result_df = pd.DataFrame(index=df.index)
        
        # Add required columns
        result_df['aid'] = df['aid']
        result_df['variant_aid'] = df['sku']  # Using 'sku' from SQL query as variant_aid
        result_df['company'] = 0
        result_df['classification_system'] = 'Warengruppensystem'
        
        # Add attribute columns and is_mandatory  columns
        for i in range(len(attributes)):
            result_df[f'attribute[{i}]'] = df.get(f'attribute[{i}]', '')
            result_df[f'attribute_value[{i}]'] = df.get(f'attribute_value[{i}]', '')
            result_df[f'is_mandatory[{i}]'] = df.get(f'is_mandatory[{i}]', 1)
        
        # Reorder columns to match the desired output
        result_df = result_df[output_columns]

        
        # Save to CSV with proper encoding
        output_file = OUTPUT_DIR / "variant_export.csv"
        result_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        print(f"Total variants processed: {len(result_df)}")
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_variant: {e}")
        import traceback
        traceback.print_exc()
        return None
        
    except Exception as e:
        print(f"Error in import_variant: {e}")
        import traceback
        traceback.print_exc()
        raise
    
def import_sku_variant():
    try:
        from run_comparison_standalone import diff
        
        if not diff:
            raise ValueError("No AIDs found in diff")
            
        print(f"Processing {len(diff)} AIDs for variants...")
        
        # Read and execute the query from SQL file
        sql_query = read_sql_query("get_variant_sku.sql", diff)
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No variant data returned from database")
            return None
        
        # Define attributes mapping with proper values
        # Handle encoding for 'Größe' column - check all possible encodings
        groesse_col = next((col for col in df.columns if col in ['Größe', 'GrÃ¶ÃŸe', 'GrÃƒÂ¶ÃƒÅ¸e']), None)
        if groesse_col is None:
            print("Warning: Could not find size column in:", df.columns.tolist())
            return None
            
        attributes = [
            ('Size_Größe', df[groesse_col].fillna('')),
            ('Colour_Farbe', df['Farbe'].fillna('') if 'Farbe' in df.columns else pd.Series([''] * len(df))),
            ('Ursprungsland', df['Ursprungsland'].str[:2].fillna('') if 'Ursprungsland' in df.columns else pd.Series([''] * len(df)))
        ]
        
        # Add attributes to dataframe and create is_mandatory  columns
        for i, (name, values) in enumerate(attributes):
            df[f'attribute[{i}]'] = name
            df[f'attribute_value[{i}]'] = values
            df[f'is_mandatory[{i}]'] = 1  # Temporary unique names
        
        # Create a list of columns for the final output
        output_columns = ['aid', 'variant_aid', 'company', 'classification_system']
        for i in range(len(attributes)):
            output_columns.extend([f'attribute[{i}]', f'attribute_value[{i}]', f'is_mandatory[{i}]'])
        
        # Create result DataFrame with all required columns
        result_df = pd.DataFrame(index=df.index)
        
        # Add required columns
        result_df['aid'] = df['aid']
        result_df['variant_aid'] = df['variant_aid']  # Using 'sku' from SQL query as variant_aid
        result_df['company'] = 0
        result_df['classification_system'] = 'Warengruppensystem'
        
        # Add attribute columns and is_mandatory  columns
        for i in range(len(attributes)):
            result_df[f'attribute[{i}]'] = df.get(f'attribute[{i}]', '')
            result_df[f'attribute_value[{i}]'] = df.get(f'attribute_value[{i}]', '')
            result_df[f'is_mandatory[{i}]'] = df.get(f'is_mandatory[{i}]', 1)
        
        # Reorder columns to match the desired output
        result_df = result_df[output_columns]

        
        # Save to CSV with proper encoding
        output_file = OUTPUT_DIR / "VARIANT_IMPORT - SKU-Variantenverknüpfung Import.csv"
        result_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_variant: {e}")
        import traceback
        traceback.print_exc()
        return None
        
    except Exception as e:
        print(f"Error in import_variant: {e}")
        import traceback
        traceback.print_exc()
        raise

def import_artikel_pricestaffeln():
    try:
        # Read and execute the query from SQL file
        sql_query = read_sql_query("get_article_price.sql", None)
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No price data returned from database")
            return None
        
        # Rename columns to match expected format
        df = df.rename(columns={
            'ArtikelCode': 'aid',
            'Preis': 'price',
            'Menge_von': 'quantity_from',
            'Menge_bis': 'quantity_to'
        })
        
        # Pivot the data to get price tiers as columns using pivot_table
        df_pivot = df.pivot_table(
            index='aid',
            columns='Staffel',
            values='price',
            aggfunc='first'  # Keep the first price if there are duplicates
        ).reset_index()
        
        # Rename columns for clarity
        df_pivot = df_pivot.rename(columns={
            1: 'price_1',
            2: 'price_2',
            3: 'price_3'
        })
        
        # Create final dataframe with required structure
        result = []
        pricelists = ['Preisstaffel 1-3', 'Preisstaffel 2-3']
        
        # Create rows for each article and pricelist combination
        for _, row in df_pivot.iterrows():
            for pricelist in pricelists:
                # Only add if at least one price exists for this pricelist
                price_cols = ['price_1', 'price_2', 'price_3'] if pricelist == 'Preisstaffel 1-3' else ['price_2', 'price_3']
                if any(pd.notna(row.get(col)) and row.get(col) != '' for col in price_cols):
                    row_dict = row.to_dict()
                    row_dict['pricelist'] = pricelist
                    result.append(row_dict)
        
        final_df = pd.DataFrame(result)
        
        # Add required columns
        final_df['company'] = '1'
        final_df['currency'] = 'EUR'
        final_df['unit'] = 'Stk'
        final_df['valid_from'] = datetime.now().strftime("%Y%m%d")
        final_df['limitValidity'] = '0'
        
        # Set price tiers based on pricelist
        final_df['price[0]'] = final_df['price_1']
        final_df['amountFrom[0]'] = '1'
        final_df['discountable_idx[0]'] = 'J'
        final_df['surchargeable_idx[0]'] = 'J'
        
        if 'Preisstaffel 2-3' in final_df['pricelist'].values:
            final_df.loc[final_df['pricelist'] == 'Preisstaffel 2-3', 'price[0]'] = final_df['price_2']
            final_df.loc[final_df['pricelist'] == 'Preisstaffel 2-3', 'price[1]'] = final_df['price_2']
            final_df.loc[final_df['pricelist'] == 'Preisstaffel 2-3', 'price[2]'] = final_df['price_3']
            final_df.loc[final_df['pricelist'] == 'Preisstaffel 1-3', 'price[1]'] = final_df['price_2']
            final_df.loc[final_df['pricelist'] == 'Preisstaffel 1-3', 'price[2]'] = final_df['price_3']
            
            final_df['amountFrom[1]'] = '100'
            final_df['discountable_idx[1]'] = 'J'
            final_df['surchargeable_idx[1]'] = 'J'
            final_df['amountFrom[2]'] = '1000'
            final_df['discountable_idx[2]'] = 'J'
            final_df['surchargeable_idx[2]'] = 'J'
            # Initialize all price columns if they don't exist
            for i in range(4):
                col_name = f'price[{i}]'
                if col_name not in final_df:
                    final_df[col_name] = ''

        # Convert decimal separators from . to ,
        for i in range(3):
            col_name = f'price[{i}]'
            final_df[col_name] = final_df[col_name].astype(str).str.replace('\.', ',', regex=True)

        # Reorder columns
        columns = [
            'aid', 'company', 'currency', 'unit', 'pricelist',
            'valid_from', 'limitValidity',
            'price[0]', 'amountFrom[0]', 'discountable_idx[0]', 'surchargeable_idx[0]',
            'price[1]', 'amountFrom[1]', 'discountable_idx[1]', 'surchargeable_idx[1]',
            'price[2]', 'amountFrom[2]', 'discountable_idx[2]', 'surchargeable_idx[2]'
        ]
        final_df = final_df[columns]

        # Define the desired column order for main price data
        main_price_columns = [
            'aid', 'company', 'currency', 'unit', 'pricelist',
            'valid_from', 'limitValidity'
        ]
        
        # Add price tier columns in order
        for i in range(3):
            main_price_columns.extend([
                f'price[{i}]',
                f'amountFrom[{i}]',
                f'discountable_idx[{i}]',
                f'surchargeable_idx[{i}]'
            ])
        
        # Keep only columns that exist in the dataframe
        main_price_columns = [col for col in main_price_columns if col in final_df.columns]
        final_df = final_df[main_price_columns]
        
        # Save to CSV with proper encoding
        output_file = OUTPUT_DIR / "PRICELIST- Artikel-Preisstafeln.csv"
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')

        print(f"Preisstafeln exported successfully to: {output_file}")
        #create validity data
        valid_path = Path("C:/Users/gia.luongdo/Python/importer_artikel_project/data/Price_ERP.csv")
        if not valid_path.exists():
            print(f"Warning: Validity file not found at {valid_path}")
            return output_file if output_file.exists() else None
            
        val_df = pd.read_csv(valid_path, sep=';', header=0, dtype=str)
        val_df.columns = [col.lower() for col in val_df.columns]
        
        # Rename 'data' column to 'pricelist' if it exists
        if 'data' in val_df.columns:
            val_df = val_df.rename(columns={'data': 'pricelist'})
        # Filter out rows where aid contains '_obsolet'
        if 'aid' in val_df.columns:
            val_df = val_df[~val_df['aid'].str.contains('_obsolet', case=False, na=False)]
        # Replace dots with commas in the price column
        if 'price' in val_df.columns:
            val_df['price'] = val_df['price'].astype(str).str.replace('.', ',', regex=False)
            
        required_columns = ['aid', 'pricelist', 'price', 'min_amount', 'valid_from', 'aktiv']
        existing_columns = [col for col in required_columns if col in val_df.columns]
        val_df = val_df[existing_columns]
        val_df = val_df[val_df['aktiv'] == 'ja']
        val_df = val_df[val_df['pricelist'].str.contains('Preisstaffel', regex=True)]
        val_df['company'] = '1'
        val_df['currency'] = 'EUR'
        val_df['unit'] = 'Stk'
        val_df['limitValidity'] = '0'
        val_df['amountFrom[0]'] = '1'
        val_df['amountFrom[1]'] = '100'
        val_df['amountFrom[2]'] = '1000'
        val_df['discountable_idx[0]'] = 'J'
        val_df['surchargeable_idx[0]'] = 'J'
        # Process Preisstaffel 1-3
        staffel_1_3 = val_df[val_df['pricelist'].str.contains('Preisstaffel 1-3', case=False, na=False)].copy()
        if not staffel_1_3.empty:
            # For Preisstaffel 1-3, we need 3 price tiers
            price_cols = ['price[0]', 'price[1]', 'price[2]']
            for i in range(3):
                staffel_1_3[price_cols[i]] = ''
                staffel_1_3[f'discountable_idx[{i}]'] = 'J'
                staffel_1_3[f'surchargeable_idx[{i}]'] = 'J'
            
            # Assign prices in descending order
            def convert_price(x):
                try:
                    # Try to convert to float, handling both comma and period as decimal separators
                    if isinstance(x, str):
                        x = x.replace(',', '.')
                    return float(x)
                except (ValueError, TypeError):
                    return 0.0
                    
            prices = staffel_1_3.groupby('aid')['price'].apply(
                lambda x: x.apply(convert_price).sort_values(ascending=False).head(3).tolist()
            )
            
            # Fill in the prices
            for i in range(3):
                staffel_1_3[price_cols[i]] = staffel_1_3['aid'].map(
                    prices.apply(lambda x: str(x[i]).replace('.', ',') if i < len(x) else '')
                )
        
        # Process Preisstaffel 2-3
        staffel_2_3 = val_df[val_df['pricelist'].str.contains('Preisstaffel 2-3', case=False, na=False)].copy()
        if not staffel_2_3.empty:
            # For Preisstaffel 2-3, we need 3 price tiers
            price_cols = ['price[0]', 'price[1]', 'price[2]']
            
            # Initialize all price tiers and their properties
            for i in range(3):
                staffel_2_3[price_cols[i]] = ''
                staffel_2_3[f'discountable_idx[{i}]'] = 'J'
                staffel_2_3[f'surchargeable_idx[{i}]'] = 'J'
            
            # Get all prices for each aid and sort them in descending order
            prices = staffel_2_3.groupby('aid')['price'].apply(
                lambda x: x.apply(convert_price).sort_values(ascending=False).tolist()
            )
            
            # Fill in the prices for all three tiers
            for i in range(3):
                staffel_2_3[price_cols[i]] = staffel_2_3['aid'].map(
                    prices.apply(lambda x: str(x[i]).replace('.', ',') if i < len(x) else (str(x[-1]).replace('.', ',') if x else ''))
                )
        
        # Combine both staffels
        val_df = pd.concat([staffel_1_3, staffel_2_3], ignore_index=True)
        
        # Create a clean aid without _obsolet suffix for better duplicate detection
        val_df['clean_aid'] = val_df['aid'].str.replace('_obsolet', '')
        
        # Sort by aid and pricelist to ensure consistent duplicate removal
        val_df = val_df.sort_values(['clean_aid', 'pricelist'])
        
        # Remove duplicate rows based on clean_aid and pricelist
        # Prefer non-obsolet versions by keeping the last occurrence
        val_df = val_df.drop_duplicates(subset=['clean_aid', 'pricelist'], keep='last')
        
        # Remove the temporary clean_aid column
        val_df = val_df.drop(columns=['clean_aid'])
        
        # Remove unnecessary columns
        columns_to_drop = ['price', 'min_amount', 'aktiv']
        for col in columns_to_drop:
            if col in val_df.columns:
                val_df = val_df.drop(columns=[col])
        
        # Ensure all required columns exist
        for i in range(3):
            if f'price[{i}]' not in val_df.columns:
                val_df[f'price[{i}]'] = ''
            if f'discountable_idx[{i}]' not in val_df.columns:
                val_df[f'discountable_idx[{i}]'] = ''
            if f'surchargeable_idx[{i}]' not in val_df.columns:
                val_df[f'surchargeable_idx[{i}]'] = ''
        
        # Format valid_from to YYYYMMDD
        try:
            val_df['valid_from'] = pd.to_datetime(val_df.get('valid_from', datetime.now())).dt.strftime('%Y%m%d')
        except:
            val_df['valid_from'] = datetime.now().strftime('%Y%m%d')
            
        # Add valid_to as yesterday's date
        val_df['valid_to'] = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')

        # Define the desired column order
        base_columns = [
            'aid', 'company', 'pricelist',  # Basic information
            'valid_from', 'valid_to',       # Validity dates
            'currency', 'unit',             # Currency and unit
            'limitValidity'                 # Other flags
        ]
        
        # Add price tier columns in order: price[0], amountFrom[0], discountable_idx[0], surchargeable_idx[0], etc.
        price_tiers = []
        for i in range(3):
            price_tiers.extend([
                f'price[{i}]',
                f'amountFrom[{i}]',
                f'discountable_idx[{i}]',
                f'surchargeable_idx[{i}]'
            ])
        
        # Combine all columns and keep only those that exist in the dataframe
        all_columns = base_columns + price_tiers
        final_columns = [col for col in all_columns if col in val_df.columns]
        
        # Ensure 'aktiv' column is not included
        val_df = val_df[final_columns]
        
        # Double check and remove 'aktiv' column if it still exists
        if 'aktiv' in val_df.columns:
            val_df = val_df.drop(columns=['aktiv'])
        
        # Export merged dataframe to CSV
        validity_output = OUTPUT_DIR / "PRICELIST_pricestaffeln_validity.csv"
        val_df.to_csv(validity_output, index=False, encoding='utf-8-sig', sep=';')
        print(f"Preisstafeln_validity exported successfully to: {validity_output}")
        
        # Also reorder and export the main price data file
        if 'price' in globals() and 'main_price_output' in globals():
            main_price_columns = [
                'aid', 'company', 'currency', 'unit', 'pricelist',
                'valid_from', 'limitValidity'
            ] + price_tiers
            main_price_columns = [col for col in main_price_columns if col in price.columns]
            price = price[main_price_columns]
            price.to_csv(main_price_output, index=False, encoding='utf-8-sig', sep=';')
            print(f"Main price data exported successfully to: {main_price_output}")

    except Exception as e:
        print(f"Error in import_artikel_pricestaffeln: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def import_artikel_preisstufe_3_7():
    try:
        # Read and execute the query from SQL file
        sql_query = read_sql_query("get_article_price.sql", None)
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No price data returned from database")
            return None
        
        # Rename columns to match expected format
        df = df.rename(columns={
            'ArtikelCode': 'aid',
            'Preis': 'price',
            'Menge_von': 'quantity_from',
            'Menge_bis': 'quantity_to'
        })
        
        # Pivot the data to get price tiers as columns using pivot_table to handle duplicates
        df_pivot = df.pivot_table(
            index='aid',
            columns='Staffel',
            values='price',
            aggfunc='first'  # Keep the first price if there are duplicates
        ).reset_index()
        
        # Rename columns for clarity
        df_pivot = df_pivot.rename(columns={
            3: 'price_3',
            4: 'price_4',
            5: 'price_5',
            6: 'price_6',
            7: 'price_7'
        })
        
        # Create final dataframe with required structure
        result = []
        pricelists = ['Preisstufe 3', 'Preisstufe 4', 'Preisstufe 5', 'Preisstufe 6', 'Preisstufe 7']
        
        # Create one row per article per price level, but only if the price exists
        for _, row in df_pivot.iterrows():
            for pricelist in pricelists:
                price_level = int(pricelist.split()[-1])
                price_col = f'price_{price_level}'
                
                # Only add if price exists for this level
                if pd.notna(row.get(price_col)) and row.get(price_col) != '':
                    row_dict = row.to_dict()
                    row_dict['pricelist'] = pricelist
                    result.append(row_dict)
        
        final_df = pd.DataFrame(result)
        
        # Add required columns
        final_df['company'] = '1'
        final_df['currency'] = 'EUR'
        final_df['unit'] = 'Stk'
        final_df['valid_from'] = datetime.now().strftime("%Y%m%d")
        final_df['limitValidity'] = '0'
        final_df['discountable_idx'] = 'J'
        final_df['surchargeable_idx'] = 'J'
        final_df['amountFrom'] = '1'
        
        # Add price column based on pricelist
        final_df['price'] = 0.0
        final_df.loc[final_df['pricelist'] == 'Preisstufe 3', 'price'] = final_df['price_3']
        final_df.loc[final_df['pricelist'] == 'Preisstufe 4', 'price'] = final_df['price_4']
        final_df.loc[final_df['pricelist'] == 'Preisstufe 5', 'price'] = final_df['price_5']
        final_df.loc[final_df['pricelist'] == 'Preisstufe 6', 'price'] = final_df['price_6']
        final_df.loc[final_df['pricelist'] == 'Preisstufe 7', 'price'] = final_df['price_7']
        
        # Set price tiers based on pricelist
        final_df['price[0]'] = final_df['price_3']
        final_df['price[1]'] = final_df['price_4']
        final_df['price[2]'] = final_df['price_5']
        final_df['price[3]'] = final_df['price_6']
        final_df['price[4]'] = final_df['price_7']
        final_df['price'] = final_df['price'].astype(str).str.replace('\.', ',', regex=True)
        
        # Reorder columns
        columns = [
            'aid', 'company', 'price', 'currency', 'unit', 'pricelist', 
            'valid_from', 'limitValidity', 'amountFrom', 'discountable_idx', 'surchargeable_idx'
        ]
        final_df = final_df[columns]
        
        # Save to CSV with proper encoding and decimal comma
        output_file = OUTPUT_DIR / "PRICELIST- Artikel-Preisstufe_3_7.csv"
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        # Ensure price column is treated as string with comma decimal separator
        if 'price' in final_df.columns:
            final_df['price'] = final_df['price'].astype(str).str.replace('.', ',', regex=False)
        
        print(f"Preisstufe_3_7 exported successfully to: {output_file}")
        #create validity data
        valid_path = Path("C:/Users/gia.luongdo/Python/importer_artikel_project/data/Price_ERP.csv")
        if not valid_path.exists():
            print(f"Warning: Validity file not found at {valid_path}")
            return output_file if output_file.exists() else None
            
        val_df = pd.read_csv(valid_path, sep=';', header=0, dtype=str)
        val_df.columns = [col.lower() for col in val_df.columns]
        # Rename 'data' column to 'pricelist' if it exists
        if 'data' in val_df.columns:
            val_df = val_df.rename(columns={'data': 'pricelist'})
            
        # Define required columns, excluding 'min_amount'
        required_columns = ['aid', 'pricelist', 'price', 'valid_from', 'aktiv']
        existing_columns = [col for col in required_columns if col in val_df.columns]
        val_df = val_df[existing_columns]
        
        # Filter out rows where aid contains '_obsolet'
        if 'aid' in val_df.columns:
            val_df = val_df[~val_df['aid'].str.contains('_obsolet', case=False, na=False)]
            
        val_df = val_df[val_df['aktiv'] == 'ja']
        val_df = val_df[val_df['pricelist'].str.contains('Preisstufe', regex=True, na=False)]
        
        # Replace dots with commas in the price column
        if 'price' in val_df.columns:
            val_df['price'] = val_df['price'].astype(str).str.replace('.', ',', regex=False)
            
        val_df['discountable_idx'] = 'J'
        val_df['surchargeable_idx'] = 'J'
        val_df['amountFrom'] = '1'
        val_df['limitValidity'] = '0'
        val_df['company'] = '1'
        val_df['currency'] = 'EUR'
        val_df['unit'] = 'Stk'
        val_df['valid_to'] = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        val_df['valid_from'] = val_df['valid_from'].apply(lambda x: pd.to_datetime(x).strftime('%Y%m%d'))
    
        # Ensure 'aktiv' column is removed before export
        if 'aktiv' in val_df.columns:
            val_df = val_df.drop(columns=['aktiv'])
            
        # Export merged dataframe to CSV with unique name
        validity_output = OUTPUT_DIR / "PRICELIST_preisstufe3_7_validity.csv"
        val_df.to_csv(validity_output, index=False, encoding='utf-8-sig', sep=';')
        print(f"Preisstufe_3_7_validity exported successfully to: {validity_output}")
        
    except Exception as e:
        print(f"Error in import_artikel_preisstufe_3_7: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
        
def import_artikel_basicprice(basicprice_filename="PRICELIST - Artikel-Basispreis.csv", validity_filename="Basispreis_validity_data.csv"):
    """Import and process basic price information for articles.
    
    Args:
        basicprice_filename: Output filename for basic price data
        validity_filename: Input filename for validity dates (if any)
        
    Returns:
        Path: Path object to the generated CSV file, or None if an error occurred
    """
    try:
        sql_file = Path(__file__).parent.parent / 'sql' / 'get_article_price.sql'
        if not sql_file.exists():
            return None
            
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_query = f.read().strip()
            
        if not sql_query:
            return None
        
        result = execute_query(sql_query)
        if result is None:
            return None
            
        df = pd.DataFrame(result)
        if df.empty:
            return None
            
        price_column = 'Preis' if 'Preis' in df.columns else None
        if price_column is None:
            return None
            
        # Process the data
        df['company'] = '1'
        df[price_column] = df[price_column].astype(str).str.replace('.', ',')
        
        # Initialize basicPrice as empty string
        df['basicPrice'] = ''
        
        # Set basicPrice only for rows where Staffel is 1
        if 'Staffel' in df.columns:
            df.loc[df['Staffel'] == 1, 'basicPrice'] = df.loc[df['Staffel'] == 1, price_column]
        
        df['currency'] = 'EUR'
        df['valid_from'] = datetime.now().strftime("%Y%m%d")
        df['limitValidity'] = '0'
        df['discountable'] = 'J'
        df['surchargeable'] = 'J'
        df['unit'] = 'Stk'
        df['use_default_sales_unit'] = 1
        
        id_column = 'ArtikelCode'
        if id_column not in df.columns:
            return None
        
            
        df['aid'] = df[id_column].astype(str).str.strip()
        
        # Remove duplicate AIDs by keeping the first occurrence
        df = df.drop_duplicates(subset=['aid'], keep='first')
        
        output_columns = ['aid', 'company', 'basicPrice', 'currency', 'valid_from', 'limitValidity', 'discountable', 'surchargeable', 'unit', 'use_default_sales_unit']
        final_df = df[output_columns].copy()
        
        output_file = OUTPUT_DIR / basicprice_filename
        output_file.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        print(f"Exported {len(final_df)} basic price records")

        #create validity data
        valid_path = Path("C:/Users/gia.luongdo/Python/importer_artikel_project/data/Price_ERP.csv")
        if not valid_path.exists():
            print(f"Warning: Validity file not found at {valid_path}")
            return output_file if output_file.exists() else None
            
        val_df = pd.read_csv(valid_path, sep=';', header=0, dtype=str)
        val_df.columns = [col.lower() for col in val_df.columns]
        # Rename 'data' column to 'pricelist' if it exists
        if 'data' in val_df.columns:
            val_df = val_df.rename(columns={'data': 'pricelist', 'price': 'basicPrice'})
            
        # Define required columns, excluding 'min_amount'
        required_columns = ['aid', 'pricelist', 'basicPrice', 'valid_from', 'aktiv']
        existing_columns = [col for col in required_columns if col in val_df.columns]
        val_df = val_df[existing_columns]
        
        # Filter out rows where aid contains '_obsolet'
        if 'aid' in val_df.columns:
            val_df = val_df[~val_df['aid'].str.contains('_obsolet', case=False, na=False)]
            
        val_df = val_df[val_df['aktiv'] == 'ja']
        val_df = val_df[val_df['pricelist'].str.contains('Private_', regex=True, na=False)]
        
        # Replace dots with commas in the price column
        if 'basicPrice' in val_df.columns:
            val_df['basicPrice'] = val_df['basicPrice'].astype(str).str.replace('.', ',', regex=False)
            
        val_df['discountable'] = 'J'
        val_df['surchargeable'] = 'J'
        val_df['limitValidity'] = '0'
        val_df['company'] = '1'
        val_df['currency'] = 'EUR'
        val_df['unit'] = 'Stk'
        val_df['use_default_sales_unit'] = 1
        val_df['valid_to'] = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
        val_df['valid_from'] = val_df['valid_from'].apply(lambda x: pd.to_datetime(x).strftime('%Y%m%d'))
        val_df = val_df.drop(['aktiv', 'pricelist'], axis=1)



        # Export merged dataframe to CSV
        validity_output = OUTPUT_DIR / "PRICELIST_basicprice_validity.csv"
        val_df.to_csv(validity_output, index=False, encoding='utf-8-sig', sep=';')
    except Exception as e:
        print(f"Error in import_artikel_basicprice: {e}")
        import traceback
        traceback.print_exc()
        return None

def import_sku_EAN():
    try:
        # Make run_comparison_standalone import optional
        try:
            from run_comparison_standalone import diff, diff1
        except (ImportError, FileNotFoundError):
            diff = None
            diff1 = None

        # Read the SQL query
        with open(Path(__file__).parent.parent / 'sql' / 'get_EAN.sql', 'r', encoding='utf-8') as f:
            sql_query = f.read()
            df = pd.DataFrame(execute_query(sql_query))

        if df.empty:
            print("No EAN data found in the database")
            return None
            
        # Process the data
        df['QtyId'] = pd.to_numeric(df['QtyId'], errors='coerce').astype(int)
        df['Verpackungseinheit'] = df['Verpackungseinheit'].astype(str)
        df['IsEndsWithS'] = df['IsEndsWithS'].astype(int)
        # Set default unit to 'Stk' for QtyId = 1
        df.loc[df['QtyId'] == 1, 'Verpackungseinheit'] = 'Stk'
        df.loc[df['IsEndsWithS'] == 1, 'Verpackungseinheit'] = 'SP'
        
        # Apply mappings for other QtyId values
        mask = df['QtyId'] != 1
        df.loc[mask, 'Verpackungseinheit'] = df.loc[mask, 'Verpackungseinheit'].replace({
            '1': 'Stk',
            '5': '5er',
            '10': '10er'
        })
        
        # Prepare output DataFrame with required columns
        from datetime import datetime, timedelta
        result_df = pd.DataFrame({
            'aid': df['ArtikelCode'],
            'company': 0,
            'EAN': df['EAN13'].astype(str),
            'numbertype': '2',
            'valid_from': datetime.now().strftime("%Y%m%d"),
            #'valid_to': (datetime.now() - timedelta(days=1)).strftime("%Y%m%d"),
            'unit': df['Verpackungseinheit'],
            'purpose': '1'
        })
        
        # Ensure output directory exists
        output_file = OUTPUT_DIR / "article_ean.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Export to CSV with proper encoding
        result_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        print(f"EAN data exported successfully to: {output_file}")
        print(f"Total records exported: {len(result_df)}")
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_sku_EAN: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

# Removed duplicate import_sku_basis function

def import_sku_gebinde():
    """Import SKU packaging data (Gebinde) from database.
    
    Returns:
        Path: Path to the generated CSV file, or None if an error occurred
    """
    try:
        # Make diff optional
        try:
            from run_comparison_standalone import diff
            if not diff:
                diff = None
        except (ImportError, NameError, FileNotFoundError):
            diff = None
            
        # Read the SQL query from file
        with open(Path(__file__).parent.parent / 'sql' / 'get_sku_gebinde.sql', 'r', encoding='utf-8') as f:
            sql_query = f.read()
            
        # If we have diff, use it to filter the query
        if diff is not None and len(diff) > 0:
            print(f"Processing {len(diff)} SKUs for packaging data...")
            sql_query = read_sql_query("get_sku_gebinde.sql", diff)
        else:
            print("Processing all SKUs for packaging data...")
            # Remove any AID filtering from the query
            sql_query = '\n'.join([line for line in sql_query.split('\n') 
                                  if 'ArtNr IN' not in line and 'ArtikelCode IN' not in line])
                
        # Execute query and get data
        df = pd.DataFrame(execute_query(sql_query))
            
        if df.empty:
            print("No packaging data available")
            return None
        
        # Process each row to create packaging records
        rename_columns = {
            'ArtikelCode': 'aid',
            'Karton_Länge': 'length',
            'Karton_Breite': 'width',
            'Karton_Höhe': 'height',
            'Produktgewicht': 'weight',
            'Kartoneinheit': 'packaging_unit',
            'Verpackungseinheit': 'Verpackungseinheit'
        }
        # First rename the columns
        df = df.rename(columns=rename_columns)
        
        # Now process the renamed columns
        for col in ['length', 'width', 'height']:
            if col in df.columns:
                # Convert to numeric, handling both strings with commas and numeric types
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')
                # Multiply by 10 and format with comma as decimal
                df[col] = (df[col].fillna(0) * 10).apply(lambda x: f"{x:.1f}".replace('.', ','))
        
        # Process weight separately (don't multiply by 10)
        if 'weight' in df.columns:
            df['weight'] = df['weight'].fillna('0').astype(str).str.replace('.', ',', regex=False)
            
        # Set units and other fixed values
        df['length_unit'] = 'mm'
        df['width_unit'] = 'mm'
        df['height_unit'] = 'mm'
        df['weight_unit'] = 'g'
        df['is_packing_unit'] = 1
        df['company'] = 1
        df['content_unit'] = 'Stk'
        
        # Set packaging_factor from packaging_unit
        df['packaging_factor'] = df['packaging_unit']
        
        # Format packaging_unit with 'K' prefix
        if 'packaging_unit' in df.columns:
            df['packaging_unit'] = 'K' + df['packaging_unit'].astype(str)
            
        result_df = df
        
        # Add length, width, height, weight columns with default values if they don't exist
        for col in ['length', 'width', 'height', 'weight']:
            if col not in result_df.columns:
                result_df[col] = ''
        
        # Define output columns for the first file (without Verpackungseinheit)
        output_columns = [
            'aid', 'company', 'packaging_unit', 'packaging_factor', 
            'length', 'width', 'height', 
            'is_packing_unit', 'content_unit', 'length_unit', 'width_unit', 'height_unit'
        ]
        
        # Filter to only include columns that exist in the dataframe
        output_columns = [col for col in output_columns if col in result_df.columns]
        
        # Save the first file with standard packaging data
        output_file1 = OUTPUT_DIR / "artikel_gebinde.csv"
        result_df[output_columns].to_csv(output_file1, index=False, encoding='utf-8-sig', sep=';')

        # Only create the second file if Verpackungseinheit exists
        if 'Verpackungseinheit' in result_df.columns:
            # Rename Verpackungseinheit to packaging_unit for the second file and add 'er' to the end
            del result_df['packaging_unit']
            result_df = result_df.rename(columns={'Verpackungseinheit': 'packaging_unit'})
            result_df['packaging_unit'] = result_df['packaging_unit'].astype(str) + 'er'
            result_df['is_packing_unit'] = 0
            result_df = result_df[~(result_df['packaging_unit'].isin(['1er', '1']))]
            result_df['packaging_factor'] = result_df['packaging_unit'].str.replace('er', '').astype(int)
            result_df['length'] = 0
            result_df['width'] = 0
            result_df['height'] = 0
            result_df['length_unit'] = 'mm'
            result_df['width_unit'] = 'mm'
            result_df['height_unit'] = 'mm'
            
            
            # Define columns for the second file (with packaging_unit)
            output_columns_ve = [
                'aid', 'packaging_unit', 'packaging_factor',
                'is_packing_unit', 'company',
                'content_unit', 'length_unit', 'width_unit', 'height_unit', 'length', 'width', 'height'
            ]
            
            # Save the second file with packaging_unit
            output_file2 = OUTPUT_DIR / "ARTICLE_PACKAGING_IMPORT - SKU-Gebindedaten_VE.csv"
            result_df[output_columns_ve].to_csv(output_file2, index=False, encoding='utf-8-sig', sep=';')
            print(f"Exported {len(result_df)} packaging records (both files)")
            return output_file2 if output_file2.exists() else None
        else:
            print(f"Exported {len(result_df)} packaging records (standard file only)")
            return output_file1 if output_file1.exists() else None
        
    except Exception as e:
        print(f"Error in import_sku_gebinde: {e}")
        raise
        import traceback
        traceback.print_exc()
            
def import_order():
    try:
        sql_file = Path(__file__).parent.parent / 'sql' / 'get_order.sql'
        with open(sql_file, 'r', encoding='utf-8-sig') as f:  
            sql_query = f.read().strip()  
        if not sql_query:
            print("Error: SQL query is empty")
            return None
            
        result = execute_query(sql_query)
        if result is None or result.empty:
            return None
   
        rename_columns = {
            'OrderNr_Lang': 'txId',
            'POCode': 'txIdExternal',
            'AdrId': 'supplier_id',
            'Name': 'clerk',
            'OSDate': 'txDate',
            'OrgDatum': 'ex_txDate'
        }
        
        rename_columns = {k: v for k, v in rename_columns.items() if k in result.columns}
        result = result.rename(columns=rename_columns)
        
        result['company'] = 1
        result['order_auto'] = 1
        result['currency'] = 'USD'
        result['ex_txDate'] = result['ex_txDate'].dt.strftime("%Y%m%d")
        result['txDate'] = result['txDate'].dt.strftime("%Y%m%d")
        result['supplier_id'] = result['txId'].str[:5]
        result['txDef'] = 'Kontrakt_EWOrder'
        result['company'] = 1
            
        if 'clerk' in result.columns:
            result['clerk'] = result['clerk'].apply(
                lambda x: x.decode('utf-16-le') if isinstance(x, bytes) else str(x)
            )
        
        available_columns = ['txId', 'txIdExternal', 'supplier_id', 'clerk', 'ex_txDate', 'txDate', 'company', 'currency', 'order_auto', 'txDef', 'company']
        selected_columns = [col for col in available_columns if col in result.columns]
        result = result[selected_columns]
        
        output_file = OUTPUT_DIR / "order_data.csv"
        result.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_order: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def import_order_are_15():
    try:
        sql_file = Path(__file__).parent.parent / 'sql' / 'getorder_are_15.sql'
        with open(sql_file, 'r', encoding='utf-8-sig') as f:  
            sql_query = f.read().strip()  
        if not sql_query:
            print("Error: SQL query is empty")
            return None
            
        result = execute_query(sql_query)
        if result is None or result.empty:
            return None
   
        rename_columns = {
            'OrderNr_Lang': 'txId',
            'POCode': 'txIdExternal',
            'AdrId': 'supplier_id',
            'Name': 'clerk',
            'OSDate': 'txDate',
            'OrgDatum': 'ex_txDate',
            'OrderNr': 'OrderNr'
        }
        
        rename_columns = {k: v for k, v in rename_columns.items() if k in result.columns}
        result = result.rename(columns=rename_columns)
        
        result['company'] = 1
        result['order_auto'] = 1
        result['currency'] = 'USD'
        result['ex_txDate'] = result['ex_txDate'].dt.strftime("%Y%m%d")
        result['txDate'] = result['txDate'].dt.strftime("%Y%m%d")
        result['supplier_id'] = result['OrderNr'].str[:5]
        result['txDef'] = 'Kontrakt_EWOrder'
        result['company'] = 1
            
        if 'clerk' in result.columns:
            result['clerk'] = result['clerk'].apply(
                lambda x: x.decode('utf-16-le') if isinstance(x, bytes) else str(x)
            )
        
        available_columns = ['txId', 'txIdExternal', 'supplier_id', 'clerk', 'ex_txDate', 'txDate', 'company', 'currency', 'order_auto', 'txDef', 'company']
        selected_columns = [col for col in available_columns if col in result.columns]
        result = result[selected_columns]
        
        output_file = OUTPUT_DIR / "order_are_15_data.csv"
        result.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_order_not_15: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def import_order_pos():
    try:
        sql_file = Path(__file__).parent.parent / 'sql' / 'get_orderpos.sql'
        with open(sql_file, 'r', encoding='utf-8-sig') as f:  
            sql_query = f.read().strip()  
        if not sql_query:
            print("Error: SQL query is empty")
            return None
            
        result = execute_query(sql_query)
        if result is None or result.empty:
            return None
            
        rename_columns = {
            'OrderNr_Lang': 'txId',
            'Menge': 'quantity',
            'OPreis': 'price',
            'ArtikelCode': 'aid',
            'erfaßt_am': 'valid_from'
        }
        
        rename_columns = {k: v for k, v in rename_columns.items() if k in result.columns}
        result = result.rename(columns=rename_columns)
        
        result['company'] = 1
        result['priceUnit'] = 'Stk'
        result['supplier_id'] = result['txId'].str[:5]
        result['factory'] = 'Düsseldorf'
        result['commodity_group_path'] = ''
        result['unit'] = 'Stk'
        result['use_proc_unit_for_purchase'] = '0'
        result['supplierAid'] = ''
        result['pos_text'] = ''
        result['valid_from'] = result['valid_from'].dt.strftime("%Y%m%d")
        # Convert price to string and replace decimal point with comma
        result['price'] = result['price'].astype(str).str.replace('.', ',', regex=False)
        
            
        if 'clerk' in result.columns:
            result['clerk'] = result['clerk'].apply(
                lambda x: x.decode('utf-16-le') if isinstance(x, bytes) else str(x)
            )
        
        available_columns = ['txId', 'quantity', 'price', 'aid', 'company', 'priceUnit', 'supplier_id', 'factory', 'commodity_group_path', 'unit', 'use_proc_unit_for_purchase', 'supplierAid', 'valid_from', 'pos_text']
        selected_columns = [col for col in available_columns if col in result.columns]
        result = result[selected_columns]
        
        output_file = OUTPUT_DIR / "order_pos_data.csv"
        result.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_order: {str(e)}")
        import traceback
        traceback.print_exc()
        return None

def import_order_pos_are_15():
    try:
        output_file = OUTPUT_DIR / "order_pos_are_15_data.csv"
        
        # Create empty DataFrame with expected columns
        columns = ['txId', 'quantity', 'price', 'aid', 'company', 'priceUnit', 
                  'supplier_id', 'factory', 'commodity_group_path', 'unit', 
                  'use_proc_unit_for_purchase', 'supplierAid', 'valid_from', 'pos_text']
        empty_df = pd.DataFrame(columns=columns)
        
        # Write empty file with headers
        empty_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        print(f"Created empty file: {output_file}")
        
        sql_file = Path(__file__).parent.parent / 'sql' / 'get_orderpos_are_15.sql'
        with open(sql_file, 'r', encoding='utf-8-sig') as f:  
            sql_query = f.read().strip()  
            
        if not sql_query:
            print("Error: SQL query is empty")
            return output_file if output_file.exists() else None
            
        print(f"Executing query: {sql_query}")
        result = execute_query(sql_query)
        
        if result is None or result.empty:
            print("No results found, returning empty file")
            return output_file if output_file.exists() else None
            
        print(f"Found {len(result)} rows")
            
        rename_columns = {
            'OrderNr_Lang': 'txId',
            'Menge': 'quantity',
            'OPreis': 'price',
            'ArtikelCode': 'aid',
            'erfaßt_am': 'valid_from'
        }
        
        rename_columns = {k: v for k, v in rename_columns.items() if k in result.columns}
        result = result.rename(columns=rename_columns)
        
        result['company'] = 1
        result['priceUnit'] = 'Stk'
        result['supplier_id'] = result['txId'].str[:5]
        result['factory'] = 'Düsseldorf'
        result['commodity_group_path'] = ''
        result['unit'] = 'Stk'
        result['use_proc_unit_for_purchase'] = '0'
        result['supplierAid'] = ''
        result['pos_text'] = ''
        # Convert price to string and replace decimal point with comma
        result['price'] = result['price'].astype(str).str.replace('.', ',', regex=False)
        
        if 'valid_from' in result.columns:
            result['valid_from'] = result['valid_from'].dt.strftime("%Y%m%d")
        
        if 'clerk' in result.columns:
            result['clerk'] = result['clerk'].apply(
                lambda x: x.decode('utf-16-le') if isinstance(x, bytes) else str(x)
            )
        
        selected_columns = [col for col in columns if col in result.columns]
        result = result[selected_columns]
        
        # Write the actual data
        result.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        print(f"Data written to: {output_file}")
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_order_pos_are_15: {str(e)}")
        import traceback
        traceback.print_exc()
        return output_file if 'output_file' in locals() and output_file.exists() else None

def import_order_classification():
    try:
        # Read order position data directly
        orderpos_sql_file = Path(__file__).parent.parent / 'sql' / 'get_orderpos.sql'
        with open(orderpos_sql_file, 'r', encoding='utf-8-sig') as f:
            orderpos_sql = f.read().strip()
            
        if not orderpos_sql:
            print("Error: Order position SQL query is empty")
            return None
            
        # Execute the order position query
        orderpos_data = execute_query(orderpos_sql)
        if orderpos_data is None or orderpos_data.empty:
            print("Warning: No order position data returned from query")
            return None
            
        # Export order position data to CSV
        orderpos_output = OUTPUT_DIR / "orderpos_data.csv"
        orderpos_data.to_csv(orderpos_output, index=False, encoding='utf-8-sig', sep=';')
        print(f"Exported order position data to: {orderpos_output}")
        print(f"Order position data columns: {orderpos_data.columns.tolist()}")
        print(f"Total order positions: {len(orderpos_data)}")
            
        # Process the main order classification query
        sql_file = Path(__file__).parent.parent / 'sql' / 'get_order.sql'
        with open(sql_file, 'r', encoding='utf-8-sig') as f:  
            sql_query = f.read().strip()  
            
        if not sql_query:
            print("Error: SQL query is empty")
            return None
            
        order_data = execute_query(sql_query)
        if order_data is None or order_data.empty:
            return None
            
        # First check which columns exist in the data
        available_columns = order_data.columns.tolist()
        print("\n=== Order Data ===")
        print(f"Available columns: {available_columns}")
        print(f"Total orders: {len(order_data)}")
        
        # Print sample data for inspection
        if not order_data.empty:
            print("\nSample order data:")
            print(order_data.head(2).to_string())
        
        # Define the column mappings
        column_mapping = {
            'OrderNr_Lang': 'txId',
            'POCode': 'txIdExternal',
            'AdrId': 'supplier_id',
            'Name': 'clerk',
            'erfasst_am': 'txDate',
            'OrgDatum': 'ex_txDate'
        }
        
        # Only keep mappings where the source column exists
        rename_columns = {k: v for k, v in column_mapping.items() if k in available_columns}
        print(f"Renaming columns: {rename_columns}")
        
        # Rename the columns
        order_data = order_data.rename(columns=rename_columns)
        
        # Set default values
        order_data['order_auto'] = 1
        order_data['currency'] = 'USD'
        
        # Handle date columns if they exist
        if 'ex_txDate' in order_data.columns and pd.api.types.is_datetime64_any_dtype(order_data['ex_txDate']):
            order_data['ex_txDate'] = order_data['ex_txDate'].dt.strftime("%Y%m%d")
        else:
            order_data['ex_txDate'] = datetime.now().strftime("%Y%m%d")
            
        if 'txDate' in order_data.columns and pd.api.types.is_datetime64_any_dtype(order_data['txDate']):
            order_data['txDate'] = order_data['txDate'].dt.strftime("%Y%m%d")
        else:
            order_data['txDate'] = datetime.now().strftime("%Y%m%d")
        order_data['supplier_id'] = order_data['txId'].str[:5]
        order_data['txDef'] = 'Kontrakt_EWOrder'
        order_data['company'] = 1
        order_data['classification_system'] = 'Version'
        order_data['feature[0]'] = 'CUT'
        order_data['feature_value[0]'] = '1'
        order_data['feature[1]'] = 'SpecSheet'
        order_data['feature_value[1]'] = ''
        order_data['K_Typ'] = '1'

        

        if 'clerk' in order_data.columns:
            order_data['clerk'] = order_data['clerk'].apply(
                lambda x: x.decode('utf-16-le') if isinstance(x, bytes) else str(x)
            )
        
        available_columns = ['txId', 'K_Typ', 'classification_system', 'feature[0]', 'feature_value[0]', 'feature[1]', 'feature_value[1]']
        selected_columns = [col for col in available_columns if col in order_data.columns]
        order_data = order_data[selected_columns]
        
        output_file = OUTPUT_DIR / "order_classification.csv"
        order_data.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_order: {str(e)}")
        import traceback
        traceback.print_exc()
        return None      
def import_stock_lager(diff_areas=None):
    """
    Import stock/lager data from MDB database and export to CSV.
    
    Args:
        diff_areas (list, optional): List of area codes to filter. If None, all areas are included.
    
    Returns:
        tuple: Paths to the generated CSV files
    """
    try:
        # Import diff_areas only when needed
        if diff_areas is None:
            try:
                from run_comparison_standalone import diff_areas as _diff_areas
                diff_areas = _diff_areas
            except (ImportError, AttributeError):
                pass

        # Read the SQL query from file
        with open(Path(__file__).parent.parent / 'sql' / 'get_lager.sql', 'r', encoding='utf-8') as f:
            sql_query = f.read()
        
        # Prepare parameters
        params = ['Kommissionierungslager', -1]
        
        # Add area filter if diff_areas is provided
        # Convert diff_areas to list if it's a set
        if isinstance(diff_areas, set):
            diff_areas = list(diff_areas)
            
        if diff_areas and len(diff_areas) > 0:
            print(f"Filtering {len(diff_areas)} areas")
            # For large number of areas, we'll filter in Python instead of SQL
            # This avoids the parameter limit issue
            area_condition = ""
        else:
            area_condition = ""
            
        sql_query = sql_query.format(diff_areas_filter=area_condition)
        
        # Execute the query
        df = pd.DataFrame(execute_query(sql_query, params))
        if df.empty:
            return None, None, None
            
        # Format the area column
        df['area'] = df.apply(
            lambda row: f"{row['Reihe']}-{row['Regal']}-{int(row['Palette']):04d}",
            axis=1
        )
        
        df_unfiltered = df.copy()
        
        # Apply diff_areas filter to the main DataFrame if needed
        if diff_areas and len(diff_areas) > 0:
            df['area_lower'] = df['area'].str.lower()
            valid_areas_lower = {str(area).lower() for area in diff_areas}
            df = df[df['area_lower'].isin(valid_areas_lower)]
            df = df.drop(columns=['area_lower'])
    
        if df.empty:
            return None, None, None
        # Rest of your code remains the same
        # Add required columns
        df['company'] = 0
        df['factory'] = 'Düsseldorf'
        df['refilPoint'] = 25
        df['refilPointIsPercent'] = -1
        df['refilQuantity'] = 0
        df['unit'] = 'STK'
        df['is_priority_area'] = 1
        df['isPriorityArea'] = 1  # Add this line to match the column name in main_columns
        df['storage_area_type'] = 'PICKING'  # Add missing column with default value
        df['refilQuantity'] = 0
        df['unit'] = 'Stk'
        df['is_priority_area'] = 1
        
        
        # Select and reorder columns for main output
        main_columns = [
            'location', 'factory', 'area', 'is_priority_area'
        ]
        main_output_columns = [col for col in main_columns if col in df.columns]
        
        # Save main output file
        main_output_file = OUTPUT_DIR / "STOCK - Lager.csv"
        df[main_output_columns].to_csv(
            main_output_file, index=False, encoding='utf-8-sig', sep=';'
        )
        
        # Create and save priority area file
        priority_columns = ['aid', 'area', 'company', 'factory', 'location']
        priority_df = df[['aid', 'area', 'company', 'factory', 'location']].copy()
        priority_df = priority_df[priority_columns]  # Reorder columns
        
        priority_file = OUTPUT_DIR / "STOCKARTICLE_PRIORITY_AREA - Prioritätsplätze.csv"
        priority_df.to_csv(
            priority_file, index=False, encoding='utf-8-sig', sep=';'
        )
        
        # Create and save location definition file
        loc_def_columns = [
            'aid', 'area', 'company', 'factory', 'location', 
            'quantity', 'refilPoint', 'refilPointIsPercent',
            'unit'
        ]
        loc_def_df = df[loc_def_columns].copy()
        
        loc_def_file = OUTPUT_DIR / "Stockarticle_LocDef-Stellplatzdefinitionen.csv"
        loc_def_df.to_csv(
            loc_def_file, index=False, encoding='utf-8-sig', sep=';'
        )
        
        return (
            main_output_file if main_output_file.exists() else None,
            priority_file if priority_file.exists() else None,
            loc_def_file if loc_def_file.exists() else None
        )
            
    except Exception as e:
        print(f"Error in import_stock_lager: {e}")
        import traceback
        traceback.print_exc()
        return None, None, None
    

def import_business_partner(diff_partner_ids=None):
    """
    Import business partner data from MDB database and export to CSV.
    
    Args:
        diff_partner_ids (list, optional): List of partner IDs to filter. If None, all partners are included.
        
    Returns:
        Path: Path to the generated CSV file, or None if an error occurred
    """
    try:
        # Import diff_partner_ids only when needed
        if diff_partner_ids is None:
            try:
                from run_comparison_standalone import diff_partner_ids as _diff_ids
                diff_partner_ids = _diff_ids
            except (ImportError, AttributeError):
                pass

        # Read the SQL query from file
        sql_file = Path(__file__).parent.parent / 'sql' / 'get_business_partner.sql'
        
        # Check if SQL file exists
        if not sql_file.exists():
            print(f"Warning: SQL file not found at {sql_file}")
            # Create a placeholder file if it doesn't exist
            with open(sql_file, 'w', encoding='utf-8') as f:
                f.write("SELECT * FROM tAdressen") # Placeholder query
            print(f"Created placeholder SQL file at {sql_file}")

        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_query = f.read()
            
        # Execute the query
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No business partner data found")
            return None
            
        # Filter by diff_partner_ids if provided
        if diff_partner_ids and len(diff_partner_ids) > 0:
            print(f"Filtering {len(diff_partner_ids)} business partners")
            # Assume 'AdrId' is the identifier column in tAdressen
            if 'AdrId' in df.columns:
                df['AdrId'] = df['AdrId'].astype(str)
                valid_ids = {str(pid) for pid in diff_partner_ids}
                df = df[df['AdrId'].isin(valid_ids)]
            else:
                print("Warning: 'AdrId' column not found, cannot apply filter")
            
        if df.empty:
            print("No business partner data found after filtering")
            return None

        # Process the data
        # Add default columns similar to other import functions
        df['company'] = 1
        
        # Define output file
        output_file = OUTPUT_DIR / "BUSINESS_PARTNER.csv"
        
        # Save to CSV
        df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        print(f"Exported {len(df)} business partner records to: {output_file}")
        
        return output_file if output_file.exists() else None
            
    except Exception as e:
        print(f"Error in import_business_partner: {e}")
        import traceback
        traceback.print_exc()
        return None

# This allows the script to be run directly
if __name__ == "__main__":
    import_artikel_classification()
    import_sku_classification()
    
    
    

