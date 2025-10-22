import sys
import os
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

from src.database import execute_query, read_sql_query
from run_comparison_standalone import diff, diff1, diff_EAN

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
if sys.stderr.encoding != 'utf-8':
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
def extract_numbers(value):
    #Extract numbers from a string
    if pd.isna(value):
        return ''
    import re
    numbers = re.findall(r'\d+', str(value))
    return ''.join(numbers) if numbers else ''

def import_sku_basis():
    try:
        if not diff:
            raise ValueError("No AIDs found")
        
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
        df['factory'] = 'DÃ¼sseldorf'
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
        
        df = pd.DataFrame(execute_query(read_sql_query("get_skus.sql", diff)))
        df_color = pd.DataFrame(execute_query(read_sql_query("get_farbe_sortierung.sql")))
        if df.empty:
            print("No data returned")
            return None
            
        # Add classification columns
        df['company'] = 0
        df['classification_system'] = 'Warengruppensystem'
        df['product_group_superior'] = df['Marke'] + '||Produktlinie||ROOT'
        df['ArtikelCode'] = df['aid']
        df['Farbe'] = df['Farbe'].str.split('/').str[0].str.strip()
        
        # Get color sorting data
        df_color = pd.DataFrame(execute_query(read_sql_query('get_farbe_sortierung.sql')))
        # Merge with main dataframe to get Sort values
        df = pd.merge(df, df_color, left_on='Farbe', right_on='ERP_Farben', how='left')
        
        # Define feature mappings
        features = [
            ('Grammatur', df['Grammatur'].str.extract(r'(\d+)')[0]),
            ('Oeko_MadeInGreen', df['Oeko_MadeInGreen']),
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
            ('Colour_Farbsortierung', df['Sort'].fillna('')),
            # Handle encoding for 'Größe' column - check both possible encodings
            ('Size_Größe', df.get('Größe', df.get('GrÃ¶ÃŸe', pd.Series('')))),
            ('Size_Größe', df.get('Größe', df.get('GrÃ¶ÃŸe', pd.Series('')))),
            ('Size_Größenspiegel', df.get('Größenspiegel', df.get('GrÃ¶ÃŸenspiegel', pd.Series('')))),
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
        from run_comparison_standalone import diff1
        
        if not diff1:
            raise ValueError("No AIDs found")
        
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
        df['factory'] = 'DÃ¼sseldorf'
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



def import_artikel_classification():
    #Import article classification data for AIDs not in ERP
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
    #Import article association data (Zuordnung) for AIDs not in ERP
    try:
        from run_comparison_standalone import diff1
        
        if not diff1:
            raise ValueError("No AIDs found")
        
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
    #Import article keywords for AIDs not in ERP
    try:
        from run_comparison_standalone import diff1
        
        if not diff1:
            raise ValueError("No AIDs found")
        
        # Read and execute the query from SQL file
        sql_query = read_sql_query("get_article_keyword.sql", diff1)
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No article keyword data returned")
            return None
        
        # Process the data
        # Replace empty or NaN values with 'kein SchlÃ¼sselwort'
        df['keyword'] = df['keyword'].fillna('kein SchlÃ¼sselwort')
        df['keyword'] = df['keyword'].replace('', 'kein SchlÃ¼sselwort')
        
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

def import_artikel_text():
    try:
        from run_comparison_standalone import diff1
        
        if not diff1:
            raise ValueError("No AIDs found")
        
        # Read and execute the query from SQL file
        sql_query = read_sql_query("get_article_text_DE.sql", diff1)
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No article text data returned")
            return []
        
        # Process the data
        # Replace NaN values with empty string and strip whitespace
        df = df.fillna('').apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        
        # Define text classifications and their corresponding text content
        text_classifications = [
            ('Webshoptext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Artikeltext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Katalogtext', df['ArtText'] + ' ' + df['ArtSpec1'] + ' ' + df['ArtSpec2']),
            ('Vertriebstext', df['ArtBem'] + ' ' + df['ArtText'] + ' ' + df['VEText'] + ' ' + df['VEText2'] + ' ' + df['VEText_SP']),
            ('Rechnungstext', df['ArtBem'])
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
            df_result = df_result[['aid', 'company', 'textClassification', 'text', 'language']]
            
            # Create output filename based on classification
            output_file = OUTPUT_DIR / f"article_text_{classification.lower()}.csv"
            df_result.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
            
            if output_file.exists():
                output_files.append(output_file)
                print(f"{classification} data exported with {len(df_result)} rows to: {output_file}")
        
        return output_files if output_files else None
        
    except Exception as e:
        print(f"Error in import_artikel_text: {e}")
        import traceback
        traceback.print_exc()
        raise

import re

def import_sku_text():
    try:
        from run_comparison_standalone import diff
        
        if not diff:
            raise ValueError("No AIDs found")
        
        # Read and execute the query from SQL file
        sql_query = read_sql_query("get_sku_text_DE.sql", diff)
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No article text data returned")
            return []
        
        # Process the data
        # Replace NaN values with empty string and strip whitespace
        df = df.fillna('').apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        
        # Define text to image mapping for Pflegekennzeichnung
        def replace_with_images(text):
            if not isinstance(text, str):
                return text
                
            # Replace text with corresponding image HTML tags
            # Check if image file exists
            image_path = r"C:/Users/gia.luongdo/Python/importer_artikel_project/data/image/Screenshot 2025-10-08 123245.png"
            if not os.path.exists(image_path):
                print(f"Warning: Image file not found at {image_path}")
                
            replacements = {
                '(durchgestrichenes Dreieck)': f'<img src="{image_path}" alt="Nicht waschen" width="50" height="50">',
                'nicht bleichen': '<img src="bleichen-durchgestrichen.png" alt="Nicht bleichen" width="50" height="50">',
                'nicht bÃ¼geln': '<img src="buegeln-durchgestrichen.png" alt="Nicht bÃ¼geln" width="50" height="50">',
                'nicht chemisch reinigen': '<img src="chemisch-reinigen-durchgestrichen.png" alt="Nicht chemisch reinigen" width="50" height="50">',
                'nicht im Trockner trocknen': '<img src="waermetrocknung-durchgestrichen.png" alt="Nicht im Trockner trocknen" width="50" height="50">',
                'nicht schleudern': '<img src="schleudern-durchgestrichen.png" alt="Nicht schleudern" width="50" height="50">',
            }
            
            for key, value in replacements.items():
                text = text.replace(key, value)
                
            return text
        
        # Apply text processing to Pflegekennzeichnung
        df['Pflegekennzeichnung'] = df['Pflegekennzeichnung'].str.replace(';', '\\n')
        df['Pflegekennzeichnung'] = df['Pflegekennzeichnung'].apply(replace_with_images)
        df['Pflegekennzeichnung'] = df['Pflegekennzeichnung'].apply(lambda x: x[:2] + '	Â°C' + x[2:] if len(x) > 2 else x)
        
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
            df_result = df[['ArtikelCode']].copy()
            df_result.rename(columns={'ArtikelCode': 'aid'}, inplace=True)
            
            # Add required columns
            df_result['company'] = 0
            df_result['language'] = 'DE'
            df_result['textClassification'] = classification
            df_result['text'] = text_content.str.strip()  # Remove extra whitespace
            
            
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
            df_result = df_result[['aid', 'company', 'textClassification', 'text', 'language']]
            
            # Create output filename based on classification
            output_file = OUTPUT_DIR / f"sku_text_{classification.lower()}.csv"
            df_result.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
            
            if output_file.exists():
                output_files.append(output_file)
                print(f"{classification} data exported with {len(df_result)} rows to: {output_file}")
        
        return output_files if output_files else None
    except Exception as e:
        print(f"Error in import_sku_text: {e}")
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
        
        # Define attributes mapping with proper values
        attributes = [
            #('variant_aid', df['sku']),
            ('Size_GrÃ¶ÃŸe', df['GrÃ¶ÃŸe']),
            ('Colour_Farbe', df['Farbe']),
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
        
        print(f"Variant data exported successfully to: {output_file}")
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
        # Handle encoding for 'Größe' column - check both possible encodings
        groesse_col = 'Größe' if 'Größe' in df.columns else 'GrÃ¶ÃŸe'
        
        attributes = [
            ('Size_Größe', df[groesse_col]),
            ('Colour_Farbe', df['Farbe']),
            ('Ursprungsland', df['Ursprungsland'].str[:2])
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
        
        print(f"Variant data exported successfully to: {output_file}")
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
        
        # Pivot the data to get price tiers as columns
        df_pivot = df.pivot(
            index='aid',
            columns='Staffel',
            values='price'
        ).reset_index()
        
        # Rename columns for clarity
        df_pivot = df_pivot.rename(columns={
            1: 'price_1',
            2: 'price_2',
            3: 'price_3'
        })
        
        # Get unique article IDs and ensure we have exactly one row per article
        df_pivot = df_pivot.drop_duplicates(subset=['aid'], keep='first')
        
        # Create final dataframe with required structure
        result = []
        pricelists = ['Preisstaffel 1-3', 'Preisstaffel 2-3']
        
        # Create two rows for each article (one for each pricelist)
        for _, row in df_pivot.iterrows():
            for pricelist in pricelists:
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

        # Save to CSV with proper encoding
        output_file = OUTPUT_DIR / "PRICELIST- Artikel-Preisstafeln.csv"
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')

        print(f"Price data exported successfully to: {output_file}")

        valid_path = Path("C:/Users/gia.luongdo/Python/importer_artikel_project/data/Price_ERP.csv")
        if not valid_path.exists():
            print(f"Warning: Validity file not found at {valid_path}")
            return output_file if output_file.exists() else None
            
        validity_df = pd.read_csv(valid_path, sep=';', header=0, dtype=str)
        validity_df.columns = [col.lower() for col in validity_df.columns]
        
        required_columns = ['aid', 'valid_from', 'price', 'min_amount']
        existing_columns = [col for col in required_columns if col in validity_df.columns]
        validity_df = validity_df[existing_columns]
        
        if 'price' in validity_df.columns:
            validity_df['price'] = pd.to_numeric(validity_df['price'], errors='coerce')
        if 'min_amount' in validity_df.columns:
            validity_df['min_amount'] = pd.to_numeric(validity_df['min_amount'], errors='coerce')

        if not validity_df.empty and 'aid' in validity_df.columns and 'aid' in final_df.columns:
            # Clean and prepare aid columns
            final_df['aid'] = final_df['aid'].astype(str).str.strip()
            validity_df['aid'] = validity_df['aid'].astype(str).str.strip().str.replace('_obsolet', '', regex=False)
            
            # Handle column name conflicts
            suffix_cols = {col: f"{col}_new" for col in validity_df.columns 
                         if col in final_df.columns and col != 'aid'}
            
            if suffix_cols:
                validity_df = validity_df.rename(columns=suffix_cols)
            
            # Perform the merge with indicator to check matches
            merged_df = pd.merge(
                final_df, 
                validity_df, 
                on='aid', 
                how='left',
                indicator=True
            )
            
            # Drop the merge indicator column
            merged_df = merged_df.drop('_merge', axis=1)
            
            # Ensure valid_from_new is in datetime format and get max value per aid
            if 'valid_from_new' in merged_df.columns:
                merged_df['valid_from_new'] = pd.to_datetime(merged_df['valid_from_new'], errors='coerce')
                # Group by aid and get the max valid_from_new for each aid
                max_dates = merged_df.groupby('aid')['valid_from_new'].max().reset_index()
                # Update the original dataframe with the max dates
                merged_df = merged_df.drop('valid_from_new', axis=1).merge(
                    max_dates, 
                    on='aid', 
                    how='left'
                )
                
                # Replace valid_from with valid_from_new and format as YYYYMMDD
                if 'valid_from' in merged_df.columns and 'valid_from_new' in merged_df.columns:
                    # Save original valid_from for reference
                    merged_df['valid_from_original'] = merged_df['valid_from'].copy()
                    
                    # Convert valid_from_new to datetime and format as YYYYMMDD string, keep NaT as empty string
                    merged_df['valid_from'] = pd.to_datetime(merged_df['valid_from_new'], errors='coerce').dt.strftime('%Y%m%d')
                    
                    # Replace 'NaT' with empty string
                    merged_df['valid_from'] = merged_df['valid_from'].replace('NaT', '')
                    
                    # For any empty values, keep the original valid_from value if it exists and is not empty
                    mask = (merged_df['valid_from'] == '') & (merged_df['valid_from_original'].notna() & 
                                                           (merged_df['valid_from_original'] != ''))
                    if 'valid_from_original' in merged_df.columns:
                        # Format original dates as YYYYMMDD if they exist and are not empty
                        merged_df.loc[mask, 'valid_from'] = pd.to_datetime(
                            merged_df.loc[mask, 'valid_from_original'],
                            errors='coerce'
                        ).dt.strftime('%Y%m%d').replace('NaT', '')
                    
                    # Drop the temporary columns
                    merged_df = merged_df.drop('valid_from_new', axis=1, errors='ignore')
                    if 'valid_from_original' in merged_df.columns:
                        merged_df = merged_df.drop('valid_from_original', axis=1)
                pass
            
            # Check if we have price data
            
            if all(col in merged_df.columns for col in ['price', 'min_amount', 'pricelist']):
                
                merged_df['min_amount'] = pd.to_numeric(merged_df['min_amount'], errors='coerce')
                merged_df['price'] = pd.to_numeric(merged_df['price'], errors='coerce')
                
                original_prices = merged_df[['aid', 'pricelist', 'price[0]']].copy()
                
                # Update price[0] for Preisstaffel 1-3 with max price where min_amount=1
                mask1 = (merged_df['pricelist'] == 'Preisstaffel 1-3') & (merged_df['min_amount'] == 1.0)
                if mask1.any():
                    max_price_1 = merged_df[mask1].groupby('aid')['price'].max().reset_index()
                    max_price_1 = max_price_1.rename(columns={'price': 'new_price'})
                    
                    for _, row in max_price_1.iterrows():
                        aid = row['aid']
                        new_price = row['new_price']
                        mask = (merged_df['aid'] == aid) & (merged_df['pricelist'] == 'Preisstaffel 1-3')
                        merged_df.loc[mask, 'price[0]'] = new_price
                
                # Update price[0] for Preisstaffel 2-3 with max price where min_amount=100
                mask2 = (merged_df['pricelist'] == 'Preisstaffel 2-3') & (merged_df['min_amount'] == 100.0)
                if mask2.any():
                    max_price_100 = merged_df[mask2].groupby('aid')['price'].max().reset_index()
                    max_price_100 = max_price_100.rename(columns={'price': 'new_price'})
                    
                    for _, row in max_price_100.iterrows():
                        aid = row['aid']
                        new_price = row['new_price']
                        mask = (merged_df['aid'] == aid) & (merged_df['pricelist'] == 'Preisstaffel 2-3')
                        merged_df.loc[mask, 'price[0]'] = new_price
                
                # Update price[1] with values where min_amount=100
                mask_100 = (merged_df['min_amount'] == 100.0)
                if mask_100.any():
                    price_100 = merged_df[mask_100].groupby('aid')['price'].first().reset_index()
                    price_100 = price_100.rename(columns={'price': 'price_for_100'})
                    
                    merged_df = merged_df.merge(price_100, on='aid', how='left')
                    merged_df['price[1]'] = merged_df['price_for_100']
                    merged_df = merged_df.drop('price_for_100', axis=1)
                
                # Update price[2] with values where min_amount=1000
                mask_1000 = (merged_df['min_amount'] == 1000.0)
                if mask_1000.any():
                    price_1000 = merged_df[mask_1000].groupby('aid')['price'].first().reset_index()
                    price_1000 = price_1000.rename(columns={'price': 'price_for_1000'})
                    
                    merged_df = merged_df.merge(price_1000, on='aid', how='left')
                    merged_df['price[2]'] = merged_df['price_for_1000']
                    merged_df = merged_df.drop('price_for_1000', axis=1)
                
                # Format price columns with comma as decimal separator
                for col in ['price[0]', 'price[1]', 'price[2]']:
                    if col in merged_df.columns:
                        merged_df[col] = merged_df[col].astype(str).str.replace('\.', ',', regex=True)
                
                # Remove temporary columns
                columns_to_drop = ['price', 'min_amount']
                columns_to_drop = [col for col in columns_to_drop if col in merged_df.columns]
                if columns_to_drop:
                    merged_df = merged_df.drop(columns=columns_to_drop)
            else:
                pass
            
            # Remove duplicates based on 'aid' and 'pricelist', keeping the first occurrence
            if 'aid' in merged_df.columns and 'pricelist' in merged_df.columns:
                merged_df = merged_df.drop_duplicates(subset=['aid', 'pricelist'], keep='first')
            #add valid_to
            now = datetime.now()
            merged_df['valid_to'] = now.strftime('%Y%m%d')
            
            # Export merged dataframe to CSV
            merged_output_file = OUTPUT_DIR / "PRICELIST_pricestaffeln_validity.csv"
            merged_df.to_csv(merged_output_file, index=False, encoding='utf-8-sig', sep=';')
            print(f"Price data with validity exported successfully to: {merged_output_file}")

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
        
        # Pivot the data to get price tiers as columns
        df_pivot = df.pivot(
            index='aid',
            columns='Staffel',
            values='price'
        ).reset_index()
        
        # Rename columns for clarity
        df_pivot = df_pivot.rename(columns={
            3: 'price_3',
            4: 'price_4',
            5: 'price_5',
            6: 'price_6',
            7: 'price_7'
        })
        
        # Get unique article IDs and ensure we have exactly one row per article
        df_pivot = df_pivot.drop_duplicates(subset=['aid'], keep='first')
        
        # Create final dataframe with required structure
        result = []
        pricelists = ['Preisstufe 3', 'Preisstufe 4', 'Preisstufe 5', 'Preisstufe 6', 'Preisstufe 7']
        
        # Create two rows for each article (one for each pricelist)
        for _, row in df_pivot.iterrows():
            for pricelist in pricelists:
                row_dict = row.to_dict()
                row_dict['pricelist'] = pricelist
                result.append(row_dict)
        
        final_df = pd.DataFrame(result)
        print(final_df)
        
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
        
        # Save to CSV with proper encoding
        output_file = OUTPUT_DIR / "PRICELIST- Artikel-Preisstufe_3_7.csv"
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        print(f"Price data exported successfully to: {output_file}")
        #create validity file
        valid_path = Path("C:/Users/gia.luongdo/Python/importer_artikel_project/data/Price_ERP.csv")
        if not valid_path.exists():
            print(f"Warning: Validity file not found at {valid_path}")
            return output_file if output_file.exists() else None
            
        # Read CSV with original column names first to check
        validity_df = pd.read_csv(valid_path, sep=';', header=0, dtype=str)
        
        # Print original column names for debugging
        print("Original validity_df columns:", validity_df.columns.tolist())
        
        # Normalize column names (strip and lowercase)
        column_mapping = {col: str(col).strip().lower() for col in validity_df.columns}
        validity_df = validity_df.rename(columns=column_mapping)
        
        # Handle case where 'aid' might be 'AID' in the original file
        if 'aid' not in validity_df.columns and 'AID'.lower() in [k.lower() for k in column_mapping.keys()]:
            aid_col = [k for k in column_mapping.keys() if k.lower() == 'aid'][0]
            validity_df = validity_df.rename(columns={aid_col: 'aid'})
        
        # Define and select required columns
        required_columns = ['aid', 'valid_from', 'price', 'min_amount']
        existing_columns = [col for col in required_columns if col in validity_df.columns]
        
        # Warn about any missing columns
        missing_columns = set(required_columns) - set(existing_columns)
        if missing_columns:
            print(f"Warning: Missing columns in validity file: {', '.join(missing_columns)}")
        
        # Select only the existing required columns
        validity_df = validity_df[existing_columns].copy()
        
        # Clean up aid by removing _obsolet suffix
        validity_df['aid'] = validity_df['aid'].str.replace('_obsolet', '', regex=False)
        
        # Add suffix to avoid column name conflicts
        validity_df = validity_df.rename(columns={
            'valid_from': 'valid_from_new',
            'price': 'price_new',
            'min_amount': 'min_amount_new'
        })
        
        print("\nValidity data sample:")
        print(validity_df.head())
        
        # Debug: Print columns and sample aids before merge
        print("\n=== DEBUG VALIDITY_DF BEFORE MERGE ===")
        print("validity_df shape:", validity_df.shape)
        print("validity_df columns:", validity_df.columns.tolist())
        print("validity_df sample aids (first 5):")
        for aid in validity_df['aid'].head().tolist():
            print(f"  - {aid} (type: {type(aid)})")
        
        # Check for any missing values in 'aid' column
        print("\nMissing values in validity_df['aid']:", validity_df['aid'].isna().sum())
        
        # Check for any duplicate aids
        print("Number of duplicate aids in validity_df:", validity_df['aid'].duplicated().sum())
        
        # Check sample data from final_df
        print("\n=== FINAL_DF SAMPLE ===")
        print("final_df shape:", final_df.shape)
        print("final_df sample aids (first 5):")
        for aid in final_df['aid'].head().tolist():
            print(f"  - {aid} (type: {type(aid)})")
        
        # Check for any common aids between the two dataframes
        common_aids = set(validity_df['aid'].dropna()).intersection(set(final_df['aid']))
        print("\nNumber of common aids:", len(common_aids))
        if common_aids:
            print("Sample of common aids:", list(common_aids)[:5])
        
        print("===================================\n")
        
        # Check for any matching aids
        common_aids = set(final_df['aid']).intersection(set(validity_df['aid']))
        print(f"Found {len(common_aids)} common aids between dataframes")
        if common_aids:
            print("Sample of common aids:", list(common_aids)[:5])
        
        print("\nPreparing to merge dataframes...")
        
        # First, remove duplicates in validity_df by keeping the first occurrence
        print("Removing duplicates in validity_df...")
        validity_df_deduped = validity_df.drop_duplicates(subset=['aid'], keep='first')
        print(f"Reduced from {len(validity_df)} to {len(validity_df_deduped)} rows after deduplication")
        
        # Now perform the merge
        print("Merging dataframes...")
        merged_df = pd.merge(
            final_df,
            validity_df_deduped,
            on='aid',
            how='left'  # Keep all rows from final_df, add matching rows from validity_df
        )
        
        # Check merge results
        print("\n=== MERGE RESULTS ===")
        matched = merged_df['valid_from_new'].notna().sum()
        print(f"Rows with matches from validity_df: {matched}/{len(merged_df)} ({(matched/len(merged_df)*100):.1f}%)")
        
        # Debug: Print columns after merge
        print("\nColumns in merged_df:", merged_df.columns.tolist())
        
        # Show sample of non-merged rows (where valid_from_new is NA)
        print("\nSample of non-merged rows (no match in validity_df):")
        print(merged_df[merged_df['valid_from_new'].isna()].head())
        
        # Show sample of successfully merged rows
        if 'valid_from_new' in merged_df.columns:
            merged_rows = merged_df[merged_df['valid_from_new'].notna()]
            if not merged_rows.empty:
                print("\nSample of successfully merged rows:")
                print(merged_rows.head())
            else:
                print("\nNo rows were successfully merged. Check if AIDs match between files.")
        else:
            print("\n'valid_from_new' column not found in merged data")
        
        # Format the columns from validity_df
        if 'valid_from_new' in merged_df.columns:
            # Convert valid_from_new to datetime
            merged_df['valid_from_new_dt'] = pd.to_datetime(merged_df['valid_from_new'], errors='coerce')
            
            # Get min and max dates directly from the validity_df to ensure we have all dates
            validity_dates = validity_df[['aid', 'valid_from_new']].copy()
            validity_dates['valid_from_new'] = pd.to_datetime(validity_dates['valid_from_new'], errors='coerce')
            
            # Get min and max dates for each aid from the original validity data
            min_dates = validity_dates.groupby('aid')['valid_from_new'].min().reset_index()
            max_dates = validity_dates.groupby('aid')['valid_from_new'].max().reset_index()
            
            # Rename columns for clarity
            min_dates = min_dates.rename(columns={'valid_from_new': 'min_date'})
            max_dates = max_dates.rename(columns={'valid_from_new': 'max_date'})
            
            # Get unique prices for each AID and sort them in ascending order
            price_mapping = validity_df[['aid', 'price_new']].copy()
            price_mapping['price_new'] = pd.to_numeric(price_mapping['price_new'], errors='coerce')
            price_mapping = price_mapping.dropna(subset=['price_new'])
            
            # For each AID, get all available prices and sort them in ascending order
            aid_prices = price_mapping.groupby('aid')['price_new'].apply(lambda x: sorted(list(set(x))))  # Sort in ascending order
            
            # Assign prices to pricelists for each AID
            def assign_prices(group):
                aid = group['aid'].iloc[0]
                if aid in aid_prices:
                    prices = aid_prices[aid]
                    # Create a mapping of pricelist to price index
                    pricelist_order = [
                        'Preisstufe 7',  # Gets the first (lowest) price
                        'Preisstufe 6',
                        'Preisstufe 5',
                        'Preisstufe 4',
                        'Preisstufe 3'   # Gets the last (highest) price if enough prices exist
                    ]
                    
                    # Create price dictionary
                    price_dict = {}
                    for i, pricelist in enumerate(pricelist_order):
                        if i < len(prices):
                            price_dict[pricelist] = prices[i]
                        elif prices:  # If we run out of prices, use the last available price
                            price_dict[pricelist] = prices[-1]
                        else:
                            price_dict[pricelist] = None
                    
                    # Apply prices to each pricelist
                    for pricelist, price in price_dict.items():
                        mask = (group['pricelist'] == pricelist)
                        group.loc[mask, 'price_new'] = price
                        
                return group
            
            # Apply price assignment for each AID
            merged_df = merged_df.groupby('aid', group_keys=False).apply(assign_prices)
            
            # Update price column with the new prices
            merged_df['price'] = merged_df['price_new'].astype(str).str.replace('\.', ',', regex=True)
            
            # Merge with the main dataframe
            merged_df = pd.merge(merged_df, min_dates, on='aid', how='left')
            merged_df = pd.merge(merged_df, max_dates, on='aid', how='left')
            
            # For Preisstufe 3-6, use max valid_from
            mask_p3_to_p6 = merged_df['pricelist'].isin(['Preisstufe 3', 'Preisstufe 4', 'Preisstufe 5', 'Preisstufe 6'])
            # For Preisstufe 7, use min valid_from
            mask_p7 = merged_df['pricelist'] == 'Preisstufe 7'
            
            # Debug: Show min and max dates for sample AID
            sample_aid = '1050-black-4XL'
            if sample_aid in merged_df['aid'].values:
                sample_min = merged_df[merged_df['aid'] == sample_aid]['min_date'].iloc[0]
                sample_max = merged_df[merged_df['aid'] == sample_aid]['max_date'].iloc[0]
                print(f"\nDebug for {sample_aid}:")
                print(f"Min date: {sample_min} (type: {type(sample_min)})")
                print(f"Max date: {sample_max} (type: {type(sample_max)})")
                print("\nSample of validity data for this AID:")
                print(validity_df[validity_df['aid'] == sample_aid][['valid_from_new', 'price_new', 'min_amount_new']].head().to_string())
                
                # Debug: Show the actual values being used
                print("\nDebug - Values being used:")
                print(f"For Preisstufe 3-6: {sample_max}")
                print(f"For Preisstufe 7: {sample_min}")
            
            # Apply the appropriate valid_from based on pricelist
            merged_df.loc[mask_p3_to_p6, 'valid_from'] = merged_df.loc[mask_p3_to_p6, 'max_date'].dt.strftime('%Y%m%d')
            merged_df.loc[mask_p7, 'valid_from'] = merged_df.loc[mask_p7, 'min_date'].dt.strftime('%Y%m%d')
            
            # Clean up temporary columns
            columns_to_drop = ['valid_from_new_dt', 'min_date', 'max_date', 'valid_from_new', 'price_new']
            columns_to_drop = [col for col in columns_to_drop if col in merged_df.columns]
            merged_df = merged_df.drop(columns=columns_to_drop)
            
            # Keep original valid_from if new one is not available (for non-matching pricelists)
            if 'valid_from' in merged_df.columns and 'valid_from_new' in merged_df.columns:
                merged_df['valid_from'] = merged_df['valid_from'].fillna(merged_df['valid_from_new'])
        
        # Format min_amount_new: convert to int if whole number, otherwise float with 2 decimals
        if 'min_amount_new' in merged_df.columns:
            # First convert to numeric, handling any non-numeric values
            merged_df['min_amount_new'] = pd.to_numeric(merged_df['min_amount_new'], errors='coerce')
            
            # Format as integer if whole number, otherwise as float with 2 decimals
            def format_min_amount(x):
                if pd.isna(x):
                    return ''
                if x.is_integer():
                    return str(int(x))
                return f"{x:.2f}".replace('.', ',')
                
            merged_df['min_amount_new'] = merged_df['min_amount_new'].apply(format_min_amount)
        
        # Add valid_to column with current date
        now = datetime.now()
        merged_df['valid_to'] = now.strftime('%Y%m%d')
        
        # Reorder columns to maintain consistency
        columns = [
            'aid', 'company', 'price', 'price_new', 'currency', 'unit', 'pricelist', 
            'valid_from', 'valid_from_new', 'valid_to', 'min_amount_new',
            'limitValidity', 'amountFrom', 'discountable_idx', 'surchargeable_idx'
        ]
        # Only include columns that exist in the dataframe
        merged_df = merged_df[[col for col in columns if col in merged_df.columns]]
        
        # Export merged dataframe to CSV
        merged_output_file = OUTPUT_DIR / "PRICELIST - Preisstufe_validity.csv"
        merged_df.to_csv(merged_output_file, index=False, encoding='utf-8-sig', sep=';')
        print(f"\nMerged data sample:")
        print(merged_df.head())
        print(f"\nPrice data with validity exported successfully to: {merged_output_file}")
        print(f"Total rows in merged data: {len(merged_df)}")
        
        return output_file if output_file.exists() else None
    except Exception as e:
        print(f"Error in import_artikel_preisstufe_3_7: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
        raise
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
            print(f"Error: SQL file not found at {sql_file}")
            return None
            
        with open(sql_file, 'r', encoding='utf-8') as f:
            sql_query = f.read().strip()
            
        if not sql_query:
            print("Error: SQL query is empty")
            return None
        
        result = execute_query(sql_query)
        if result is None:
            print("Error: No data returned from database")
            return None
            
        df = pd.DataFrame(result)
        if df.empty:
            print("Warning: No data returned from the query")
            return None
            
        price_column = 'Preis' if 'Preis' in df.columns else None
        if price_column is None:
            print("Error: Could not find price column in the result")
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
        
        id_column = 'ArtikelCode'
        if id_column not in df.columns:
            print("Error: Could not find 'ArtikelCode' column")
            return None
            
        df['aid'] = df[id_column].astype(str).str.strip()
        
        # Remove duplicate AIDs by keeping the first occurrence
        df = df.drop_duplicates(subset=['aid'], keep='first')
        
        output_columns = ['aid', 'company', 'basicPrice', 'currency', 'valid_from', 'limitValidity', 'discountable', 'surchargeable']
        final_df = df[output_columns].copy()
        
        output_file = OUTPUT_DIR / basicprice_filename
        output_file.parent.mkdir(parents=True, exist_ok=True)
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        print(f"✅ Exported {len(final_df)} price records to: {output_file}")

        # Process and export validity data
        valid_path = Path("C:/Users/gia.luongdo/Python/importer_artikel_project/data/Price_ERP.csv")
        if not valid_path.exists():
            print(f"Warning: Validity file not found at {valid_path}")
            return output_file if output_file.exists() else None
            
        try:
            # Read and process validity data
            validity_df = pd.read_csv(valid_path, sep=';', header=0, dtype=str)
            
            # Normalize column names (strip whitespace and convert to lowercase)
            validity_df.columns = [col.strip().lower() for col in validity_df.columns]
            
            # Check for required columns
            if 'aid' not in validity_df.columns or 'valid_from' not in validity_df.columns:
                print("Warning: Required columns not found in validity file")
                return output_file
                
            # Clean up AID values (remove _obsolet suffix if present)
            validity_df['aid'] = validity_df['aid'].astype(str).str.replace('_obsolet', '').str.strip()
            
            # Convert price to numeric for comparison
            if 'price' in validity_df.columns:
                validity_df['price'] = pd.to_numeric(validity_df['price'].astype(str).str.replace(',', '.'), errors='coerce')
            
            # Convert min_amount to numeric for comparison
            if 'min_amount' in validity_df.columns:
                validity_df['min_amount'] = pd.to_numeric(validity_df['min_amount'].astype(str).str.replace(',', '.'), errors='coerce')
            
            # Filter rows where min_amount = 1
            if 'min_amount' in validity_df.columns:
                validity_df = validity_df[validity_df['min_amount'] == 1]
            
            # For each AID, keep only the row with the highest price
            if 'price' in validity_df.columns and not validity_df.empty:
                validity_df = validity_df.loc[validity_df.groupby('aid')['price'].idxmax()].copy()
            
            # Select and rename columns to match target format
            validity_columns = {
                'aid': 'aid',
                'valid_from': 'valid_from',
                'price': 'price',
                'min_amount': 'min_amount'
            }
            
            # Only keep columns that exist in the source data
            validity_columns = {k: v for k, v in validity_columns.items() if k in validity_df.columns}
            validity_df = validity_df[list(validity_columns.keys())].rename(columns=validity_columns)
            
            # Add _new suffix to all columns from validity_df except 'aid'
            validity_df_renamed = validity_df.rename(columns={col: f"{col}_new" for col in validity_df.columns if col != 'aid'})
            
            # Merge with final_df on 'aid' and store in merged_df
            merged_df = pd.merge(
                final_df,
                validity_df_renamed,
                on='aid',
                how='left'
            )
            
            # Update merged_df with the requested changes
            if 'valid_from_new' in merged_df.columns:
                # Format valid_from_new as YYYYMMDD and replace valid_from
                merged_df['valid_from'] = pd.to_datetime(merged_df['valid_from_new']).dt.strftime('%Y%m%d')
            
            # Remove min_amount_new column if it exists
            if 'min_amount_new' in merged_df.columns:
                merged_df = merged_df.drop(columns=['min_amount_new'])
            
            # Update basicPrice with price_new if it exists and then remove the price_new column
            if 'price_new' in merged_df.columns:
                merged_df['basicPrice'] = merged_df['price_new'].fillna('')
                merged_df = merged_df.drop(columns=['price_new'])
            
            # Remove valid_from_new column if it exists
            if 'valid_from_new' in merged_df.columns:
                merged_df = merged_df.drop(columns=['valid_from_new'])
            
            # Add valid_to column with today's date in YYYYMMDD format
            merged_df['valid_to'] = datetime.now().strftime('%Y%m%d')
            
            # Save the updated merged_df
            merged_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
            print(f"✅ Updated basic price data with validity information: {output_file}")
            
            # Export the merged data for reference
            merged_output = OUTPUT_DIR / 'PRICELIST - Basicprice_validity.csv'
            merged_df.to_csv(merged_output, index=False, encoding='utf-8-sig', sep=';')
            print(f"✅ Exported merged data to: {merged_output}")
            
        except Exception as e:
            print(f"Error processing validity data: {e}")
            import traceback
            traceback.print_exc()
        
        # Process and export Price_Basic_ERP.csv to Price_Basic.csv
        try:
            price_basic_file = r"C:\Users\gia.luongdo\Python\importer_artikel_project\data\Price_Basic_ERP.csv"
            price_basic_df = pd.read_csv(price_basic_file, encoding='utf-8-sig', sep=';').rename(columns=lambda x: x.lower() if x != x.lower() else x)
            price_basic_df['valid_to'] = datetime.now().strftime("%Y%m%d")
            price_basic_df['valid_from'] = datetime.now().strftime("%Y%m%d")
            price_basic_df['currency'] = 'EUR'
            price_basic_df['limitValidity'] = '0'
            price_basic_df['discountable'] = 'J'
            price_basic_df['surchargeable'] = 'J'

            output_path = r"C:\Users\gia.luongdo\Python\importer_artikel_project\data\Price_Basic_validity.csv"
            price_basic_df.to_csv(output_path, index=False, encoding='utf-8-sig', sep=';')
            print(f"✅ Exported Price_Basic.csv to: {output_path}")
            
        except Exception as e:
            print(f"Warning: Could not process Price_Basic_ERP.csv - {str(e)}")
        
        return str(output_file)
        



    except Exception as e:
        print(f"Error in import_artikel_basicprice: {e}")
        import traceback
        traceback.print_exc()
        return None

def import_sku_EAN():
    try:
        if not diff_EAN:
            return None
            
        ean_list = [str(ean).strip() for ean in diff_EAN if str(ean).strip().isdigit()]
        if not ean_list:
            return None

        with open(Path(__file__).parent.parent / 'sql' / 'get_EAN.sql', 'r', encoding='utf-8') as f:
            sql_template = f.read()

        batch_size = 100
        all_dfs = []
        
        for i in range(0, len(ean_list), batch_size):
            batch = ean_list[i:i + batch_size]
            placeholders = ','.join(['?'] * len(batch))
            sql_query = sql_template.replace('{aid_placeholders}', placeholders)
            
            try:
                batch_df = pd.DataFrame(execute_query(sql_query, params=tuple(batch)))
                if not batch_df.empty:
                    all_dfs.append(batch_df)
            except Exception as e:
                continue
        
        if not all_dfs:
            return None
            
        df = pd.concat(all_dfs, ignore_index=True)
        df['QtyId'] = pd.to_numeric(df['QtyId'], errors='coerce').fillna(0).astype(int)
        df['Verpackungseinheit'] = df['Verpackungseinheit'].astype(str)
        df.loc[df['QtyId'] == 1, 'Verpackungseinheit'] = 'Stk'
        
        # Then apply other mappings only for QtyId != 1
        mask = df['QtyId'] != 1
        df.loc[mask, 'Verpackungseinheit'] = df.loc[mask, 'Verpackungseinheit'].replace({
            '1': 'Stk',
            '5': '5er',
            '10': '10er'
        })
        
        # Add required columns
        df['aid'] = df['ArtikelCode']
        df['company'] = 0
        df['EAN'] = df['EAN13'].astype(str)  # Changed from 'EAN' to 'EAN13'
        df['numbertype'] = '2'
        df['valid_from'] = datetime.now().strftime("%Y%m%d")
        df['unit'] = df['Verpackungseinheit']
        df['purpose'] = '1'

        #Reorder columns
        columns = [
            'aid', 'company', 'EAN', 'numbertype', 'valid_from', 'unit', 'purpose'
        ]
        df = df[columns]
        
        # Save to CSV with proper encoding
        output_file = OUTPUT_DIR / "article_ean.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        print(f"EAN data exported successfully to: {output_file}")
        
        return output_file if output_file.exists() else None
    except Exception as e:
        print(f"Error in import_artikel_ean: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
        raise
def import_sku_gebinde():
    try:
        from run_comparison_standalone import diff
        
        if not diff:
            raise ValueError("No AIDs found")
            
        sql_query = read_sql_query("get_sku_gebinde.sql", diff)
        df = pd.DataFrame(execute_query(sql_query))
            
        if df.empty:
            print("No packaging data available")
            return None
        
        rename_columns = {
            'ArtikelCode': 'aid',
            'Karton_HÃ¶he': 'height',
            'Karton_Breite': 'width',
            'Karton_LÃ¤nge': 'length',
            'Verpackungseinheit': 'packaging_unit',
            'Kartoneinheit': 'carton_unit',
            'Produktgewicht': 'weight'
        }
        
        rename_columns = {k: v for k, v in rename_columns.items() if k in df.columns}
        df = df.rename(columns=rename_columns)
        
        result = []
        for _, row in df.iterrows():
            # Format packaging unit for Verpackungseinheit
            packaging_unit = str(row['packaging_unit']).strip()
            if packaging_unit == '1':
                display_unit = 'Stk'
            elif packaging_unit.isdigit() and int(packaging_unit) > 1:
                display_unit = f"{packaging_unit}er Pack"
            else:
                display_unit = packaging_unit
                
            result.append({
                'aid': row['aid'],
                'packaging_configuration': 'Verpackungseinheit',
                'packaging_unit': display_unit,
                'packaging_factor': packaging_unit,
                'length': float(row.get('length', 0)) * 10 if row.get('length') is not None else None,
                'width': float(row.get('width', 0)) * 10 if row.get('width') is not None else None,
                'height': float(row.get('height', 0)) * 10 if row.get('height') is not None else None,
                'weight': row['weight'],
                'company': 0
            })
            
            # Format carton unit for Kartoneinheit
            carton_unit = str(row['carton_unit']).strip()
            if carton_unit.isdigit():
                display_carton = f"K{carton_unit}"
                carton_factor = int(carton_unit)  # Keep numeric value for factor
            else:
                display_carton = carton_unit
                carton_factor = 1  # Default factor if not a number
                
            result.append({
                'aid': row['aid'],
                'packaging_configuration': 'Kartoneinheit',
                'packaging_unit': display_carton,
                'packaging_factor': carton_factor,
                'length': float(row['length']) * 10 if pd.notna(row.get('length')) else None,
                'width': float(row['width']) * 10 if pd.notna(row.get('width')) else None,
                'height': float(row['height']) * 10 if pd.notna(row.get('height')) else None,
                'weight': row['weight'],
                'is_packing_unit': 1,
                'company': 0
            })
        
        result = pd.DataFrame(result)
        
        required_columns = ['aid', 'packaging_unit', 'packaging_factor', 'length', 'width', 'height', 'is_packing_unit', 'company']
        for col in required_columns:
            if col not in result.columns:
                result[col] = None
        
        result = result[required_columns]
        
        if 'company' not in result.columns:
            result['company'] = 1
        
        output_file = OUTPUT_DIR / "artikel_gebinde.csv"
        result.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_artikel_keyword: {e}")
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
        
        result['company'] = 0
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
            'ArtikelCode': 'aid'
        }
        
        rename_columns = {k: v for k, v in rename_columns.items() if k in result.columns}
        result = result.rename(columns=rename_columns)
        
        result['company'] = 1
        result['priceUnit'] = 'USD'
        result['supplier_id'] = result['txId'].str[:5]
        result['factory'] = 'DÃ¼sseldorf'
        result['commodity_group_path'] = '20251002/Lieferdatum ab Werk/ROOT'
            
        if 'clerk' in result.columns:
            result['clerk'] = result['clerk'].apply(
                lambda x: x.decode('utf-16-le') if isinstance(x, bytes) else str(x)
            )
        
        available_columns = ['txId', 'quantity', 'price', 'aid', 'company', 'priceUnit', 'supplier_id', 'factory', 'commodity_group_path', 'txDef']
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
def import_order_classification():
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
            'erfaÃŸt_am': 'txDate',
            'OrgDatum': 'ex_txDate'
        }
        
        rename_columns = {k: v for k, v in rename_columns.items() if k in result.columns}
        result = result.rename(columns=rename_columns)
        
        result['order_auto'] = 1
        result['currency'] = 'USD'
        result['ex_txDate'] = result['ex_txDate'].dt.strftime("%Y%m%d")
        result['txDate'] = result['txDate'].dt.strftime("%Y%m%d")
        result['supplier_id'] = result['txId'].str[:5]
        result['txDef'] = 'Kontrakt_EWOrder'
        result['company'] = 1
        result['classification_system'] = 'Version'
        result['feature[0]'] = 'CUT'
        result['feature_value[0]'] = '1'
        result['feature[1]'] = 'SpecSheet'
        result['feature_value[1]'] = ''
        result['K_Typ'] = '1'

        

        if 'clerk' in result.columns:
            result['clerk'] = result['clerk'].apply(
                lambda x: x.decode('utf-16-le') if isinstance(x, bytes) else str(x)
            )
        
        available_columns = ['txId', 'K_Typ', 'classification_system', 'feature[0]', 'feature_value[0]', 'feature[1]', 'feature_value[1]']
        selected_columns = [col for col in available_columns if col in result.columns]
        result = result[selected_columns]
        
        output_file = OUTPUT_DIR / "order_classification.csv"
        result.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_order: {str(e)}")
        import traceback
        traceback.print_exc()
        return None       
# This allows the script to be run directly
if __name__ == "__main__":
    """import_sku_basis()
    import_sku_classification()
    import_sku_keyword()
    import_artikel_basis()
    import_artikel_classification()
    import_artikel_zuordnung()
    import_artikel_keyword()
    import_artikel_text()
    import_sku_text()
    import_artikel_variant()
    import_sku_variant()
    import_artikel_pricestaffeln()
    import_artikel_basicprice()
    import_artikel_preisstufe_3_7()
    import_sku_EAN()
    import_sku_gebinde()
    import_order()
    import_order_pos()
    import_order_classification()"""
    import_artikel_basicprice()

    

   
    

    

   
    
