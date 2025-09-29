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
        
        df = pd.DataFrame(execute_query(read_sql_query("get_skus.sql", diff)))
        if df.empty:
            print("No data returned")
            return None
            
        # Add classification columns
        df['company'] = 0
        df['classification_system'] = 'Warengruppensystem'
        df['product_group_superior'] = df['Marke'] + '||Produktlinie||ROOT'
        df['ArtikelCode'] = df['aid']
        df['Farbe'] = df['Farbe'].str.split('/').str[0].str.strip()
        
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
            ('Size_Größe', df['Größe']),
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
        attributes = [
            ('Size_Größe', df['Größe']),
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
        output_file = OUTPUT_DIR / "article_price.csv"
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        print(f"Price data exported successfully to: {output_file}")
        
        return output_file if output_file.exists() else None
        
    except Exception as e:
        print(f"Error in import_artikel_price: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
        raise
    
def import_artikel_basicprice():
    try:
        # Read and execute the query from SQL file
        sql_query = read_sql_query("get_article_price.sql", None)
        df = pd.DataFrame(execute_query(sql_query))
        
        if df.empty:
            print("No price data returned from database")
            return None
        
        # Add required columns
        filtered_df = df.loc[df['Staffel'] == 1].copy()
        filtered_df['aid'] = filtered_df['ArtikelCode']
        filtered_df['company'] = '1'
        filtered_df['currency'] = 'EUR'
        filtered_df['unit'] = 'Stk'
        filtered_df['valid_from'] = datetime.now().strftime("%Y%m%d")
        filtered_df['limitValidity'] = '0'
        filtered_df['discountable'] = 'J'
        filtered_df['surchargeable'] = 'J'
        filtered_df['basicPrice'] = filtered_df['Preis']
        #Reorder columns
        columns = [
            'aid', 'company', 'basicPrice', 'currency', 'unit', 'limitValidity', 
            'discountable', 'surchargeable', 'valid_from'
        ]
        filtered_df = filtered_df[columns]
    # Save to CSV with proper encoding
        output_file = OUTPUT_DIR / "article_basicprice.csv"
        filtered_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        print(f"Price data exported successfully to: {output_file}")
        
        return output_file if output_file.exists() else None
    except Exception as e:
        print(f"Error in import_artikel_basicprice: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
        raise

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
        
        # Reorder columns
        columns = [
            'aid', 'company', 'price', 'currency', 'unit', 'pricelist', 
            'valid_from', 'limitValidity', 'amountFrom', 'discountable_idx', 'surchargeable_idx'
        ]
        final_df = final_df[columns]
        
        # Save to CSV with proper encoding
        output_file = OUTPUT_DIR / "article_preisstufe_3_7.csv"
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';')
        
        print(f"Price data exported successfully to: {output_file}")
        
        return output_file if output_file.exists() else None
    except Exception as e:
        print(f"Error in import_artikel_preisstufe_3_7: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
        raise

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
        df.loc[df['QtyId'] == 1, 'Verpackungseinheit'] = 'Stück'
        
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
        df['Verwendungszweck'] = '1'

        #Reorder columns
        columns = [
            'aid', 'company', 'EAN', 'numbertype', 'valid_from', 'unit', 'Verwendungszweck'
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
# This allows the script to be run directly
if __name__ == "__main__":
    import_sku_basis()
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
    import_artikel_Preisliste_3_7()
    import_sku_EAN()