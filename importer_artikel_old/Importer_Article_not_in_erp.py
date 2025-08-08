import logging
import pyodbc
import pandas as pd
import re
from datetime import datetime

# Database configuration
mdb_file = r"C:\Users\gia.luongdo\Desktop\ERP-Importer\db_Artikel_Export2.mdb"
conn_str = (
    r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
    f"DBQ={mdb_file};"
)

# Configure logging to write to a file
logging.basicConfig(
    filename='importer_log.txt',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO)

# Connect to the Access database
conn = pyodbc.connect(conn_str)
logging.info("Connected to the database successfully for article not_in_erp operations.")

def extract_numbers(text):
    """Helper function to extract numbers from text."""
    if pd.isna(text) or text is None:
        return ''
    # Convert to string and find first consecutive digits
    text_str = str(text)
    match = re.search(r'\d+', text_str)
    return match.group() if match else ''

def read_and_write_article_data_not_in_erp():
    """Reads article data not in ERP from the database and writes it to a CSV file."""
    # Read the CSV file with aid values that need to be processed
    csv_file_path = r"C:\Users\gia.luongdo\Desktop\aidew_notin_aiderp.csv"
    
    try:
        import os
        if not os.path.exists(csv_file_path):
            logging.error(f"CSV file not found at {csv_file_path}. Please run check_2_columns.py first to generate the filter file")
            print("Error: CSV file not found!")
            return

        # Read the CSV file to get the list of aid values
        filter_df = pd.read_csv(csv_file_path, encoding='utf-8-sig')
        # Use the actual column name from the CSV
        aid_column = 'aid_ew_not_in_aid_erp'
        if aid_column not in filter_df.columns:
            logging.error(f"Column '{aid_column}' not found in CSV file. Available columns: {filter_df.columns.tolist()}")
            return
        aid_values = filter_df[aid_column].astype(str).tolist()
        aid_list_str = "','".join([str(aid) for aid in aid_values])
        
        # Define the query to get article data filtered by aid values from CSV
        query = f"""
            SELECT 
                m.ArtBasis as aid, 
                m.Ursprungsland, 
                t.ArtBem as name 
            FROM 
                t_Art_MegaBase m 
                INNER JOIN t_Art_Text_DE t ON m.ArtNr = t.ArtNr 
            WHERE 
                m.Marke IN ('Corporate', 'EXCD', 'XO')
                AND m.ArtBasis IN ('{aid_list_str}')
        """
        df = pd.read_sql(query, conn)

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

        # Write DataFrame to CSV with correct separator and columns
        output_file = r"C:\Users\gia.luongdo\Desktop\ERP-Importer\IMPORTER_ARTICLE_Neuanlage_Basis_notinERP.csv"
        df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';', errors='ignore')
        print(f'Data exported to {output_file}')
        logging.info(f'Data exported to {output_file}')
        logging.info(f'Processed {len(df)} records filtered by CSV file')
        
    except Exception as e:
        logging.error(f"Error in read_and_write_article_data_not_in_erp: {e}", exc_info=True)
        print(f"An error occurred: {e}")

def read_and_write_classification_data_not_in_erp():
    """Reads and exports classification data for articles not in ERP."""
    # Read the CSV file with aid values that need to be processed
    csv_file_path = r"C:\\Users\\gia.luongdo\\Desktop\\aidew_notin_aiderp.csv"
    
    try:
        import os
        if not os.path.exists(csv_file_path):
            logging.error(f"CSV file not found at {csv_file_path}")
            print("Error: CSV file not found!")
            return

        # Read the CSV file to get the list of aid values
        filter_df = pd.read_csv(csv_file_path, encoding='utf-8-sig')
        # Use the actual column name from the CSV
        aid_column = 'aid_ew_not_in_aid_erp'
        if aid_column not in filter_df.columns:
            logging.error(f"Column '{aid_column}' not found in CSV file. Available columns: {filter_df.columns.tolist()}")
            return
        aid_values = filter_df[aid_column].astype(str).tolist()
        aid_list_str = "','".join([str(aid) for aid in aid_values])
        
        # Define the query with filter
        query = f"""
            SELECT m.ArtBasis AS aid, m.Produktgruppe as product_group, m.Marke as Marke, 
                   m.Grammatur as Grammatur, m.Artikel_Partner as Artikel_Partner, 
                   m.ArtSort as ArtSort, m.Materialart as Materialart, 
                   m.Zusammensetzung as Zusammensetzung, m.Gender as Gender, 
                   f.flag_workwear as workwear, f.flag_veredelung as veredelung, 
                   f.flag_discharge as discharge, f.flag_dtg as dtg, 
                   f.flag_dyoj as dyoj, f.flag_dyop as dyop, 
                   f.flag_flock as flock, f.flag_siebdruck as siebdruck, 
                   f.flag_stick as stick, f.flag_sublimation as sublimation, 
                   f.flag_transfer as transfer, f.flag_premium as premium, 
                   f.flag_extras as extras, f.flag_outdoor as outdoor, 
                   f.flag_plussize as oversize, f.isNoLabel as label, 
                   f.isErw as erw 
            FROM [t_Art_MegaBase] m 
            INNER JOIN [t_Art_Flags] f ON m.ArtNr = f.ArtNr 
            WHERE m.Marke IN ('Corporate', 'EXCD', 'XO')
            AND m.ArtBasis IN ('{aid_list_str}')
        """
        
        df = pd.read_sql(query, conn)
        
        if df.empty:
            print("No data found for the specified filters.")
            logging.warning("No data found for the specified filters.")
            return
            
        # Process the data
        df['Grammatur'] = df['Grammatur'].apply(extract_numbers)
        df['Gender'] = df['Gender'].replace('Kinder', '')
        
        # Add all required columns
        features = [
            ('Grammatur', 'Grammatur'),
            ('Oeko_MadeInGreen', ''),
            ('Partnerlook', lambda x: x['Artikel_Partner'].str[:4] if 'Artikel_Partner' in x.columns else ''),
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
        
        # Create result DataFrame
        result_rows = []
        for _, row in df.iterrows():
            base_data = {
                'aid': row.get('aid', ''),
                'company': 0,
                'classification_system': 'Warengruppensystem',
                'product_group': row.get('product_group', ''),
                'product_group_superior': row.get('Marke', '') + '||Produktlinie||ROOT'
            }
            
            # Add features
            for i, (feature_name, feature_value) in enumerate(features):
                base_data[f'feature[{i}]'] = feature_name
                
                if callable(feature_value):
                    value = feature_value(row)
                elif feature_value == '':
                    value = ''
                elif feature_value in row:
                    value = row[feature_value]
                else:
                    value = ''
                    
                base_data[f'feature_value[{i}]'] = value
            
            result_rows.append(base_data)
        
        result_df = pd.DataFrame(result_rows)
        
        # Generate output filename with _notinERP suffix
        output_file = r"C:\\Users\\gia.luongdo\\Desktop\\ERP-Importer\\IMPORTER_ARTICLE_CLASSIFICATION_Merkmale_Basis_notinERP.csv"
        
        # Write to CSV with Windows-1252 encoding
        result_df.to_csv(output_file, index=False, encoding='windows-1252', sep=';')
        
        print(f'Data exported to {output_file}')
        logging.info(f'Data exported to {output_file}')
        logging.info(f'Processed {len(result_df)} classification records')
        
    except Exception as e:
        logging.error(f"Error in read_and_write_classification_data_not_in_erp: {e}", exc_info=True)
        print(f"An error occurred: {e}")

def read_and_write_Zuordnung_Basis_not_in_erp():
    """Reads and exports Zuordnung (assignment) data for articles not in ERP."""
    # Read the CSV file with aid values that need to be processed
    csv_file_path = r"C:\\Users\\gia.luongdo\\Desktop\\aidew_notin_aiderp.csv"
    
    try:
        import os
        if not os.path.exists(csv_file_path):
            logging.error(f"CSV file not found at {csv_file_path}")
            print("Error: CSV file not found!")
            return

        # Read the CSV file to get the list of aid values
        filter_df = pd.read_csv(csv_file_path, encoding='utf-8-sig')
        # Use the actual column name from the CSV
        aid_column = 'aid_ew_not_in_aid_erp'
        if aid_column not in filter_df.columns:
            logging.error(f"Column '{aid_column}' not found in CSV file. Available columns: {filter_df.columns.tolist()}")
            return
        aid_values = filter_df[aid_column].astype(str).tolist()
        aid_list_str = "','".join([str(aid) for aid in aid_values])
        
        # Define the query with filter
        query = f"""
            SELECT ArtBasis AS aid, Artikel_Partner as aid_assigned, 
                   Artikel_Alternativen as aid_alternativen 
            FROM [t_Art_MegaBase]
            WHERE Marke IN ('Corporate', 'EXCD', 'XO')
            AND ArtBasis IN ('{aid_list_str}')
        """
        
        df = pd.read_sql(query, conn)
        
        if df.empty:
            print("No data found for the specified filters.")
            logging.warning("No data found for the specified filters.")
            return
        
        # Process the data
        df['aid_assigned'] = df['aid_assigned'] + df['aid_alternativen']
        df_short = df[['aid', 'aid_assigned']].copy()
        df_short['aid_assigned'] = df_short['aid_assigned'].str.split(';')
        df_short['aid_assigned'] = df_short['aid_assigned'].str[:-1]
        df_final = df_short.explode('aid_assigned')
        
        # Add columns with specified values
        df_final['company'] = 0
        df_final['remove_assocs'] = 0
        df_final['type'] = 3
        
        # Generate output filename with _notinERP suffix
        output_file = r"C:\\Users\\gia.luongdo\\Desktop\\ERP-Importer\\IMPORTER_ARTICLE_ASSIGNMENT_Zuordnung_Basis_notinERP.csv"
        
        # Write to CSV with Windows-1252 encoding
        df_final.to_csv(output_file, index=False, encoding='windows-1252', sep=';')
        
        print(f'Data exported to {output_file}')
        logging.info(f'Data exported to {output_file}')
        logging.info(f'Processed {len(df_final)} assignment records')
        
    except Exception as e:
        logging.error(f"Error in read_and_write_Zuordnung_Basis_not_in_erp: {e}", exc_info=True)
        print(f"An error occurred: {e}")

def read_and_write_Schlüsselworte_Basis_not_in_erp():
    """Reads and exports keyword data for articles not in ERP."""
    # Read the CSV file with aid values that need to be processed
    csv_file_path = r"C:\\Users\\gia.luongdo\\Desktop\\aidew_notin_aiderp.csv"
    
    try:
        import os
        if not os.path.exists(csv_file_path):
            logging.error(f"CSV file not found at {csv_file_path}")
            print("Error: CSV file not found!")
            return

        # Read the CSV file to get the list of aid values
        filter_df = pd.read_csv(csv_file_path, encoding='utf-8-sig')
        # Use the actual column name from the CSV
        aid_column = 'aid_ew_not_in_aid_erp'
        if aid_column not in filter_df.columns:
            logging.error(f"Column '{aid_column}' not found in CSV file. Available columns: {filter_df.columns.tolist()}")
            return
        aid_values = filter_df[aid_column].astype(str).tolist()
        aid_list_str = "','".join([str(aid) for aid in aid_values])
        
        # Define the query with filter
        query = f"""
            SELECT m.ArtBasis AS aid, t.SuchText as keyword 
            FROM [t_Art_MegaBase] m 
            INNER JOIN t_Art_Text_DE t ON m.ArtNr = t.ArtNr 
            WHERE m.Marke IN ('Corporate', 'EXCD', 'XO')
            AND m.ArtBasis IN ('{aid_list_str}')
        """
        
        df = pd.read_sql(query, conn)
        
        if df.empty:
            print("No data found for the specified filters.")
            logging.warning("No data found for the specified filters.")
            return
        
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
        
        # Generate output filename with _notinERP suffix
        output_file = r"C:\\Users\\gia.luongdo\\Desktop\\ERP-Importer\\IMPORTER_ARTICLE_KEYWORD_Schlüsselworte_Basis_notinERP.csv"
        
        # Write to CSV with utf-8-sig encoding
        df.to_csv(output_file, index=False, encoding='utf-8-sig', sep=';', errors='ignore')
        
        print(f'Data exported to {output_file}')
        logging.info(f'Data exported to {output_file}')
        logging.info(f'Processed {len(df)} keyword records')
        
    except Exception as e:
        logging.error(f"Error in read_and_write_Schlüsselworte_Basis_not_in_erp: {e}", exc_info=True)
        print(f"An error occurred: {e}")

# Add a main block to run all functions if the script is executed directly
if __name__ == "__main__":
    try:
        # Run all export functions
        read_and_write_article_data_not_in_erp()
        read_and_write_classification_data_not_in_erp()
        read_and_write_Zuordnung_Basis_not_in_erp()
        read_and_write_Schlüsselworte_Basis_not_in_erp()
    except Exception as e:
        logging.error(f"Error in main execution: {e}", exc_info=True)
        print(f"An error occurred: {e}")
    finally:
        # Close the database connection
        if 'conn' in locals() and conn is not None:
            conn.close()
            logging.info("Database connection closed.")
