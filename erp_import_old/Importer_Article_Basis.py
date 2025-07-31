import logging
import pyodbc
import pandas as pd
from datetime import datetime
# Removed invalid import statement 

mdb_file = r"C:\Users\gia.luongdo\Desktop\ERP-Importer\db_Artikel_Export2.mdb"
artikel_basis_csv = r"C:\Users\gia.luongdo\Desktop\ERP-Importer\IMPORTER_ARTICLE_Neuanlage_Basis.csv"
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
logging.info("Connected to the database successfully.")
#IMPORTER_ARTICLE_Neuanlage_Basis:
#Reads data from the database
def read_and_write_article_data():
    table_name = 't_Art_MegaBase'
    query = f"SELECT m.ArtBasis as aid, m.Ursprungsland, t.ArtBem as name FROM {table_name} m inner join t_Art_Text_DE t on m.ArtNr = t.ArtNr WHERE Marke IN ('Corporate', 'EXCD', 'XO')"
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
    df = df[['aid', 'company', 'country_of_origin', 'automatic_batch_numbering_pattern', 'batch_management', 'batch_number_range', 'batch_numbering_type', 'date_requirement', 'discountable', 'factory', 'isPi', 'isShopArticle', 'isSl', 'isSt', 'isVerifiedArticle', 'isCatalogArticle', 'unitPi', 'unitSl', 'unitSt', 'name', 'replacement_time', 'taxPi', 'taxSl', 'valid_from']]

    # Write DataFrame to CSV with correct separator and columns
    df.to_csv(artikel_basis_csv, index=False, encoding='utf-8-sig', sep=';', errors='ignore')
    print(f'Data exported to {artikel_basis_csv}')
    logging.info(f'Data exported to {artikel_basis_csv}')

# IMPORTER_ARTICLE_CLASSIFICATION_Merkmale_Basis
def read_and_write_classification_data():
    # Define the table name and query
    classification_table_name = 't_Art_MegaBase'
    table_name = 't_Art_Flags'
    classification_query = f"SELECT m.ArtBasis AS aid, m.Produktgruppe as product_group, m.Marke as Marke, m.Grammatur as Grammatur, m.Artikel_Partner as Artikel_Partner , m.ArtSort as ArtSort, m.Materialart as Materialart, m.Zusammensetzung as Zusammensetzung, m.Gender as Gender, f.flag_workwear as workwear, f.flag_veredelung as veredelung, f.flag_discharge as discharge, f.flag_dtg as dtg, f.flag_dyoj as dyoj, f.flag_dyop as dyop, f.flag_flock as flock, f.flag_siebdruck as siebdruck, f.flag_stick as stick, f.flag_sublimation as sublimation, f.flag_transfer as transfer, f.flag_premium as premium, f.flag_extras as extras, f.flag_outdoor as outdoor, f.flag_plussize as oversize, f.isNoLabel as label, f.isErw as erw FROM [{classification_table_name}] m inner join [{table_name}] f on m.ArtNr = f.ArtNr WHERE m.Marke IN ('Corporate', 'EXCD', 'XO')"
    classification_df = pd.read_sql(classification_query, conn)
    
    # Clean Grammatur column - extract only the first consecutive numbers
    def extract_numbers(text):
        if pd.isna(text) or text is None:
            return ''
        # Convert to string and find first consecutive digits
        import re
        text_str = str(text)
        match = re.search(r'\d+', text_str)
        return match.group() if match else ''
    
    # Apply the function to clean Grammatur
    classification_df['Grammatur'] = classification_df['Grammatur'].apply(extract_numbers)
    
    # Transform Gender column - change 'Kinder' to empty string
    classification_df['Gender'] = classification_df['Gender'].replace('Kinder', '')
    
    # Add columns with specified values
    classification_df['company'] = 0
    classification_df['classification_system'] = 'Warengruppensystem'
    classification_df['product_group_superior'] = classification_df['Marke']+'||Produktlinie||ROOT'
    classification_df['feature[0]'] = 'Grammatur'
    classification_df['feature_value[0]'] = classification_df['Grammatur']
    classification_df['feature[1]'] = 'Oeko_MadeInGreen'
    classification_df['feature_value[1]'] = ''
    classification_df['feature[2]'] = 'Partnerlook'
    classification_df['feature_value[2]'] = classification_df['Artikel_Partner'].str[:4]
    classification_df['feature[3]'] = 'Sortierung'
    classification_df['feature_value[3]'] = classification_df['ArtSort']
    classification_df['feature[4]'] = 'Fabric_Herstellung'
    classification_df['feature_value[4]'] = classification_df['Materialart']
    classification_df['feature[5]'] = 'Material'
    classification_df['feature_value[5]'] = classification_df['Zusammensetzung']
    classification_df['feature[6]'] = 'Workwear'
    classification_df['feature_value[6]'] = abs(classification_df['workwear'])
    classification_df['feature[7]'] = 'Produktlinie_Veredelung'
    classification_df['feature_value[7]'] = abs(classification_df['veredelung'])
    classification_df['feature[8]'] = 'Produktlinie_Veredelungsart_Discharge'
    classification_df['feature_value[8]'] = abs(classification_df['discharge'])
    classification_df['feature[9]'] = 'Produktlinie_Veredelungsart_DTG'
    classification_df['feature_value[9]'] = abs(classification_df['dtg'])
    classification_df['feature[10]'] = 'Produktlinie_Veredelungsart_DYOJ'
    classification_df['feature_value[10]'] = abs(classification_df['dyoj'])
    classification_df['feature[11]'] = 'Produktlinie_Veredelungsart_DYOP'
    classification_df['feature_value[11]'] = abs(classification_df['dyop'])
    classification_df['feature[12]'] = 'Produktlinie_Veredelungsart_Flock'
    classification_df['feature_value[12]'] = abs(classification_df['flock'])
    classification_df['feature[13]'] = 'Produktlinie_Veredelungsart_Siebdruck'
    classification_df['feature_value[13]'] = abs(classification_df['siebdruck'])
    classification_df['feature[14]'] = 'Produktlinie_Veredelungsart_Stick'
    classification_df['feature_value[14]'] = abs(classification_df['stick'])
    classification_df['feature[15]'] = 'Produktlinie_Veredelungsart_Sublimationsdruck'
    classification_df['feature_value[15]'] = abs(classification_df['sublimation'])
    classification_df['feature[16]'] = 'Produktlinie_Veredelungsart_Transferdruck'
    classification_df['feature_value[16]'] = abs(classification_df['transfer'])
    classification_df['feature[17]'] = 'Brand_Premium_Item'
    classification_df['feature_value[17]'] = abs(classification_df['premium'])
    classification_df['feature[18]'] = 'Extras'
    classification_df['feature_value[18]'] = abs(classification_df['extras'])
    classification_df['feature[19]'] = 'Kids'
    classification_df['feature_value[19]'] = 1 - abs(classification_df['erw'])
    classification_df['feature[20]'] = 'Outdoor'
    classification_df['feature_value[20]'] = abs(classification_df['outdoor'])
    classification_df['feature[21]'] = 'Size_Oversize'
    classification_df['feature_value[21]'] = abs(classification_df['oversize'])
    classification_df['feature[22]'] = 'Geschlecht'
    classification_df['feature_value[22]'] = classification_df['Gender']
    classification_df['feature[23]'] = 'Brand_Label'
    classification_df['feature_value[23]'] = abs(classification_df['label'])

    
    # Reorder columns as requested
    classification_df = classification_df[['aid', 'company', 'classification_system', 'product_group', 'product_group_superior',
        'feature[0]', 'feature_value[0]', 'feature[1]', 'feature_value[1]', 'feature[2]', 'feature_value[2]', 'feature[3]', 'feature_value[3]',
        'feature[4]', 'feature_value[4]', 'feature[5]', 'feature_value[5]', 'feature[6]', 'feature_value[6]', 'feature[7]', 'feature_value[7]',
        'feature[8]', 'feature_value[8]', 'feature[9]', 'feature_value[9]', 'feature[10]', 'feature_value[10]', 'feature[11]', 'feature_value[11]',
        'feature[12]', 'feature_value[12]', 'feature[13]', 'feature_value[13]', 'feature[14]', 'feature_value[14]', 'feature[15]', 'feature_value[15]',
        'feature[16]', 'feature_value[16]', 'feature[17]', 'feature_value[17]', 'feature[18]', 'feature_value[18]', 'feature[19]', 'feature_value[19]',
        'feature[20]', 'feature_value[20]', 'feature[21]', 'feature_value[21]', 'feature[22]', 'feature_value[22]', 'feature[23]', 'feature_value[23]']]
    
    
    # Write DataFrame to CSV (add your desired path)
    classification_csv = r"C:\Users\gia.luongdo\Desktop\ERP-Importer\IMPORTER_ARTICLE_CLASSIFICATION_Merkmale_Basis.csv"
    classification_df.to_csv(classification_csv, index=False, encoding='windows-1252', sep=';')
    print(f'Data exported to {classification_csv}')
    logging.info(f'Data exported to {classification_csv}')

#IMPORTER_ARTICLE_ASSIGNMENT_Zuordnung_Basis
def read_and_write_Zuordnung_Basis():
    table_name = 't_Art_MegaBase'
    query = f"SELECT ArtBasis AS aid, Artikel_Partner as aid_assigned, Artikel_Alternativen as aid_alternativen FROM [{table_name}] WHERE Marke IN ('Corporate', 'EXCD', 'XO')"
    df = pd.read_sql(query, conn)
    df['aid_assigned'] = df['aid_assigned']+ df['aid_alternativen']
    df_short = df[['aid', 'aid_assigned']].copy()
    df_short['aid_assigned'] = df_short['aid_assigned'].str.split(';')
    df_short['aid_assigned'] = df_short['aid_assigned'].str[:-1]
    df_final = df_short.explode('aid_assigned')
    # Add columns with specified values
    df_final['company'] = 0
    df_final['remove_assocs'] = 0
    df_final['type'] = 3
    Zuordnung_csv = r"C:\Users\gia.luongdo\Desktop\ERP-Importer\IMPORTER_ARTICLE_ASSIGNMENT_Zuordnung_Basis.csv"
    df_final.to_csv(Zuordnung_csv, index=False, encoding='windows-1252', sep=';')
    print(f'Data exported to {Zuordnung_csv}')
    logging.info(f'Data exported to {Zuordnung_csv}')

#IMPORTER_ARTICLE_KEYWORD_Schlüsselworte_Basis
def read_and_write_Schlüsselworte_Basis():
    table_name = 't_Art_MegaBase'
    query = f"SELECT ArtBasis AS aid, Artikel_Partner as aid_assigned, t.SuchText as keyword FROM [{table_name}] m inner join t_Art_Text_DE t on m.ArtNr = t.ArtNr WHERE Marke IN ('Corporate', 'EXCD', 'XO')"
    df = pd.read_sql(query, conn)
    # Thay thế các giá trị trống trong cột keyword bằng 'kein Schlüsselwort'
    df['keyword'] = df['keyword'].fillna('kein Schlüsselwort')
    df['keyword'] = df['keyword'].replace('', 'kein Schlüsselwort')
    # Add columns with specified values
    df['company'] = 0
    df['language'] = 'DE'
    df['separator'] = ','
    df = df[['aid', 'company', 'keyword', 'language', 'separator']]
    Schlüsselworte_csv = r"C:\Users\gia.luongdo\Desktop\ERP-Importer\IMPORTER_ARTICLE_KEYWORD_Schlüsselworte_Basis.csv"
    df.to_csv(Schlüsselworte_csv, index=False, encoding='utf-8-sig', sep=';', errors='ignore')
    print(f'Data exported to {Schlüsselworte_csv}')
    logging.info(f'Data exported to {Schlüsselworte_csv}')



# Connection will be closed by main.py