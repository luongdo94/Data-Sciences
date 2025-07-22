# %%
import logging
import pyodbc
import pandas as pd
from datetime import datetime
import warnings

# Suppress pandas SQLAlchemy warning
warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy connectable')

mdb_file = r"C:\Users\gia.luongdo\Desktop\ERP-Importer\db_Artikel_Export2.mdb"
sku_basis_csv = r"C:\Users\gia.luongdo\Desktop\ERP-Importer\IMPORTER_SKU_Neuanlage_Basis.csv"
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
def read_and_write_aku_data():
    table_name = 't_Art_Mega_SKU'
    query = """
        SELECT 
            sku.ArtikelCode AS SKU,
            m.ArtBasis AS aid,
            m.ArtNr AS ArtNr,
            m.Ursprungsland,
            t.ArtBem AS name
        FROM 
            ((t_Art_Mega_SKU sku
            INNER JOIN t_Art_Text_DE t ON sku.ArtNr = t.ArtNr)
            INNER JOIN t_Art_MegaBase m ON sku.ArtNr = m.ArtNr)
        WHERE 
            m.Marke IN ('Corporate', 'EXCD', 'XO')
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
    #df['name'] = df['name']
    df['replacement_time'] = 1
    df['taxPi'] = 'Waren'
    df['taxSl'] = 'Waren'
    df['valid_from'] = datetime.now().strftime("%Y%m%d")

    # Take only first 2 characters from Ursprungsland and rename to 'country'
    df['country_of_origin'] = df['Ursprungsland'].str[:2]
    
    # Save the link data before reordering columns
    df_link = df[['SKU', 'aid', 'ArtNr']].copy()
    
    # Reorder columns for the main export
    df = df[['SKU', 'company', 'country_of_origin', 'automatic_batch_numbering_pattern', 
             'batch_management', 'batch_number_range', 'batch_numbering_type', 
             'date_requirement', 'discountable', 'factory', 'isPi', 'isShopArticle', 
             'isSl', 'isSt', 'isVerifiedArticle', 'isCatalogArticle', 'unitPi', 
             'unitSl', 'unitSt', 'name', 'replacement_time', 'taxPi', 'taxSl', 'valid_from']]
    
    # Write DataFrames to CSV files
    df.to_csv(sku_basis_csv, index=False, encoding='utf-8', sep=',')
    df_link.to_csv(r"C:\Users\gia.luongdo\Desktop\ERP-Importer\IMPORTER_SKU_ArtBasis.csv", 
                  index=False, encoding='windows-1252', sep=',')
    
    print(f'Data exported to {sku_basis_csv} and IMPORTER_SKU_ArtBasis.csv')
    logging.info(f'Data exported to {sku_basis_csv} and IMPORTER_SKU_ArtBasis.csv')

# IMPORTER_SKU_CLASSIFICATION_Merkmale_Basis
def read_and_write_sku_classification_data():
    """Reads classification data from the database and writes it to a CSV file."""
    # Define the table name and query
    classification_table_name = 't_Art_MegaBase'
    table_name = 't_Art_Flags'
    classification_query = f"""
        SELECT
            sku.ArtikelCode AS SKU,
            m.Ursprungsland AS Ursprungsland,
            sku.Größe AS Größe,
            sku.Größenspiegel AS Größenspiegel,
            sku.Hauptfarbe AS Farbgruppe,
            sku.FarbeNeu AS Farbe,
            sku.isColorCombination AS zweifarbig,
            sku.Karton_Länge as Verpackungslänge,
            sku.Karton_Breite as Verpackungsbreite,
            sku.Karton_Höhe as Verpackungshoehe,
            sku.Produktgewicht as Produktgewicht,
            sku.WarenNr as WarenNr,
            sku.isColorMelange as ColorMelange,
            sku.VZTA_gültig_bis as [VZTA aktiv bis],
            sku.VZTA_gültig_von as [VZTA aktiv von],
            sku.ArtSort AS sku_ArtSort,
            m.Materialart AS Fabric_Herstellung,
            m.Produktgruppe AS product_group,
            m.Marke AS Marke,
            m.Grammatur AS Grammatur,
            m.Artikel_Partner AS Artikel_Partner,
            m.Zusammensetzung AS Zusammensetzung,
            m.Gender AS Gender,
            f.flag_workwear AS workwear,
            f.flag_veredelung AS veredelung,
            f.flag_discharge AS discharge,
            f.flag_dtg AS dtg,
            f.flag_dyoj AS dyoj,
            f.flag_dyop AS dyop,
            f.flag_flock AS flock,
            f.flag_siebdruck AS siebdruck,
            f.flag_stick AS stick,
            f.flag_sublimation AS sublimation,
            f.flag_transfer AS transfer,
            f.flag_premium AS premium,
            f.flag_extras AS extras,
            f.flag_outdoor AS outdoor,
            f.flag_plussize AS oversize,
            f.isNoLabel AS label,
            f.isErw AS erw
        FROM (t_Art_MegaBase m
        INNER JOIN t_Art_Flags f ON m.ArtNr = f.ArtNr )
        INNER JOIN t_Art_Mega_SKU sku ON m.ArtNr = sku.ArtNr
        WHERE m.Marke IN ('Corporate', 'EXCD', 'XO')
    """
    classification_df = pd.read_sql(classification_query, conn)

    # Add columns with specified values
    classification_df['company'] = 0
    classification_df['classification_system'] = 'Warengruppensystem'
    classification_df['product_group_superior'] = classification_df['Marke'] + '||Produktlinie||ROOT'
    classification_df['ArtikelCode'] = classification_df['SKU']
    classification_df['feature[0]'] = 'Grammatur'
    classification_df['feature_value[0]'] = classification_df['Grammatur']
    classification_df['feature[1]'] = 'Oeko_MadeInGreen'
    classification_df['feature_value[1]'] = ''
    classification_df['feature[2]'] = 'Partnerlook'
    classification_df['feature_value[2]'] = classification_df['Artikel_Partner'].str[:4]
    classification_df['feature[3]'] = 'Sortierung'
    classification_df['feature_value[3]'] = classification_df['sku_ArtSort']
    classification_df['feature[4]'] = 'Fabric_Herstellung'
    classification_df['feature_value[4]'] = classification_df['Fabric_Herstellung']
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
    classification_df['feature[22]'] = 'Gender'
    classification_df['feature_value[22]'] = classification_df['Gender']
    classification_df['feature[23]'] = 'Brand_Label'
    classification_df['feature_value[23]'] = abs(classification_df['label'])
    classification_df['feature[24]'] = 'Colour_Farbe'
    classification_df['feature_value[24]'] = classification_df['Farbe']
    classification_df['feature[25]'] = 'Colour_Farbgruppe'
    classification_df['feature_value[25]'] = classification_df['Farbgruppe']
    classification_df['feature[26]'] = 'Size_Größe'
    classification_df['feature_value[26]'] = classification_df['Größe']
    classification_df['feature[27]'] = 'Size_Größenspiegel'
    classification_df['feature_value[27]'] = classification_df['Größenspiegel']
    classification_df['feature[28]'] = 'Colour_zweifarbig'
    classification_df['feature_value[28]'] = classification_df['zweifarbig']
    classification_df['feature[29]'] = 'Ursprungsland'
    classification_df['feature_value[29]'] = classification_df['Ursprungsland'].str[:2]
    classification_df['feature[30]'] = 'Verpackung_Länge'
    classification_df['feature_value[30]'] = classification_df['Verpackungslänge']
    classification_df['feature[31]'] = 'Verpackung_Breite'
    classification_df['feature_value[31]'] = classification_df['Verpackungsbreite']
    classification_df['feature[32]'] = 'Verpackung_Hoehe'
    classification_df['feature_value[32]'] = classification_df['Verpackungshoehe']
    classification_df['feature[33]'] = 'Fabric_Gewicht'
    classification_df['feature_value[33]'] = classification_df['Produktgewicht']
    classification_df['feature[34]'] = 'Zolltext_Stat.Warennummer'
    classification_df['feature_value[34]'] = classification_df['WarenNr']
    classification_df['feature[35]'] = 'Fabric_Melange'
    classification_df['feature_value[35]'] = classification_df['ColorMelange']
    classification_df['feature[36]'] = 'Zolltext_VZTA_aktiv_bis'
    classification_df['feature_value[36]'] = classification_df['VZTA aktiv bis']
    classification_df['feature[37]'] = 'Zolltext_VZTA_aktiv_von'
    classification_df['feature_value[37]'] = classification_df['VZTA aktiv von']
    # Reorder columns
    feature_cols = []
    for i in range(38):
        feature_cols.extend([f'feature[{i}]', f'feature_value[{i}]'])
    classification_df = classification_df[
        ['SKU', 'company', 'classification_system', 'product_group', 'product_group_superior'] + feature_cols
    ]

    # Write DataFrame to CSV (add your desired path)
    classification_csv = r"C:\Users\gia.luongdo\Desktop\ERP-Importer\IMPORTER_SKU_CLASSIFICATION_Classification_Basis.csv"
    classification_df.to_csv(
        classification_csv,
        index=False,
        encoding='windows-1252',
        sep=',',
    )
    print(f'Data exported to {classification_csv}')
    logging.info(f'Data exported to {classification_csv}')

#IMPORTER_SKU_Keyword
def read_and_write_SKU_Keyword():
    table_name = 't_Art_MegaBase'
    query = """
        SELECT 
            sku.ArtikelCode AS SKU,
            0 as company,
            t.SuchText as keyword_list,
            'de' as language,
            ',' as seperator
        FROM (t_Art_MegaBase m
        INNER JOIN t_Art_Mega_SKU sku ON m.ArtNr = sku.ArtNr)
        INNER JOIN t_Art_Text_DE t ON sku.ArtNr = t.ArtNr
        WHERE m.Marke IN ('Corporate', 'EXCD', 'XO')
    """
    df = pd.read_sql(query, conn)
    Zuordnung_csv = r"C:\Users\gia.luongdo\Desktop\ERP-Importer\IMPORTER_SKU_Keyword.csv"
    df.to_csv(Zuordnung_csv, index=False, encoding='windows-1252', sep=',')
    print(f'Data exported to {Zuordnung_csv}')
    logging.info(f'Data exported to {Zuordnung_csv}')
#Link SKU and Article Basis
#def link_sku_article_basis():
    #query = f"SELECT sku.ArtikelCode AS SKU, m.ArtBasis as aid, m.ArtNr as ArtNr FROM t_Art_Mega_SKU sku INNER JOIN t_Art_MegaBase m ON sku.ArtNR = m.ArtNr WHERE m.Marke IN ('Corporate', 'EXCD', 'XO');"
    #df = pd.read_sql(query, conn)

    #df.to_csv(r"C:\Users\gia.luongdo\Desktop\ERP-Importer\IMPORTER_SKU_ArtBasis.csv", index=False, encoding='windows-1252', sep=',')

# Connection will be closed by main.py

# %%



