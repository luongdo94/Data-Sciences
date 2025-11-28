import pyodbc
import csv
import json
import base64
from cryptography.fernet import Fernet
import pandas as pd
from datetime import datetime




cipher_suite = Fernet(b'l0cD3IJGNVc4c6dArV_u8XkXMffeumPFNd4hltjoiA8=')

# Returns the decrypted access credentials for the ERP database
def load_credential(file_path):
    with open(file_path, 'rb') as file:
        encrypted_data = file.read()
        
    decrypted_data = cipher_suite.decrypt(encrypted_data)
    credentials = json.loads(decrypted_data.decode('utf8'))
    
    return credentials['username'], credentials['password']

def load_query(file_path):
    try:
        with open(file_path, 'r') as file:
            query = file.read()
        return query
    except FileNotFoundError as file_error:
        print("SQL file error: ",file_error)

def read_sql_to_dataframe(sql_query, credential_file=None, server='PFDusSQL,50432', database='fet_test'):
    """
    Read data from SQL query and return as DataFrame
    
    Parameters:
    -----------
    sql_query : str
        SQL query string to execute
    credential_file : str, optional
        Path to credentials file (default: C:\\Temp\\fet_test_credentials.enc)
    server : str, optional
        Server name (default: 'PFDusSQL,50432')
    database : str, optional
        Database name (default: 'fet_test')
    
    Returns:
    --------
    pd.DataFrame
        DataFrame containing SQL query results
    """
    try:
        # Use default credential file if not provided
        if credential_file is None:
            credential_file = "C:\\Temp\\fet_test_credentials.enc"
        
        # Load login credentials
        username, password = load_credential(credential_file)
        
        # Create connection string for pyodbc
        conn_str = f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
        
        # Connect and read data
        conn = pyodbc.connect(conn_str)
        try:
            df = pd.read_sql(sql_query, conn)
        finally:
            conn.close()
        
        return df
    
    except Exception as e:
        print(f"Error reading data from SQL: {e}")
        return None

def get_article_numbers(credential_file=None, server='PFDusSQL,50432', database='fet_test', csv_filename='article_numbers.csv'):
    """
    Get article IDs and number strings from the database
    
    Parameters:
    -----------
    credential_file : str, optional
        Path to credentials file (default: C:\\Temp\\fet_test_credentials.enc)
    server : str, optional
        Server name (default: 'PFDusSQL,50432')
    database : str, optional
        Database name (default: 'fet_test')
    csv_filename : str, optional
        Path to export CSV file (default: 'article_numbers.csv' in current directory)
        Set to None to skip CSV export
    
    Returns:
    --------
    pd.DataFrame
        DataFrame containing article IDs (aid) and number strings (NUMSTRING)
    """
    # SQL query to join article proxy and article number tables
    sql_query = """
    SELECT distinct a.aid, n.NUMSTRING, u.NAME
    FROM fet_user.FET_ARTICLE_PROXY a 
    INNER JOIN fet_user.FET_ARTICLE_NUMBER n ON a.GUID=n.ARTICLE_PROXY_GUID
    INNER JOIN fet_user.V_UNIT u on n.UNIT_GUID=u.UNIT_GUID
    WHERE a.aid not like '%obsolet' and u.NAME not in('Unit')
    """
    
    # Execute query and return DataFrame
    df = read_sql_to_dataframe(sql_query, credential_file, server, database)
    
    if df is not None:
        # Rename NUMSTRING to EAN
        df.rename(columns={'NUMSTRING': 'EAN', 'NAME': 'unitname'}, inplace=True)
        # Map unitname to unit
        unit_mapping = {
            'Single Packed': 'SP',
            'Single Piece': 'SPC',
            'Stück': 'Stk',
            '5er Pack': '5er',
            '10er Pack': '10er'
        }
        df['unit'] = df['unitname'].map(unit_mapping).fillna(df['unitname'])

        # Add requested columns
        df['company'] = 0
        df['numbertype'] = 2
        df['valid_from'] = datetime.now().strftime('%Y%m%d')
        df['valid_to'] = ''
        df['purpose'] = 1

        print(f"Successfully retrieved {len(df)} article records")
        
        # Export to CSV if filename is provided
        if csv_filename:
            try:
                df.to_csv(csv_filename, sep=';', index=False, encoding='windows-1252')
                print(f"Data exported successfully to: {csv_filename}")
            except Exception as e:
                print(f"Error exporting to CSV: {e}")
    else:
        print("Failed to retrieve article data")
    
    return df

if __name__ == "__main__":
    try:
        # 1. Original functionality: SKU_Carton_4_QS export
        print("Processing SKU_Carton_4_QS...")
        sql_query_sku = load_query(r'\\V-Processes\ERP\ERP_Queries\SKU_Carton_4_QS.sql')
        if sql_query_sku:
            df_sku = read_sql_to_dataframe(sql_query_sku)
            if df_sku is not None:
                csv_filename_sku = r'\\pfduskommi\Promodoro.Export\Warehouse\SKU_Carton_4_QS.csv'
                df_sku.to_csv(csv_filename_sku, sep=';', index=False, encoding='windows-1252')
                print(f"Successfully exported to: {csv_filename_sku}")
        
        # 2. New functionality: Article Numbers export
        print("\nProcessing Article Numbers...")
        csv_filename_articles = r'\\pfduskommi\Promodoro.Export\QS\SKU_EAN - Artikel-EAN.csv'
        get_article_numbers(csv_filename=csv_filename_articles)

    except Exception as e:
        print(f"An error occurred: {e}")
        input("Press Enter to exit...")
