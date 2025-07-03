import pyodbc
import csv
import json
import base64
from cryptography.fernet import Fernet
import pandas as pd
from sqlalchemy import create_engine



cipher_suite = Fernet(b'l0cD3IJGNVc4c6dArV_u8XkXMffeumPFNd4hltjoiA8=')

# Gibt den entschlüsselten Zugang zur ERP DB zurück
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
        print("SQL-Datei: ",file_error)

try:

    #key = b'wDByOLLImijDsEaiGL1gzGFsXruC_7ol1Ht5lqo='
    #cipher_suite = Fernet(key)

    #
    # Einlesen der verschlüsselten Zugangsdaten.
    #
    credential_file = "C:\\Temp\\fet_test_credentials.enc"
    username, password = load_credential(credential_file)

   #
   # Angabe des Servers und der Datenbank
    #
    server = 'PFDusSQL,50432'
    database = 'fet_test'

    connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=SQL+Server'
    sql_query = load_query(r'\\V-Processes\\ERP_Queries\\SKU_Carton_4_QS.sql')

    engine = create_engine(connection_string)
    with engine.connect() as conn:
        df = pd.read_sql_query(sql_query, conn)
        csv_filename = r'\\pfduskommi\Promodoro.Export\Warehouse\SKU_Carton_4_QS.csv'
        df.to_csv(csv_filename, sep=';', index=False, encoding='utf-8-sig')

except Exception as e:
    print(e)
    x = input()




