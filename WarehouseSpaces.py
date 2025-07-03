import pyodbc
import csv
import json
import base64
from cryptography.fernet import Fernet



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

    #
    # Aufbau der Verbindung
    #
    conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}')

    cursor = conn.cursor()

    sql_query = load_query(f'\\\\V-Processes\\ERP_Queries\\SKU_Carton_4_QS.sql')

    cursor.execute(sql_query)
    #print(sql_query)
    rows = cursor.fetchall()
    #print(rows)

    description = cursor.description
    #print(description)
    columns = [column[0] for column in cursor.description]

    csv_filename = rf'\\pfduskommi\Promodoro.Export\Warehouse\SKU_Carton_4_QS.csv'
    with open(csv_filename, mode='w', newline='', encoding= "utf-8-sig") as file:
        writer = csv.writer(file,delimiter=";")
        writer.writerow(columns)
        for row in rows:
            writer.writerow(row)

    conn.close()
except Exception as e:
    print(e)
    x = input()
    
    


