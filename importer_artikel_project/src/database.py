import os
import pyodbc
import pandas as pd
from .config import CONN_STR, SQL_DIR

def execute_sql_file(filename, params=None):
    try:
        with open(SQL_DIR / filename, 'r', encoding='utf-8') as f:
            query = f.read()
        
        with pyodbc.connect(CONN_STR) as conn:
            return pd.read_sql(query, conn, params=params)
            
    except Exception as e:
        raise Exception(f"Error executing SQL file {filename}: {e}")

def execute_query(query, params=None):
    """Execute a SQL query and return results as a DataFrame"""
    try:
        with pyodbc.connect(CONN_STR) as conn:
            return pd.read_sql(query, conn, params=params)
    except Exception as e:
        raise Exception(f"Error executing query: {e}")

def execute_query_with_aids(query_template, aids):
    """
    Execute a query with a potentially large list of AIDs using a temporary table.
    The query_template should contain {temp_table_name} as a placeholder for the temporary table name.
    """
    if not aids:
        raise ValueError("No AIDs provided")
    
    temp_table_name = "TempAIDs"  # MS Access doesn't support # for temp tables
    
    try:
        with pyodbc.connect(CONN_STR) as conn:
            cursor = conn.cursor()
            
            # Drop the table if it exists
            try:
                cursor.execute(f"DROP TABLE {temp_table_name}")
                conn.commit()
            except:
                pass
            
            # Create a regular table (MS Access doesn't support temp tables with #)
            cursor.execute(f"CREATE TABLE {temp_table_name} (AID TEXT(50) PRIMARY KEY)")
            conn.commit()
            
            # Insert AIDs in batches to avoid parameter limits
            batch_size = 100
            for i in range(0, len(aids), batch_size):
                batch = aids[i:i + batch_size]
                # Build a single INSERT statement with multiple VALUES
                values = ", ".join(["(\"{}\")".format(str(aid).replace('"', '""')) for aid in batch])
                insert_sql = f"INSERT INTO {temp_table_name} (AID) VALUES {values}"
                cursor.execute(insert_sql)
                conn.commit()
            
            # Execute the main query with the temporary table
            query = query_template.format(temp_table_name=temp_table_name)
            result = pd.read_sql(query, conn)
            
            # Clean up - drop the table when done
            try:
                cursor.execute(f"DROP TABLE {temp_table_name}")
                conn.commit()
            except:
                pass
            
            return result
            
    except Exception as e:
        # Ensure we clean up the table even if there's an error
        try:
            with pyodbc.connect(CONN_STR) as conn:
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE {temp_table_name}")
                conn.commit()
        except:
            pass
        raise Exception(f"Error executing query with AIDs: {e}")


def read_csv_file(file_path, encoding='utf-8-sig', delimiter=';', required_columns=None, dtype=None):
    """Read CSV file with error handling."""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    
    df = pd.read_csv(file_path, encoding=encoding, delimiter=delimiter, dtype=dtype, on_bad_lines='warn')
    
    if required_columns:
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing columns: {', '.join(missing)}")
    
    return df


def read_sql_query(sql_file, aids=None):
    """Read and format SQL query with optional AIDs"""
    from pathlib import Path
    
    sql_path = Path(__file__).parent.parent / "sql" / sql_file
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")
    
    sql_query = sql_path.read_text(encoding='utf-8').strip()
    sql_query = '\n'.join(line for line in sql_query.split('\n') 
                          if not line.strip().startswith('--'))
    
    if aids:
        formatted_aids = ["'" + str(aid).replace("'", "''") + "'" for aid in aids]
        sql_query = sql_query.replace("{aid_placeholders}", ", ".join(formatted_aids))
    
    return sql_query
