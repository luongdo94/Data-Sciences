# -*- coding: utf-8 -*-
import pandas as pd
import sys

# Set UTF-8 encoding for output
sys.stdout.reconfigure(encoding='utf-8')

# --- Read data ---
file_to_compare = r"C:\Users\gia.luongdo\Desktop\AID_SQL_.csv"
column1_name = 'aid'
column2_name = 'AID'
csv_delimiter = ','

try:
    # Read CSV and specify data type as string to avoid conversion to float
    # Try different encodings to handle special characters
    try:
        df = pd.read_csv(file_to_compare, delimiter=csv_delimiter, dtype={column1_name: str, column2_name: str}, encoding='utf-8')
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(file_to_compare, delimiter=csv_delimiter, dtype={column1_name: str, column2_name: str}, encoding='windows-1252')
        except UnicodeDecodeError:
            df = pd.read_csv(file_to_compare, delimiter=csv_delimiter, dtype={column1_name: str, column2_name: str}, encoding='iso-8859-1')

    # Debug: Show all columns and first few rows
    print(f"Available columns: {df.columns.tolist()}")
    print(f"First 3 rows of data:")
    print(df.head(3))
    print(f"DataFrame shape: {df.shape}")
    
    # Check if required columns exist
    if column1_name not in df.columns or column2_name not in df.columns:
        print(f"Error: Missing column '{column1_name}' or '{column2_name}'. Available columns: {df.columns.tolist()}")
        raise SystemExit

    # Process data - remove NaN and convert to string, strip whitespace
    aid_list = df[column1_name].dropna().astype(str).str.strip().tolist()
    AID_list = df[column2_name].dropna().astype(str).str.strip().tolist()

    # Remove 'nan' string values if any
    aid_list = [x for x in aid_list if x.lower() != 'nan']
    AID_list = [x for x in AID_list if x.lower() != 'nan']
    
    # Remove duplicates from both columns
    aid_list = list(set(aid_list))  # Remove duplicates from aid column
    AID_list = list(set(AID_list))  # Remove duplicates from AID column

    # Check values in aid that are not in AID
    aid_not_in_AID = []
    for aid in aid_list:
        if aid not in AID_list:
            aid_not_in_AID.append(aid)
    
    # Check values in AID that are not in aid (reverse comparison)
    AID_not_in_aid = []
    for AID in AID_list:
        if AID not in aid_list:
            AID_not_in_aid.append(AID)

    print(f"Total values to check in '{column1_name}': {len(aid_list)}")
    print(f"Number of values in '{column2_name}': {len(AID_list)}")
    
    # Show sample values to check data type
    print(f"\nSample values from '{column1_name}': {aid_list[:5]}")
    print(f"Sample values from '{column2_name}': {AID_list[:5]}")
    
    # Check data types
    print(f"\nData type of '{column1_name}' column: {df[column1_name].dtype}")
    print(f"Data type of '{column2_name}' column: {df[column2_name].dtype}")

    # Report aid values not in AID (limit output to first 10)
    if aid_not_in_AID:
        print(f"\n=== Values in '{column1_name}' NOT found in '{column2_name}' ===")
        print(f"Total: {len(aid_not_in_AID)}")
        print("First 10 examples:")
        for i, v in enumerate(aid_not_in_AID[:10]):
            print(f"  {i+1}. {v}")
        if len(aid_not_in_AID) > 10:
            print(f"  ... and {len(aid_not_in_AID) - 10} more")
        
        # Export to CSV file
        export_filename = r"C:\Users\gia.luongdo\Desktop\aid_not_in_AID_results.csv"
        export_df = pd.DataFrame({f'{column1_name}_not_in_{column2_name}': aid_not_in_AID})
        export_df.to_csv(export_filename, index=False, encoding='utf-8-sig')
        print(f"\n✓ Exported {len(aid_not_in_AID)} values to: {export_filename}")
    else:
        print(f"\n=== All values in '{column1_name}' exist in '{column2_name}' ===")
    
    # Report AID values not in aid (reverse comparison, limit output to first 10)
    if AID_not_in_aid:
        print(f"\n=== Values in '{column2_name}' NOT found in '{column1_name}' ===")
        print(f"Total: {len(AID_not_in_aid)}")
        print("First 10 examples:")
        for i, v in enumerate(AID_not_in_aid[:10]):
            print(f"  {i+1}. {v}")
        if len(AID_not_in_aid) > 10:
            print(f"  ... and {len(AID_not_in_aid) - 10} more")
    else:
        print(f"\n=== All values in '{column2_name}' exist in '{column1_name}' ===")

except FileNotFoundError:
    print(f"Error: File '{file_to_compare}' not found.")
except Exception as e:
    print(f"Error: {e}")
