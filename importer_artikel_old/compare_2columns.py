# -*- coding: utf-8 -*-
import pandas as pd
import sys
import argparse

def main():
    # Set up command line argument parsing
    parser = argparse.ArgumentParser(description='Compare two columns in a CSV file.')
    parser.add_argument('--file', type=str, default=r"C:\Users\gia.luongdo\Desktop\AID_SQL_.csv",
                      help='Path to the CSV file (default: C:\\Users\\gia.luongdo\\Desktop\\AID_SQL_.csv)')
    parser.add_argument('--col1', type=str, default='aid_ew',
                      help='Name of the first column to compare (default: aid_ew)')
    parser.add_argument('--col2', type=str, default='aid_erp',
                      help='Name of the second column to compare (default: aid_erp)')
    parser.add_argument('--delimiter', type=str, default=',',
                      help='CSV delimiter (default: ,)')
    
    args = parser.parse_args()
    
    try:
        # Read CSV with error handling for different encodings
        try:
            df = pd.read_csv(args.file, delimiter=args.delimiter, 
                           dtype={args.col1: str, args.col2: str}, 
                           encoding='utf-8')
        except UnicodeDecodeError:
            df = pd.read_csv(args.file, delimiter=args.delimiter, 
                           dtype={args.col1: str, args.col2: str}, 
                           encoding='windows-1252')
        
        print(f"\nAvailable columns: {df.columns.tolist()}")
        
        # Check if required columns exist
        if args.col1 not in df.columns or args.col2 not in df.columns:
            print(f"Error: Columns not found. Available columns: {df.columns.tolist()}")
            return
            
        # Process data
        col1 = df[args.col1].dropna().astype(str).str.strip()
        col2 = df[args.col2].dropna().astype(str).str.strip()
        
        # Find values in col1 not in col2
        diff = set(col1) - set(col2)
        
        if diff:
            print(f"\nFound {len(diff)} values in '{args.col1}' not in '{args.col2}':")
            print(list(diff)[:10])
            
            # Save results
            output_file = r"C:\Users\gia.luongdo\Desktop\aidew_notin_aiderp.csv"
            pd.DataFrame({f'{args.col1}_not_in_{args.col2}': list(diff)}).to_csv(
                output_file, index=False, encoding='utf-8-sig')
            print(f"\nResults saved to: {output_file}")
        else:
            print("\nNo differences found between the columns.")
            
    except FileNotFoundError:
        print(f"Error: File '{args.file}' not found.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
