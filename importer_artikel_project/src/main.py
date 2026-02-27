import warnings
import sys
import pandas as pd
from pathlib import Path

# Setup
OUTPUT_DIR = Path(__file__).parent.parent / 'data' / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
sys.path.append(str(Path(__file__).parent.parent))

# Import from the local module since we're already in the src directory
from src.database import read_csv_file, save_fetcsv
from src.article_importer_class import ArticleImporter
from src.order_importer_class import OrderImporter
from src.stock_importer_class import StockImporter
from src.bp_importer_class import BusinessPartnerImporter
from src.sku_color_processor import process_colors

warnings.filterwarnings('ignore', category=UserWarning, 
                      message='pandas only supports SQLAlchemy connectable')

# Helper functions
def safe_process_colors(file_path, sku_column='aid'):
    """Safely process colors for a file"""
    if file_path and Path(file_path).exists():
        try:
            process_colors(csv_file_path=Path(file_path), sku_column=sku_column)
        except Exception as e:
            print(f"Warning: Error processing colors for {file_path}: {e}")

def safe_rename(src, dst, display_name):
    """Safely rename a file"""
    try:
        src_path = Path(src)
        dst_path = Path(dst)
        if src_path.exists() and src_path != dst_path:
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            if dst_path.exists():
                dst_path.unlink()
            src_path.rename(dst_path)
            print(f"[OK] {display_name}")
    except Exception as e:
        print(f"[ERROR] Error renaming {display_name}: {e}")

def get_diff(diff_name='diff'):
    """Get diff from comparison module"""
    try:
        from run_comparison_standalone import diff, diff1
        return diff if diff_name == 'diff' else diff1
    except (ImportError, FileNotFoundError):
        return None

def process_sku_data():
    diff = get_diff('diff')
    # Initialize importer
    importer = ArticleImporter(diff=diff)
    
    # Process core SKU data
    print("\n=== Processing Core SKU Data ===")
    sku_basis_file = importer.import_sku_basis()
    sku_classification_file = importer.import_sku_classification()
    sku_keyword_file = importer.import_sku_keyword()
    sku_variant_file = importer.import_sku_variant()
    sku_update_file = importer.update_sku()
    
    # Process price data
    print("\n=== Processing Price Data ===")
    sku_basicprice_file = None
    sku_pricestaffeln_file = None
    sku_price_file = None
    try:
        sku_basicprice_file = importer.import_artikel_basicprice() # Default args are fine or baked into class method if simplified
        sku_pricestaffeln_file = importer.import_artikel_pricestaffeln()
        sku_price_file = importer.import_artikel_preisstufe_3_7()
    except Exception as e:
        print(f"Error processing price data: {e}")
        import traceback
        traceback.print_exc()
    
    # Process additional SKU data
    print("\n=== Processing Additional SKU Data ===")
    sku_text_files = importer.import_sku_text() or []
    sku_text_en_files = importer.import_sku_text_en() or []
    sku_EAN_file = importer.import_sku_ean() # Note capitalization change if any, checked class method name is import_sku_ean
    
    # Get both output files from import_sku_gebinde()
    # import_sku_gebinde returns the standard file path
    importer.import_sku_gebinde()
    
    sku_gebinde_standard_file = OUTPUT_DIR / "artikel_gebinde.csv"
    sku_gebinde_ve_file = OUTPUT_DIR / "ARTICLE_PACKAGING_IMPORT - SKU-Gebindedaten_VE.csv"
    
    # Define all output files with their final names
    output_files = [
        (sku_basis_file, "SKU - Artikel(Neuanlage).csv"),
        (sku_classification_file, "SKU_CLASSIFICATION - Artikel-Merkmale.csv"),
        (sku_keyword_file, "SKU_KEYWORD - Artikel-Schlüsselworte.csv"),
        (sku_variant_file, "VARIANT_IMPORT - SKU-Variantenverknüpfung Import.csv"),
        (sku_update_file, "SKU_UPDATE - Artikel-Aktualisierung.csv"),
        (sku_EAN_file, "SKU_EAN - Artikel-EAN.csv"),
        (sku_gebinde_ve_file, "ARTICLE_PACKAGING_IMPORT - SKU-Gebindedaten_VE.csv") if sku_gebinde_ve_file.exists() else None,
        (sku_gebinde_standard_file, "ARTICLE_PACKAGING_IMPORT - SKU-Gebindedaten.csv") if sku_gebinde_standard_file.exists() else None,
        (OUTPUT_DIR / "sku_text_artikeltext.csv", "SKU_TEXT - Artikeltext.csv"),
        (OUTPUT_DIR / "sku_text_katalogtext.csv", "SKU_TEXT - Katalogtext.csv"),
        (OUTPUT_DIR / "sku_text_pflegehinweise.csv", "SKU_TEXT - Pflegehinweise.csv"),
        (OUTPUT_DIR / "sku_text_rechnungstext.csv", "SKU_TEXT - Rechnungstext.csv"),
        (OUTPUT_DIR / "sku_text_vertriebstext.csv", "SKU_TEXT - Vertriebstext.csv"),
        (OUTPUT_DIR / "sku_text_webshoptext.csv", "SKU_TEXT - Webshoptext.csv"),
    ]
    # Remove any None entries
    output_files = [f for f in output_files if f[0] is not None]
    
    # Add price files by finding them in OUTPUT_DIR (since import functions don't return paths)
    price_files = [
        ("PRICELIST - Artikel-Basispreis.csv", "PRICELIST - Artikel-Basispreis.csv"),
        ("PRICELIST- Artikel-Preisstafeln.csv", "PRICELIST - Artikel-Preisstafeln.csv"),
        ("PRICELIST- Artikel-Preisstufe_3_7.csv", "PRICELIST - Artikel-Preisstufe_3_7.csv"),
        ("PRICELIST_basicprice_validity.csv", "PRICELIST - Basispreis_validity.csv"),
        ("PRICELIST_pricestaffeln_validity.csv", "PRICELIST - Preisstaffeln_validity.csv"),
        ("PRICELIST_preisstufe3_7_validity.csv", "PRICELIST - Preisstufe3_7_validity.csv"),
    ]
    
    for source_name, final_name in price_files:
        price_file = OUTPUT_DIR / source_name
        if price_file.exists():
            # For validity files, ensure 'aktiv' column is removed
            if 'validity' in str(price_file).lower():
                try:
                    df = read_csv_file(price_file, dtype=str)
                    if 'aktiv' in df.columns:
                        df = df.drop(columns=['aktiv'])
                        save_fetcsv(df, price_file, "ARTICLE")
                except Exception as e:
                    print(f"Warning: Could not process {price_file}: {e}")
            output_files.append((price_file, final_name))
    
    # Process colors and rename files
    print("\n=== Processing Colors ===")
    processed_files = set()
    for file_path, final_name in output_files:
        if file_path and Path(file_path).exists() and str(file_path) not in processed_files:
            safe_process_colors(file_path)
            safe_rename(file_path, Path(file_path).parent / final_name, final_name)
            processed_files.add(str(file_path))
    
    # Process text files
    text_types = ['webshoptext', 'artikeltext', 'katalogtext', 'vertriebstext', 'rechnungstext', 'pflegehinweise']
    
    # Process German text files
    for text_file in sku_text_files:
        if text_file and Path(text_file).exists():
            file_stem = Path(text_file).stem.lower()
            for text_type in text_types:
                if text_type in file_stem and '_en_' not in file_stem:
                    final_name = f"SKU_TEXT - {text_type.capitalize()}.csv"
                    safe_process_colors(text_file, f"SKU_TEXT - {text_type.capitalize()}")
                    safe_rename(text_file, Path(text_file).parent / final_name, final_name)
                    break

    # Process English text files
    for text_file in sku_text_en_files:
        if text_file and Path(text_file).exists():
            file_stem = Path(text_file).stem.lower()
            for text_type in text_types:
                if text_type in file_stem:
                    final_name = f"SKU_TEXT_EN - {text_type.capitalize()}.csv"
                    safe_process_colors(text_file, f"SKU_TEXT_EN - {text_type.capitalize()}")
                    safe_rename(text_file, Path(text_file).parent / final_name, final_name)
                    break

def process_article_data():
    diff1 = get_diff('diff1')
    print(f"\nProcessing article data for {len(diff1)} AIDs..." if diff1 else "\nProcessing all article data...")
    
    importer = ArticleImporter(diff1=diff1)
    
    # Process article data
    files = [
        (importer.import_artikel_basis(), "ARTICLE - Artikel(Neuanlage).csv"),
        (importer.import_artikel_classification(), "ARTICLE_CLASSIFICATION - Artikel-Merkmale.csv"),
        (importer.import_artikel_zuordnung(), "ARTICLE_ASSIGNMENT - Artikel-Zuordnung.csv"),
        (importer.import_artikel_keyword(), "ARTICLE_KEYWORD - Artikel-Schlüsselworte.csv"),
        (importer.import_artikel_variant(), "ARTICLE_VARIANT - Artikel-Variantenverknüpfung Import.csv")
    ]
    
    # Rename files
    for file_path, final_name in files:
        if file_path and Path(file_path).exists():
            safe_rename(file_path, Path(file_path).parent / final_name, final_name)
    
    # Process text files
    artikel_text_files = importer.import_artikel_text() or []
    artikel_text_en_files = importer.import_artikel_text_en() or []
    
    # Process German text files
    for text_file in artikel_text_files:
        if text_file and Path(text_file).exists():
            file_type = Path(text_file).stem.split('_')[-1]
            final_name = f"ARTICLE_TEXT-{file_type.upper()}.csv"
            safe_rename(text_file, Path(text_file).parent / final_name, final_name)

    # Process English text files
    for text_file in artikel_text_en_files:
        if text_file and Path(text_file).exists():
            file_type = Path(text_file).stem.split('_')[-1]
            final_name = f"ARTICLE_TEXT_EN-{file_type.upper()}.csv"
            safe_rename(text_file, Path(text_file).parent / final_name, final_name)

def process_order_data():
    print("\n=== Processing Order Data ===")
    try:
        # Initialize importer
        importer = OrderImporter()
        
        # Process order data
        order_files = [
            (importer.import_order(), "CONTRACT - Kontrakte.csv"),
            (importer.import_order_are_15(), "CONTRACT - Kontrakte_15.csv"),
            (importer.import_order_pos(), "CONTRACT_ITEM - Kontraktpositionen.csv"),
            (importer.import_order_pos_are_15(), "CONTRACT_ITEM - Kontraktpositionen_15.csv"),
            (importer.import_order_classification(), "CONTRACT - Kontrakte-Klassifikation.csv")
        ]
        
        # Rename files
        for file_path, final_name in order_files:
            if file_path and Path(file_path).exists():
                safe_rename(file_path, Path(file_path).parent / final_name, final_name)
        
        print("Order data processing completed successfully!")
    except Exception as e:
        print(f"Error processing order data: {e}")
        import traceback
        import traceback
        traceback.print_exc()

def process_stock_data():
    print("\n=== Processing Stock Data ===")
    try:
        stock_importer = StockImporter()
        stock_files = stock_importer.import_stock_lager()
        if stock_files:
            for file_path in stock_files:
                if file_path and Path(file_path).exists():
                    print(f"[OK] Stock data exported: {file_path.name}")
    except Exception as e:
        print(f"[ERROR] Stock import failed: {e}")

def process_business_partner_data():
    print("\n=== Processing Business Partner Data ===")
    try:
        bp_importer = BusinessPartnerImporter()
        
        # Main Partner Data
        partner_file = bp_importer.import_business_customer()
        if partner_file and Path(partner_file).exists():
             safe_rename(partner_file, OUTPUT_DIR / "BUSINESS_PARTNER_CUSTOMER.csv", "BUSINESS_PARTNER_CUSTOMER.csv")

        # Accounting Partner Data
        accounting_file = bp_importer.import_business_customer_accounting()
        if accounting_file and Path(accounting_file).exists():
             print(f"[OK] Accounting partner data exported: {accounting_file.name}")

        # Supplier Data
        supplier_file = bp_importer.import_business_supplier()
        if supplier_file and Path(supplier_file).exists():
            print(f"[OK] Supplier data exported: {supplier_file.name}")
            
        # Address Data
        address_file = bp_importer.import_customer_address()
        if address_file and Path(address_file).exists():
            print(f"[OK] Address data exported: {address_file.name}")
            
        # Contact Data
        contact_file = bp_importer.import_customer_contact()
        if contact_file and Path(contact_file).exists():
            print(f"[OK] Contact data exported: {contact_file.name}")

        # Keyword Data
        keyword_file = bp_importer.import_customer_keyword()
        if keyword_file and Path(keyword_file).exists():
            print(f"[OK] Keyword data exported: {keyword_file.name}")
            
        comm_files = bp_importer.import_customer_communication()
        if comm_files:
            for f in comm_files:
                if f and Path(f).exists():
                     print(f"[OK] Communication data exported: {f.name}")
        
        # Contact Communication Data
        contact_comm_files = bp_importer.import_customer_contact_communication()
        if contact_comm_files:
            for f in contact_comm_files:
                if f and Path(f).exists():
                     print(f"[OK] Contact Communication data exported: {f.name}")
        
        # Contact Role Data
        role_files = bp_importer.import_customer_employee_role()
        if role_files:
            for f in role_files:
                if f and Path(f).exists():
                     print(f"[OK] Contact Role data exported: {f.name}")
        
        # Supplier Communication Data
        sup_comm_files = bp_importer.import_supplier_communication()
        if sup_comm_files:
            for f in sup_comm_files:
                if f and Path(f).exists():
                     print(f"[OK] Supplier Communication data exported: {f.name}")

        # Supplier Address Data
        sup_address_file = bp_importer.import_supplier_address()
        if sup_address_file and Path(sup_address_file).exists():
            print(f"[OK] Supplier Address data exported: {sup_address_file.name}")
    except Exception as e:
        print(f"[ERROR] Business Partner import failed: {e}")

def main():
    try:
        # Process SKU data
        #process_sku_data()
        # Process Article data
        #process_article_data()
        # Process Order data
        #process_order_data()
        # Process Stock Data
        #process_stock_data()
        # Process Business Partner Data
        process_business_partner_data()
        
        print("\nAll data processing completed successfully!")
    except Exception as e:
        print(f"Error: {e}")
        raise
 
if __name__ == "__main__":
    main()
