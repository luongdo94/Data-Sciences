import warnings
import sys
from pathlib import Path

# Setup
OUTPUT_DIR = Path(__file__).parent.parent / 'data' / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
sys.path.append(str(Path(__file__).parent.parent))

# Import from the local module since we're already in the src directory
from src.simple_article_importer import (
    import_sku_basis, import_sku_classification, import_sku_keyword,
    import_artikel_basis, import_artikel_classification, import_artikel_zuordnung,
    import_artikel_keyword, import_artikel_text, import_sku_text,
    import_sku_variant, import_artikel_variant, import_artikel_preisstufe_3_7,
    import_artikel_basicprice, import_artikel_pricestaffeln,
    import_sku_EAN, import_sku_gebinde, update_sku,
    import_order, import_order_are_15, import_order_pos, import_order_pos_are_15, import_order_classification,
    import_artikel_text_en, import_sku_text_en,
    import_stock_lager, import_business_partner
)
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
    # Process core SKU data
    print("\n=== Processing Core SKU Data ===")
    sku_basis_file = import_sku_basis()
    sku_classification_file = import_sku_classification()
    sku_keyword_file = import_sku_keyword()
    sku_variant_file = import_sku_variant()
    sku_update_file = update_sku()
    
    # Process price data
    print("\n=== Processing Price Data ===")
    sku_basicprice_file = None
    sku_pricestaffeln_file = None
    sku_price_file = None
    try:
        sku_basicprice_file = import_artikel_basicprice(
            basicprice_filename="PRICELIST - Artikel-Basispreis.csv",
            validity_filename="Basispreis_validity_data.csv"
        )
        sku_pricestaffeln_file = import_artikel_pricestaffeln()
        sku_price_file = import_artikel_preisstufe_3_7()
    except Exception as e:
        print(f"Error processing price data: {e}")
        import traceback
        traceback.print_exc()
    
    # Process additional SKU data
    print("\n=== Processing Additional SKU Data ===")
    sku_text_files = import_sku_text() or []
    sku_text_en_files = import_sku_text_en() or []
    sku_EAN_file = import_sku_EAN()
    
    # Get both output files from import_sku_gebinde()
    sku_gebinde_ve_file = import_sku_gebinde()
    sku_gebinde_standard_file = OUTPUT_DIR / "artikel_gebinde.csv"
    
    # Define all output files with their final names
    output_files = [
        (sku_basis_file, "SKU - Artikel(Neuanlage).csv"),
        (sku_classification_file, "SKU_CLASSIFICATION - Artikel-Merkmale.csv"),
        (sku_keyword_file, "SKU_KEYWORD - Artikel-Schlüsselworte.csv"),
        (sku_variant_file, "VARIANT_IMPORT - SKU-Variantenverknüpfung Import.csv"),
        (sku_update_file, "SKU_UPDATE - Artikel-Aktualisierung.csv"),
        (sku_EAN_file, "SKU_EAN - Artikel-EAN.csv"),
        (sku_gebinde_ve_file, "ARTICLE_PACKAGING_IMPORT - SKU-Gebindedaten_VE.csv"),
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
                    df = pd.read_csv(price_file, sep=';', dtype=str)
                    if 'aktiv' in df.columns:
                        df = df.drop(columns=['aktiv'])
                        df.to_csv(price_file, index=False, sep=';', encoding='utf-8-sig')
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
    
    # Process article data
    files = [
        (import_artikel_basis(), "ARTICLE - Artikel(Neuanlage).csv"),
        (import_artikel_classification(), "ARTICLE_CLASSIFICATION - Artikel-Merkmale.csv"),
        (import_artikel_zuordnung(), "ARTICLE_ASSIGNMENT - Artikel-Zuordnung.csv"),
        (import_artikel_keyword(), "ARTICLE_KEYWORD - Artikel-Schlüsselworte.csv"),
        (import_artikel_variant(), "ARTICLE_VARIANT - Artikel-Variantenverknüpfung Import.csv")
    ]
    
    # Rename files
    for file_path, final_name in files:
        if file_path and Path(file_path).exists():
            safe_rename(file_path, Path(file_path).parent / final_name, final_name)
    
    # Process text files
    artikel_text_files = import_artikel_text() or []
    artikel_text_en_files = import_artikel_text_en() or []
    
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
        # Process order data
        order_files = [
            (import_order(), "CONTRACT - Kontrakte.csv"),
            (import_order_are_15(), "CONTRACT - Kontrakte_15.csv"),
            (import_order_pos(), "CONTRACT_ITEM - Kontraktpositionen.csv"),
            (import_order_pos_are_15(), "CONTRACT_ITEM - Kontraktpositionen_15.csv"),
            (import_order_classification(), "CONTRACT - Kontrakte-Klassifikation.csv")
        ]
        
        # Rename files
        for file_path, final_name in order_files:
            if file_path and Path(file_path).exists():
                safe_rename(file_path, Path(file_path).parent / final_name, final_name)
        
        print("Order data processing completed successfully!")
    except Exception as e:
        print(f"Error processing order data: {e}")
        import traceback
        traceback.print_exc()

def main():
    try:
        # Process SKU data
        process_sku_data()
        # Process Article data
        process_article_data()
        # Process Order data
        process_order_data()
        
        # Process Stock and Partner data
        print("\n=== Processing Stock and Partner Data ===")
        # Process Stock/Lager
        stock_files = import_stock_lager()
        if stock_files:
            for file_path in stock_files:
                if file_path and Path(file_path).exists():
                    print(f"[OK] Stock data exported: {file_path.name}")
                    
        # Process Business Partner
        partner_file = import_business_partner()
        if partner_file and Path(partner_file).exists():
             safe_rename(partner_file, OUTPUT_DIR / "BUSINESS_PARTNER_IMPORT.csv", "BUSINESS_PARTNER_IMPORT.csv")
             
        print("\nAll data processing completed successfully!")
    except Exception as e:
        print(f"Error: {e}")
        raise
 
if __name__ == "__main__":
    main()
