import warnings
import sys
from pathlib import Path

# Add parent directory to path to allow direct script execution
sys.path.append(str(Path(__file__).parent.parent))

from src.simple_article_importer import (
    import_sku_basis, 
    import_sku_classification, 
    import_sku_keyword,
    import_artikel_basis,
    import_artikel_classification,
    import_artikel_zuordnung,
    import_artikel_keyword,
    import_artikel_text,
    import_sku_text,
    import_sku_variant,
    import_artikel_variant,
    import_artikel_preisstufe_3_7,
    import_artikel_basicprice,
    import_artikel_pricestaffeln,
    import_sku_EAN,
    import_sku_gebinde,
)
from src.sku_color_processor import process_colors


warnings.filterwarnings('ignore', category=UserWarning, 
                      message='pandas only supports SQLAlchemy connectable')

def process_sku_data():
    from run_comparison_standalone import diff
    # Skip processing if diff is empty
    if not diff or len(diff) == 0:
        print("No AIDs to process in diff. Skipping SKU data generation.")
        return
        
    print(f"\nProcessing SKU data for {len(diff)} AIDs...")
    
    # Process core SKU data
    print("\n=== Processing Core SKU Data ===")
    sku_basis_file = import_sku_basis()
    sku_classification_file = import_sku_classification()
    sku_keyword_file = import_sku_keyword()
    sku_variant_file = import_sku_variant()
    
    # Process price data
    print("\n=== Processing Price Data ===")
    try:
        # 1. Process Basic Price
        sku_basicprice_file = import_artikel_basicprice(
            basicprice_filename="PRICELIST - Artikel-Basispreis.csv",
            validity_filename="Basispreis_validity_data.csv"
        )
        # 2. Process Price Staffeln
        sku_pricestaffeln_file = import_artikel_pricestaffeln()
        # 3. Process Preisstufe 3-7
        sku_price_file = import_artikel_preisstufe_3_7()
        
    except Exception as e:
        print(f"Error processing price data: {e}")
        import traceback
        traceback.print_exc()
    
    # Process additional SKU data
    print("\n=== Processing Additional SKU Data ===")
    sku_text_files = import_sku_text() or []  # List of text files, default to empty list if None
    sku_EAN_file = import_sku_EAN()
    sku_gebinde_file = import_sku_gebinde()
    
    # Check required files
    required_files = [
        (sku_basis_file, "SKU - Artikel(Neuanlage).csv"),
        (sku_classification_file, "SKU_CLASSIFICATION - Artikel-Merkmale.csv"),
        (sku_keyword_file, "SKU_KEYWORD - Artikel-Schlüsselworte.csv"),
        (sku_variant_file, "VARIANT_IMPORT - SKU-Variantenverknüpfung Import.csv")
    ]
    
    # Check if all required files exist
    missing_files = [name for f, name in required_files if f is None or not Path(str(f)).exists()]
    if missing_files:
        print("\nWarning: Some required files are missing:")
        for name in missing_files:
            print(f"  - {name}")
        print("\nDebug - Files status:")
        for f, name in required_files:
            exists = f is not None and Path(str(f)).exists()
            print(f"  {name}: {'✅ Exists' if exists else '❌ Missing'} ({f})")
    
    # Ensure all file paths are Path objects and handle potential None returns
    def ensure_path(path, default_name):
        if path is None:
            return None
        path = Path(str(path))  # Convert to Path if it's a string
        return path.parent / default_name
    
    # Define all output files with their final names
    output_files = [
        (sku_basis_file, "SKU - Artikel(Neuanlage).csv"),
        (sku_classification_file, "SKU_CLASSIFICATION - Artikel-Merkmale.csv"),
        (sku_keyword_file, "SKU_KEYWORD - Artikel-Schlüsselworte.csv"),
        (sku_variant_file, "VARIANT_IMPORT - SKU-Variantenverknüpfung Import.csv"),
        (sku_price_file, "PRICELIST - Artikel-Preisstufe_3_7.csv"),
        (sku_basicprice_file, "PRICELIST - Artikel-Basispreis.csv"),
        (sku_pricestaffeln_file, "PRICELIST - Artikel-Preisstafeln.csv"),
        (sku_EAN_file, "SKU_EAN - Artikel-EAN.csv"),
        (sku_gebinde_file, "ARTICLE_PACKAGING_IMPORT - SKU-Gebindedaten.csv")
    ]
    
    # Handle the validity file path
    if sku_basicprice_file is not None:
        validity_path = Path(str(sku_basicprice_file).replace("PRICELIST - Artikel-Basispreis.csv", "Basispreis_validity_data.csv"))
        final_sku_validity_file = (validity_path.parent / "PRICELIST - Basispreis_validity.csv", "PRICELIST - Basispreis_validity.csv")
        output_files.append(final_sku_validity_file)
    
    # Process and rename all files
    final_files = []
    for file_info in output_files:
        if len(file_info) == 2:
            file_path, final_name = file_info
            if file_path is not None and Path(str(file_path)).exists():
                final_path = ensure_path(file_path, final_name)
                final_files.append((file_path, final_path, final_name))
    
    # Helper function to safely process colors for a file
    def safe_process_colors(file_path, sku_column):
        if file_path is None:
            return
        path = Path(str(file_path))  # Convert to Path if it's a string
        if path.exists():
            try:
                process_colors(csv_file_path=path, sku_column=sku_column)
            except Exception as e:
                print(f"Warning: Error processing colors for {path}: {e}")
    
    # Process colors for all generated files
    print("\n=== Processing Colors ===")
    for _, final_path, _ in final_files:
        safe_process_colors(final_path, 'aid')
    
    # Process text files
    text_file_columns = {
        'webshoptext': 'ARTICLE_TEXT - Webshoptext',
        'artikeltext': 'ARTICLE_TEXT - Artikeltext',
        'katalogtext': 'ARTICLE_TEXT - Katalogtext',
        'vertriebstext': 'ARTICLE_TEXT - Vertriebstext',
        'rechnungstext': 'ARTICLE_TEXT - Rechnungstext',
        'pflegehinweise': 'ARTICLE_TEXT - Pflegehinweise'
    }
    
    for text_file in sku_text_files:
        if text_file:
            try:
                path = Path(str(text_file))  # Convert to Path if it's a string
                if path.exists():
                    # Get the base filename without extension to determine the type
                    file_stem = path.stem.lower()
                    for key, column in text_file_columns.items():
                        if key in file_stem:
                            safe_process_colors(path, column)
                            break
            except Exception as e:
                print(f"Warning: Error processing text file {text_file}: {e}")
    
    # Helper function to safely rename a file
    def safe_rename(src, dst):
        if src is None or dst is None:
            return
        try:
            src_path = Path(str(src))  # Convert to Path if it's a string
            dst_path = Path(str(dst))  # Convert to Path if it's a string
            
            if not src_path.exists():
                print(f"Warning: Source file does not exist: {src_path}")
                return
                
            # Ensure destination directory exists
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Remove destination file if it exists
            if dst_path.exists():
                dst_path.unlink()
                
            # Rename the file
            src_path.rename(dst_path)
            print(f"Renamed {src_path} to {dst_path}")
        except Exception as e:
            print(f"Error renaming {src} to {dst}: {e}")
    
    # Rename all files to their final names
    print("\n=== Renaming Output Files ===")
    for src_path, final_path, final_name in final_files:
        try:
            if src_path is not None and Path(str(src_path)).exists():
                src_path = Path(str(src_path))
                if src_path != final_path:
                    src_path.replace(final_path)
                print(f"✓ {final_name}")
        except Exception as e:
            print(f"✗ Error renaming {src_path} to {final_path}: {e}")

def process_article_data():
    from run_comparison_standalone import diff1
    
    # Skip processing if diff1 is empty
    if not diff1 or len(diff1) == 0:
        print("No AIDs to process in diff1. Skipping article data generation.")
        return
        
    print(f"\nProcessing article data for {len(diff1)} AIDs...")
    
    # Process article data
    artikel_basis_file = import_artikel_basis()
    artikel_classification_file = import_artikel_classification()
    artikel_zuordnung_file = import_artikel_zuordnung()
    artikel_keyword_file = import_artikel_keyword()
    artikel_variant_file = import_artikel_variant()
    artikel_text_files = import_artikel_text()  # List of text files
    
    # Check if all required files were generated
    required_files = [
        (artikel_basis_file, "ARTICLE - Artikel(Neuanlage).csv"),
        (artikel_classification_file, "ARTICLE_CLASSIFICATION - Artikel-Merkmale.csv"),
        (artikel_zuordnung_file, "ARTICLE_ASSIGNMENT - Artikel-Zuordnung.csv"),
        (artikel_keyword_file, "ARTICLE_KEYWORD - Artikel-Schlüsselworte.csv"),
        (artikel_variant_file, "ARTICLE_VARIANT - Artikel-Variantenverknüpfung Import.csv")
    ]
    
    # Filter out None values (files that weren't created)
    valid_files = [(f, name) for f, name in required_files if f and f.exists()]
    
    # Rename valid files
    for file_obj, new_name in valid_files:
        final_path = file_obj.with_name(new_name)
        file_obj.replace(final_path)
        print(f"Created: {final_path}")
    
    # Process text files if any
    if artikel_text_files:
        for text_file in artikel_text_files:
            if text_file and text_file.exists():
                file_type = text_file.stem.split('_')[-1]
                final_name = f"ARTICLE_TEXT-{file_type.upper()}.csv"
                final_path = text_file.parent / final_name
                text_file.replace(final_path)
                print(f"Created: {final_path}")

def main():
    try:
        # Process SKU data
        process_sku_data()
        # Process Article data
        process_article_data()
        print("\nAll data processing completed successfully!")
    except Exception as e:
        print(f"Error: {e}")
        raise
 
if __name__ == "__main__":
    main()
