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
    import_artikel_variant
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
    
    sku_basis_file = import_sku_basis()
    sku_classification_file = import_sku_classification()
    sku_keyword_file = import_sku_keyword()
    sku_variant_file = import_sku_variant()
    sku_text_files = import_sku_text() or []  # List of text files, default to empty list if None
    
    # Skip if any required file is None or doesn't exist
    if not all(f and f.exists() for f in [sku_basis_file, sku_classification_file, sku_keyword_file, sku_variant_file]):
        print("Warning: Not all SKU data files were generated. Some files may be missing.")
        return
    
    # Rename basic files
    final_sku_file = sku_basis_file.with_name("SKU - Artikel(Neuanlage).csv")
    final_sku_classification_file = sku_classification_file.with_name("SKU_CLASSIFICATION - Artikel-Merkmale.csv")
    final_sku_keyword_file = sku_keyword_file.with_name("SKU_KEYWORD - Artikel-Schlüsselworte.csv")
    final_sku_variant_file = sku_variant_file.with_name("VARIANT_IMPORT - SKU-Variantenverknüpfung Import.csv")
    
    # Process colors for basic files
    process_colors(csv_file_path=sku_basis_file, sku_column='aid')
    process_colors(csv_file_path=sku_classification_file, sku_column='aid')
    process_colors(csv_file_path=sku_keyword_file, sku_column='aid')
    process_colors(csv_file_path=sku_variant_file, sku_column='aid')
    
    # Process each text file
    final_sku_text_files = []
    for text_file in sku_text_files:
        if text_file and text_file.exists():
            process_colors(csv_file_path=text_file, sku_column='aid')
            file_type = text_file.stem.split('_')[-1]
            final_name = f"SKU_TEXT-{file_type.upper()}.csv"
            final_path = text_file.parent / final_name
            text_file.replace(final_path)
            final_sku_text_files.append(final_path)
    
    # Rename basic files
    sku_basis_file.replace(final_sku_file)
    sku_classification_file.replace(final_sku_classification_file)
    sku_keyword_file.replace(final_sku_keyword_file)
    sku_variant_file.replace(final_sku_variant_file)

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
        final_artikel_text_files = []
        for text_file in artikel_text_files:
            if text_file and text_file.exists():
                file_type = text_file.stem.split('_')[-1]
                final_name = f"ARTICLE_TEXT-{file_type.upper()}.csv"
                final_path = text_file.parent / final_name
                text_file.replace(final_path)
                final_artikel_text_files.append(final_path)
                print(f"Created: {final_path}")
    
    # Rename basic files
    artikel_basis_file.replace(final_artikel_basis)
    artikel_classification_file.replace(final_artikel_classification)
    artikel_zuordnung_file.replace(final_artikel_zuordnung)
    artikel_keyword_file.replace(final_artikel_keyword)
    artikel_variant_file.replace(final_artikel_variant)

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
