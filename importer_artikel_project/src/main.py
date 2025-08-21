import warnings
from simple_article_importer import (
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
from sku_color_processor import process_colors
from pathlib import Path
from fix_column_name import fix_column_names

warnings.filterwarnings('ignore', category=UserWarning, 
                      message='pandas only supports SQLAlchemy connectable')

def process_sku_data():
    sku_basis_file = import_sku_basis()
    sku_classification_file = import_sku_classification()
    sku_keyword_file = import_sku_keyword()
    sku_variant_file = import_sku_variant()
    sku_text_files = import_sku_text()  # List of text files
    
    if not all(f and f.exists() for f in [sku_basis_file, sku_classification_file, sku_keyword_file, sku_variant_file]):
        raise RuntimeError("Failed to generate SKU data")
    
    # Rename basic files
    final_sku_file = sku_basis_file.with_name("SKU - Artikel(Neuanlage).csv")
    final_sku_classification_file = sku_classification_file.with_name("SKU_CLASSIFICATION - Artikel-Merkmale.csv")
    final_sku_keyword_file = sku_keyword_file.with_name("SKU_KEYWORD - Artikel-Schlüsselworte.csv")
    #final_sku_variant_file = sku_variant_file.with_name("VARIANT_IMPORT - SKU-Variantenverknüpfung Import.csv")
    
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
    fix_column_names(sku_variant_file)

def process_article_data():
    artikel_basis_file = import_artikel_basis()
    artikel_classification_file = import_artikel_classification()
    artikel_zuordnung_file = import_artikel_zuordnung()
    artikel_keyword_file = import_artikel_keyword()
    artikel_variant_file = import_artikel_variant()
    artikel_text_files = import_artikel_text()  # List of text files
    
    if not all(f and f.exists() for f in [
        artikel_basis_file, 
        artikel_classification_file, 
        artikel_zuordnung_file, 
        artikel_keyword_file,
        artikel_variant_file,
    ]):
        raise RuntimeError("Failed to generate article data")
    
    # Rename basic files
    final_artikel_basis = artikel_basis_file.with_name("ARTICLE - Artikel(Neuanlage).csv")
    final_artikel_classification = artikel_classification_file.with_name("ARTICLE_CLASSIFICATION - Artikel-Merkmale.csv")
    final_artikel_zuordnung = artikel_zuordnung_file.with_name("ARTICLE_ASSIGNMENT - Artikel-Zuordnung.csv")
    final_artikel_keyword = artikel_keyword_file.with_name("ARTICLE_KEYWORD - Artikel-Schlüsselworte.csv")
    final_artikel_variant = artikel_variant_file.with_name("ARTICLE_VARIANT - Artikel-Variantenverknüpfung Import.csv")
    
    # Process each text file
    final_artikel_text_files = []
    for text_file in artikel_text_files:
        if text_file and text_file.exists():
            file_type = text_file.stem.split('_')[-1]
            final_name = f"ARTICLE_TEXT-{file_type.upper()}.csv"
            final_path = text_file.parent / final_name
            text_file.replace(final_path)
            final_artikel_text_files.append(final_path)
    
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
