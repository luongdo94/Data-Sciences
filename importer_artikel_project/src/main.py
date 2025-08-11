import warnings
from simple_article_importer import (
    import_sku_basis, 
    import_sku_classification, 
    import_sku_keyword,
    import_artikel_basis,
    import_artikel_classification,
    import_artikel_zuordnung,
    import_artikel_keyword
)
from sku_color_processor import process_colors
from pathlib import Path

warnings.filterwarnings('ignore', category=UserWarning, 
                      message='pandas only supports SQLAlchemy connectable')

def process_sku_data():
    sku_basis_file = import_sku_basis()
    sku_classification_file = import_sku_classification()
    sku_keyword_file = import_sku_keyword()
    
    if not all(f and f.exists() for f in [sku_basis_file, sku_classification_file, sku_keyword_file]):
        raise RuntimeError("Failed to generate SKU data")
    
    final_sku_file = sku_basis_file.with_name("skus_basis.csv")
    final_sku_classification_file = sku_classification_file.with_name("skus_classification.csv")
    final_sku_keyword_file = sku_keyword_file.with_name("skus_keyword.csv")
    
    process_colors(csv_file_path=sku_basis_file, sku_column='aid')
    process_colors(csv_file_path=sku_classification_file, sku_column='aid')
    process_colors(csv_file_path=sku_keyword_file, sku_column='aid')
    
    sku_basis_file.replace(final_sku_file)
    sku_classification_file.replace(final_sku_classification_file)
    sku_keyword_file.replace(final_sku_keyword_file)

def process_article_data():
    artikel_basis_file = import_artikel_basis()
    artikel_classification_file = import_artikel_classification()
    artikel_zuordnung_file = import_artikel_zuordnung()
    artikel_keyword_file = import_artikel_keyword()
    
    if not all(f and f.exists() for f in [
        artikel_basis_file, 
        artikel_classification_file, 
        artikel_zuordnung_file, 
        artikel_keyword_file
    ]):
        raise RuntimeError("Failed to generate article data")
    
    final_artikel_basis = artikel_basis_file.with_name("artikel_basis.csv")
    final_artikel_classification = artikel_classification_file.with_name("artikel_classification.csv")
    final_artikel_zuordnung = artikel_zuordnung_file.with_name("artikel_zuordnung.csv")
    final_artikel_keyword = artikel_keyword_file.with_name("artikel_keyword.csv")
    
    artikel_basis_file.replace(final_artikel_basis)
    artikel_classification_file.replace(final_artikel_classification)
    artikel_zuordnung_file.replace(final_artikel_zuordnung)
    artikel_keyword_file.replace(final_artikel_keyword)

def main():
    try:
        process_sku_data()
        process_article_data()
    except Exception as e:
        print(f"Error: {str(e)}")
        raise
    

if __name__ == "__main__":
    main()
