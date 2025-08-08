import warnings
from simple_article_importer import import_sku_basis, import_sku_classification, import_sku_keyword
from sku_color_processor import process_colors

warnings.filterwarnings('ignore', category=UserWarning, 
                      message='pandas only supports SQLAlchemy connectable')

def main():
    # 1. Import SKU data
    print("Starting SKU import...")
    sku_basis_file = import_sku_basis()
    sku_classification_file = import_sku_classification()
    sku_keyword_file = import_sku_keyword()
    
    if not sku_basis_file or not sku_basis_file.exists():
        print("Failed to generate SKU basis data")
        return
    
    if not sku_classification_file or not sku_classification_file.exists():
        print("Failed to generate SKU classification data")
        return
    
    if not sku_keyword_file or not sku_keyword_file.exists():
        print("Failed to generate SKU keyword data")
        return
    
    # 2. Process SKU colors
    print("\nProcessing SKU colors...")
    final_sku_file = sku_basis_file.with_name("skus_basis.csv")
    final_sku_classification_file = sku_classification_file.with_name("skus_classification.csv")
    final_sku_keyword_file = sku_keyword_file.with_name("skus_keyword.csv")
    
    try:
        process_colors(csv_file_path=sku_basis_file, sku_column='aid')
        process_colors(csv_file_path=sku_classification_file, sku_column='aid')
        sku_basis_file.replace(final_sku_file)
        sku_classification_file.replace(final_sku_classification_file)
        sku_keyword_file.replace(final_sku_keyword_file)
        print(f"\nSKU basis processing completed! Output: {final_sku_file}")
        print(f"\nSKU classification processing completed! Output: {final_sku_classification_file}")
        print(f"\nSKU keyword processing completed! Output: {final_sku_keyword_file}")
    except Exception as e:
        print(f"\nError during color processing: {str(e)}")
        raise
    

if __name__ == "__main__":
    main()
