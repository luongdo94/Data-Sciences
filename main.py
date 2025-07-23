
# Import functions from other modules
import Importer_Article_Basis
import Importer_SKU
from Importer_Article_Basis import (
    read_and_write_article_data,
    read_and_write_Zuordnung_Basis,
    read_and_write_Schlüsselworte_Basis, 
    read_and_write_classification_data
)
from Importer_SKU import (
    read_and_write_sku_classification_data,
    read_and_write_aku_data,
    read_and_write_SKU_Keyword
)

try:
    # Execute all the functions
    read_and_write_article_data()
    read_and_write_sku_classification_data()
    read_and_write_Zuordnung_Basis()
    read_and_write_Schlüsselworte_Basis()
    read_and_write_aku_data()
    read_and_write_SKU_Keyword()
    read_and_write_classification_data()
    print("All operations completed successfully!")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close database connections from both modules
    modules_to_close = [
        (Importer_SKU, "Importer_SKU"),
        (Importer_Article_Basis, "Importer_Article_Basis")
    ]
    
    for module, module_name in modules_to_close:
        try:
            if hasattr(module, 'conn') and module.conn:
                module.conn.close()
                print(f"{module_name} database connection closed.")
        except Exception as e:
            print(f"Error closing {module_name} connection: {e}")
