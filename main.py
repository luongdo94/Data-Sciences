
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
    print("All operations completed successfully!")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close database connections from both modules
    try:
        if hasattr(Importer_SKU, 'conn') and Importer_SKU.conn:
            Importer_SKU.conn.close()
            print("Importer_SKU database connection closed.")
    except Exception as e:
        print(f"Error closing Importer_SKU connection: {e}")
    
    try:
        if hasattr(Importer_Article_Basis, 'conn') and Importer_Article_Basis.conn:
            Importer_Article_Basis.conn.close()
            print("Importer_Article_Basis database connection closed.")
    except Exception as e:
        print(f"Error closing Importer_Article_Basis connection: {e}")
