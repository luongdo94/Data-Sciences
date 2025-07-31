
# Import functions from other modules
import logging
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'export_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)

logger = logging.getLogger(__name__)

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
from Importer_SKU_not_in_erp import (
    read_and_write_sku_data,
    read_and_write_sku_classification_data_not_in_erp,
    read_and_write_sku_keyword_not_in_erp
)
from Importer_Article_not_in_erp import (
    read_and_write_article_data_not_in_erp,
    read_and_write_classification_data_not_in_erp,
    read_and_write_Zuordnung_Basis_not_in_erp,
    read_and_write_Schlüsselworte_Basis_not_in_erp
)
import Importer_Article_not_in_erp

try:
    logger.info("Starting data export...")
    
    # Execute all the functions for ALL data
    logger.info("Exporting main data...")
    read_and_write_article_data()
    read_and_write_sku_classification_data()
    read_and_write_Zuordnung_Basis()
    read_and_write_Schlüsselworte_Basis()
    read_and_write_aku_data()
    read_and_write_SKU_Keyword()
    read_and_write_classification_data()
    
    # Execute functions for NOT IN ERP data
    logger.info("Exporting NOT IN ERP data...")
    # SKU related exports
    read_and_write_sku_data()
    read_and_write_sku_classification_data_not_in_erp()
    read_and_write_sku_keyword_not_in_erp()
    
    # Article related exports
    read_and_write_article_data_not_in_erp()
    read_and_write_classification_data_not_in_erp()
    read_and_write_Zuordnung_Basis_not_in_erp()
    read_and_write_Schlüsselworte_Basis_not_in_erp()
    
    logger.info("All data has been exported successfully!")

except Exception as e:
    logger.error(f"An error occurred during export: {e}", exc_info=True)
    sys.exit(1)

finally:
    # Close database connections from both modules
    logger.info("Closing database connections...")
    modules_to_close = [
        (Importer_SKU, "Importer_SKU"),
        (Importer_Article_Basis, "Importer_Article_Basis"),
        (Importer_Article_not_in_erp, "Importer_Article_not_in_erp")
    ]
    
    for module, module_name in modules_to_close:
        try:
            if hasattr(module, 'conn') and module.conn:
                module.conn.close()
                print(f"{module_name} database connection closed.")
        except Exception as e:
            print(f"Error closing {module_name} connection: {e}")
