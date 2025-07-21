
# Import functions from other modules
from Importer_Article_Basis import (
    read_and_write_article_data,
    read_and_write_Zuordnung_Basis,
    read_and_write_Schlüsselworte_Basis,
    read_and_write_classification_data,
    read_and_write_classification_data
)
from Importer_SKU import (
    read_and_write_sku_classification_data,
    read_and_write_aku_data,
    read_and_write_SKU_Keyword,
    read_and_write_SKU_EAN,
    link_sku_article_basis
)


try:
    read_and_write_article_data()
    read_and_write_sku_classification_data()
    read_and_write_Zuordnung_Basis()
    read_and_write_Schlüsselworte_Basis()
    read_and_write_aku_data()
    read_and_write_SKU_Keyword()
    read_and_write_SKU_EAN()
    link_sku_article_basis()
    print("All operations completed successfully!")
except Exception as e:
    print(f"An error occurred: {e}")

