import io
import os
import re
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Union, Any
from datetime import timedelta

# Add project root to Python path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from src.database import get_sql_server_connection as get_connection, execute_query, read_sql_query
from src.article_importer_class import ArticleImporter

# Define OUTPUT_DIR
OUTPUT_DIR = project_root / 'data' / 'output'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Set up console encoding
# if sys.stdout.encoding != 'utf-8':
#     sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
# if sys.stderr.encoding != 'utf-8':
#     sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
def extract_numbers(value):
    #Extract numbers from a string
    if pd.isna(value):
        return ''
    import re
    numbers = re.findall(r'\d+', str(value))
    return ''.join(numbers) if numbers else ''

def update_sku():
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter()
    return importer.update_sku()

def import_sku_basis(diff=None):
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter(diff=diff)
    return importer.import_sku_basis()

def import_sku_classification(diff=None):
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter(diff=diff)
    return importer.import_sku_classification()
def import_sku_keyword(diff=None):
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter(diff=diff)
    return importer.import_sku_keyword()
def import_artikel_basis(diff1=None):
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter(diff1=diff1)
    return importer.import_artikel_basis()



def import_artikel_classification(diff1=None):
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter(diff1=diff1)
    return importer.import_artikel_classification()

def import_artikel_zuordnung(diff1=None):
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter(diff1=diff1)
    return importer.import_artikel_zuordnung()

def import_artikel_keyword(diff1=None):
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter(diff1=diff1)
    return importer.import_artikel_keyword()

def import_artikel_text(diff1=None):
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter(diff1=diff1)
    return importer.import_artikel_text()

def import_artikel_text_en(diff1=None):
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter(diff1=diff1)
    return importer.import_artikel_text_en()

import re

def import_sku_text():
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter()
    return importer.import_sku_text()

def import_sku_text_en():
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter()
    return importer.import_sku_text_en()

def import_artikel_variant():
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter()
    return importer.import_artikel_variant()
    
def import_sku_variant():
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter()
    return importer.import_sku_variant()

def import_artikel_pricestaffeln():
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter()
    return importer.import_artikel_pricestaffeln()

def import_artikel_preisstufe_3_7():
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter()
    return importer.import_artikel_preisstufe_3_7()
        
def import_artikel_basicprice(basicprice_filename="PRICELIST - Artikel-Basispreis.csv", validity_filename="Basispreis_validity_data.csv"):
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter()
    return importer.import_artikel_basicprice()

def import_sku_EAN():
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter()
    return importer.import_sku_ean()

# Removed duplicate import_sku_basis function

def import_sku_gebinde():
    """Refactored to use ArticleImporter class."""
    importer = ArticleImporter()
    return importer.import_sku_gebinde()
            
# -------------------------------------------------------------------------
# ADAPTER FUNCTIONS: Wrapper for new Class-based Importer (OrderImporter)
# -------------------------------------------------------------------------

from src.order_importer_class import OrderImporter

def import_order():
    """Refactored to use OrderImporter class."""
    importer = OrderImporter()
    return importer.import_order()

def import_order_are_15():
    """Refactored to use OrderImporter class."""
    importer = OrderImporter()
    return importer.import_order_are_15()

def import_order_pos():
    """Refactored to use OrderImporter class."""
    importer = OrderImporter()
    return importer.import_order_pos()

def import_order_pos_are_15():
    """Refactored to use OrderImporter class."""
    importer = OrderImporter()
    return importer.import_order_pos_are_15()

def import_order_classification():
    """Refactored to use OrderImporter class."""
    importer = OrderImporter()
    return importer.import_order_classification()
      

# -------------------------------------------------------------------------
# ADAPTER FUNCTIONS: Wrapper for new Class-based Importer (StockImporter)
# -------------------------------------------------------------------------

from src.stock_importer_class import StockImporter

def import_stock_lager(diff_areas=None):
    """Refactored to use StockImporter class."""
    importer = StockImporter(diff_areas)
    return importer.import_stock_lager()

    


# -------------------------------------------------------------------------
# ADAPTER FUNCTIONS: Wrapper for new Class-based Importer (BusinessPartnerImporter)
# These functions maintain backward compatibility with existing code/scripts.
# -------------------------------------------------------------------------

from src.bp_importer_class import BusinessPartnerImporter

def import_business_partner(diff_partner_ids=None):
    """Refactored to use BusinessPartnerImporter class."""
    importer = BusinessPartnerImporter(diff_partner_ids)
    return importer.import_business_customer()

def import_business_customer_accounting(diff_partner_ids=None):
    """Refactored to use BusinessPartnerImporter class."""
    importer = BusinessPartnerImporter(diff_partner_ids)
    return importer.import_business_customer_accounting()

def import_business_supplier(diff_partner_ids=None):
    """Refactored to use BusinessPartnerImporter class."""
    importer = BusinessPartnerImporter(diff_partner_ids)
    return importer.import_business_supplier()

def import_BP_Communication(diff_partner_ids=None):
    """Refactored to use BusinessPartnerImporter class."""
    importer = BusinessPartnerImporter(diff_partner_ids)
    # The class method returns a list of paths, while the original function returned the directory.
    # We maintain the original return behavior (directory path) for compatibility.
    importer.import_customer_communication()
    return OUTPUT_DIR

def import_BP_Adress(diff_partner_ids=None):
    """Refactored to use BusinessPartnerImporter class."""
    importer = BusinessPartnerImporter(diff_partner_ids)
    return importer.import_customer_address()

def import_BP_Contact(diff_partner_ids=None):
    """Refactored to use BusinessPartnerImporter class."""
    importer = BusinessPartnerImporter(diff_partner_ids)
    return importer.import_customer_contact()

def import_BP_Keyword(diff_partner_ids=None):
    """Refactored to use BusinessPartnerImporter class."""
    importer = BusinessPartnerImporter(diff_partner_ids)
    return importer.import_customer_keyword()

def import_BP_Contact_Communication(diff_partner_ids=None):
    """Wrapper for contact-specific communication import."""
    importer = BusinessPartnerImporter(diff_partner_ids)
    return importer.import_customer_contact_communication()

def import_BP_Contact_Role(diff_partner_ids=None):
    """Wrapper for contact-specific employee role import."""
    importer = BusinessPartnerImporter(diff_partner_ids)
    return importer.import_customer_employee_role()

def import_Supplier_Communication():
    """Wrapper for new supplier communication import."""
    importer = BusinessPartnerImporter()
    return importer.import_supplier_communication()

def import_Supplier_Address():
    """Wrapper for new supplier address import."""
    importer = BusinessPartnerImporter()
    return importer.import_supplier_address()


# This allows the script to be run directly
# This allows the script to be run directly
if __name__ == "__main__":
    from src.bp_importer_class import BusinessPartnerImporter
    from src.article_importer_class import ArticleImporter
    
    print("Starting business partner import...")
    
    # Use class-based importer directly
    bp_importer = BusinessPartnerImporter()
    
    bp_importer.import_business_customer()
    bp_importer.import_business_supplier()
    bp_importer.import_customer_communication()
    bp_importer.import_customer_address()
    bp_importer.import_customer_contact()
    bp_importer.import_customer_keyword()
    bp_importer.import_supplier_communication()
    bp_importer.import_supplier_address()
    
    print("Starting article import...")
    # Initialize implementation with diffs (if any)
    # For now, we'll use None as diffs to import everything or handle within the class
    art_importer = ArticleImporter()
    
    # SKU Level
    art_importer.update_sku()
    art_importer.import_sku_basis()
    art_importer.import_sku_classification()
    art_importer.import_sku_keyword()
    art_importer.import_sku_text()
    art_importer.import_sku_text_en()
    art_importer.import_sku_variant()
    art_importer.import_sku_ean()
    art_importer.import_sku_gebinde()
    
    # Article Level
    art_importer.import_artikel_basis()
    art_importer.import_artikel_classification()
    art_importer.import_artikel_zuordnung()
    art_importer.import_artikel_keyword()
    art_importer.import_artikel_text()
    art_importer.import_artikel_text_en()
    art_importer.import_artikel_variant()
    
    # Pricing
    art_importer.import_artikel_pricestaffeln()
    art_importer.import_artikel_preisstufe_3_7()
    art_importer.import_artikel_basicprice()

    print("Import completed.")
