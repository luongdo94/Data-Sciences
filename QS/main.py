"""
Main script for exporting data from warehouse database to CSV.
"""

import logging
import sys
from warehouse_export import Config, DatabaseExporter, load_credential, load_query
from warehouse_export.services import fetch_article_numbers

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

def main():
    """Main function to execute the export process."""
    try:
        # Initialize configuration
        config = Config()
        config.validate()
        
        # Load credentials and query
        username, password = load_credential(config.credential_file)
        query = load_query(config.query_file)
        
        # Initialize and use the database exporter
        exporter = DatabaseExporter(config)
        try:
            exporter.connect(username, password)
            # 1. Original functionality: SKU_Carton_4_QS export
            logging.info("Processing SKU_Carton_4_QS...")
            df = exporter.execute_query(query)
            exporter.export_to_csv(df, config.output_file)
            
            # 2. New functionality: Article Numbers export
            logging.info("Processing Article Numbers...")
            df_articles = fetch_article_numbers(exporter)
            if df_articles is not None:
                exporter.export_to_csv(df_articles, config.article_numbers_output_file)
                
            logging.info("All exports completed successfully")
            return 0
        finally:
            exporter.disconnect()
            
    except Exception as e:
        logging.error(f"Export failed: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
