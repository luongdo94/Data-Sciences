"""
ERP Importer - Main Application

This is the main entry point for the ERP Importer application.
"""
import os
import sys
import pandas as pd
from typing import List, Optional
from datetime import datetime

# Add the parent directory to the path so we can import our package
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from erp_importer.services.article_service import ArticleService
from erp_importer.utils.logger import setup_logger

class ERPImporter:
    """Main application class for ERP Importer."""
    
    def __init__(self):
        """Initialize the ERP Importer application."""
        self.logger = setup_logger('ERPImporter')
        self.article_service = ArticleService()
        
    def load_aid_list(self, csv_path: str) -> List[str]:
        """
        Load a list of AIDs from a CSV file.
        
        Args:
            csv_path (str): Path to the CSV file containing AIDs.
            
        Returns:
            List[str]: List of AIDs.
        """
        try:
            self.logger.info(f"Loading AIDs from {csv_path}")
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            
            if df.empty:
                self.logger.warning("CSV file is empty")
                return []
                
            # Get the first column as AIDs
            aid_list = df.iloc[:, 0].astype(str).tolist()
            self.logger.info(f"Loaded {len(aid_list)} AIDs from {csv_path}")
            return aid_list
            
        except Exception as e:
            self.logger.error(f"Error loading AIDs from {csv_path}: {e}", exc_info=True)
            raise
    
    def export_article_data(self, aid_list: Optional[List[str]] = None, output_dir: Optional[str] = None):
        """
        Export article data.
        
        Args:
            aid_list (List[str], optional): List of AIDs to export. If None, exports all.
            output_dir (str, optional): Directory to save output files.
        """
        try:
            self.logger.info("Starting article data export...")
            
            # Create output directory if it doesn't exist
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            
            # Export article data
            self.article_service.port_article_data(
                aid_list=aid_list,
                output_dir=output_dir,
                add_timestamp=True
            )
            
            # Export classification data
            self.article_service.export_article_classification_data(
                aid_list=aid_list,
                output_dir=output_dir,
                add_timestamp=True
            )
            
            self.logger.info("Article data export completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error during article data export: {e}", exc_info=True)
            raise
    
    def run(self, filter_csv: Optional[str] = None, output_dir: Optional[str] = None):
        """
        Run the ERP Importer.
        
        Args:
            filter_csv (str, optional): Path to CSV file with AIDs to filter by.
            output_dir (str, optional): Directory to save output files.
        """
        try:
            self.logger.info("Starting ERP Importer")
            
            # Load AIDs from CSV if provided
            aid_list = None
            if filter_csv and os.path.exists(filter_csv):
                aid_list = self.load_aid_list(filter_csv)
                if not aid_list:
                    self.logger.warning("No AIDs loaded from filter CSV, exporting all data")
            
            # Default output directory to current directory if not specified
            if not output_dir:
                output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'output')
            
            # Export data
            self.export_article_data(aid_list=aid_list, output_dir=output_dir)
            
            self.logger.info("ERP Importer completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error in ERP Importer: {e}", exc_info=True)
            sys.exit(1)


def main():
    """Main entry point for the application."""
    import argparse
    
    # Set up argument parser
    parser = argparse.ArgumentParser(description='ERP Importer - Import and process data from ERP systems.')
    parser.add_argument('--filter-csv', type=str, 
                       default=r"C:\Users\gia.luongdo\Desktop\aid_not_in_AID_results.csv",
                       help='Path to CSV file containing AIDs to filter by')
    parser.add_argument('--output-dir', type=str, 
                       help='Directory to save output files (default: ./output)')
    
    # Parse command line arguments
    args = parser.parse_args()
    
    # Create and run the application
    app = ERPImporter()
    app.run(filter_csv=args.filter_csv, output_dir=args.output_dir)


if __name__ == "__main__":
    main()
