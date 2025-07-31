"""
Article service for business logic related to articles.

This module contains the business logic for article-related operations.
"""
from typing import List, Optional, Dict, Any
import pandas as pd
import os
from datetime import datetime
from ..repositories.article_repository import ArticleRepository
from ..utils.logger import setup_logger

class ArticleService:
    """Service for article-related business logic."""
    
    def __init__(self):
        """Initialize the article service."""
        self.repository = ArticleRepository()
        self.logger = setup_logger(self.__class__.__name__)
        
    def _save_to_csv(
        self, 
        df: pd.DataFrame, 
        filename: str, 
        output_dir: Optional[str] = None,
        encoding: str = 'utf-8-sig',
        sep: str = ';'
    ) -> str:
        """
        Save a DataFrame to a CSV file.
        
        Args:
            df (pd.DataFrame): The DataFrame to save.
            filename (str): The name of the output file.
            output_dir (str, optional): Directory to save the file in. Defaults to current directory.
            encoding (str, optional): File encoding. Defaults to 'utf-8-sig'.
            sep (str, optional): CSV separator. Defaults to ';'.
            
        Returns:
            str: The full path to the saved file.
        """
        try:
            # Create output directory if it doesn't exist
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            
            # Construct full file path
            file_path = os.path.join(output_dir, filename) if output_dir else filename
            
            # Save to CSV
            df.to_csv(file_path, index=False, encoding=encoding, sep=sep)
            self.logger.info(f"Data saved to {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {e}", exc_info=True)
            raise
    
    def export_article_data(
        self, 
        aid_list: Optional[List[str]] = None,
        output_dir: Optional[str] = None,
        add_timestamp: bool = True
    ) -> str:
        """
        Export article data to a CSV file.
        
        Args:
            aid_list (List[str], optional): List of article IDs to export. If None, exports all.
            output_dir (str, optional): Directory to save the output file.
            add_timestamp (bool, optional): Whether to add a timestamp to the filename.
            
        Returns:
            str: Path to the generated file.
        """
        try:
            self.logger.info(f"Exporting article data for {len(aid_list) if aid_list else 'all'} articles")
            
            # Get data from repository
            df = self.repository.get_articles(aid_list)
            
            if df.empty:
                self.logger.warning("No data to export")
                return ""
            
            # Add default columns
            df['company'] = 0
            df['automatic_batch_numbering_pattern'] = '{No,000000000}'
            df['batch_management'] = 2
            df['batch_number_range'] = 'Chargen'
            df['batch_numbering_type'] = 3
            df['date_requirement'] = 1
            df['discountable'] = 'ja'
            df['factory'] = 'Düsseldorf'
            df['isPi'] = 'ja'
            df['isShopArticle'] = 'ja'
            df['isSl'] = 'ja'
            df['isSt'] = 'ja'
            df['isVerifiedArticle'] = 'ja'
            df['isCatalogArticle'] = 'ja'
            df['unitPi'] = 'Stk'
            df['unitSl'] = 'Stk'
            df['unitSt'] = 'Stk'
            df['replacement_time'] = 1
            df['taxPi'] = 'Waren'
            df['taxSl'] = 'Waren'
            df['valid_from'] = datetime.now().strftime("%Y%m%d")
            
            # Process country of origin
            df['country_of_origin'] = df['Ursprungsland'].str[:2]
            
            # Define column order
            columns = [
                'aid', 'company', 'country_of_origin', 'automatic_batch_numbering_pattern',
                'batch_management', 'batch_number_range', 'batch_numbering_type', 'date_requirement',
                'discountable', 'factory', 'isPi', 'isShopArticle', 'isSl', 'isSt', 'isVerifiedArticle',
                'isCatalogArticle', 'unitPi', 'unitSl', 'unitSt', 'name', 'replacement_time', 'taxPi',
                'taxSl', 'valid_from'
            ]
            
            # Reorder columns
            df = df[[col for col in columns if col in df.columns]]
            
            # Generate filename
            filename = "IMPORTER_ARTICLE_Neuanlage_Basis"
            if aid_list:
                filename += "_notinERP"
            if add_timestamp:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename += f"_{timestamp}"
            filename += ".csv"
            
            # Save to CSV
            return self._save_to_csv(
                df=df,
                filename=filename,
                output_dir=output_dir,
                encoding='utf-8-sig',
                sep=';'
            )
            
        except Exception as e:
            self.logger.error(f"Error exporting article data: {e}", exc_info=True)
            raise
    
    def export_article_classification_data(
        self,
        aid_list: Optional[List[str]] = None,
        output_dir: Optional[str] = None,
        add_timestamp: bool = True
    ) -> str:
        """
        Export article classification data to a CSV file.
        
        Args:
            aid_list (List[str], optional): List of article IDs to export. If None, exports all.
            output_dir (str, optional): Directory to save the output file.
            add_timestamp (bool, optional): Whether to add a timestamp to the filename.
            
        Returns:
            str: Path to the generated file.
        """
        try:
            self.logger.info(f"Exporting article classification data for {len(aid_list) if aid_list else 'all'} articles")
            
            # Get data from repository
            df = self.repository.get_article_classification_data(aid_list)
            
            if df.empty:
                self.logger.warning("No data to export")
                return ""
            
            # Add default columns
            df['company'] = 0
            df['classification_system'] = 'Warengruppensystem'
            df['product_group_superior'] = df['Marke'] + '||Produktlinie||ROOT'
            
            # Process Grammatur to extract numbers
            df['Grammatur'] = df['Grammatur'].astype(str).str.extract(r'(\d+)')
            
            # Transform Gender column: 'Kinder' -> ''
            df['Gender'] = df['Gender'].replace('Kinder', '')
            
            # Define features and their corresponding values
            features = [
                ('Grammatur', 'Grammatur'),
                ('Oeko_MadeInGreen', ''),
                ('Partnerlook', lambda x: x['Artikel_Partner'].str[:4] if 'Artikel_Partner' in x.columns else ''),
                ('Sortierung', 'ArtSort'),
                ('Fabric_Herstellung', 'Materialart'),
                ('Material', 'Zusammensetzung'),
                ('Workwear', lambda x: x['workwear'].abs() if 'workwear' in x.columns else 0),
                ('Produktlinie_Veredelung', lambda x: x['veredelung'].abs() if 'veredelung' in x.columns else 0),
                ('Produktlinie_Veredelungsart_Discharge', lambda x: x['discharge'].abs() if 'discharge' in x.columns else 0),
                ('Produktlinie_Veredelungsart_DTG', lambda x: x['dtg'].abs() if 'dtg' in x.columns else 0),
                ('Produktlinie_Veredelungsart_DYOJ', lambda x: x['dyoj'].abs() if 'dyoj' in x.columns else 0),
                ('Produktlinie_Veredelungsart_DYOP', lambda x: x['dyop'].abs() if 'dyop' in x.columns else 0),
                ('Produktlinie_Veredelungsart_Flock', lambda x: x['flock'].abs() if 'flock' in x.columns else 0),
                ('Produktlinie_Veredelungsart_Siebdruck', lambda x: x['siebdruck'].abs() if 'siebdruck' in x.columns else 0),
                ('Produktlinie_Veredelungsart_Stick', lambda x: x['stick'].abs() if 'stick' in x.columns else 0),
                ('Produktlinie_Veredelungsart_Sublimationsdruck', 
                 lambda x: x['sublimation'].abs() if 'sublimation' in x.columns else 0),
                ('Produktlinie_Veredelungsart_Transferdruck', 
                 lambda x: x['transfer'].abs() if 'transfer' in x.columns else 0),
                ('Brand_Premium_Item', lambda x: x['premium'].abs() if 'premium' in x.columns else 0),
                ('Extras', lambda x: x['extras'].abs() if 'extras' in x.columns else 0),
                ('Kids', lambda x: 1 - x['erw'].abs() if 'erw' in x.columns else 1),
                ('Outdoor', lambda x: x['outdoor'].abs() if 'outdoor' in x.columns else 0),
                ('Size_Oversize', lambda x: x['oversize'].abs() if 'oversize' in x.columns else 0),
                ('Geschlecht', 'Gender'),
                ('Brand_Label', lambda x: x['label'].abs() if 'label' in x.columns else 0)
            ]
            
            # Create a new DataFrame with the required structure
            result_rows = []
            
            for _, row in df.iterrows():
                base_data = {
                    'aid': row.get('aid', ''),
                    'company': 0,
                    'classification_system': 'Warengruppensystem',
                    'product_group': row.get('product_group', ''),
                    'product_group_superior': row.get('Marke', '') + '||Produktlinie||ROOT'
                }
                
                # Add features
                for i, (feature_name, feature_value) in enumerate(features):
                    base_data[f'feature[{i}]'] = feature_name
                    
                    # Calculate the feature value
                    if callable(feature_value):
                        # For lambda functions that need the entire row
                        value = feature_value(row)
                    elif feature_value == '':
                        # Empty string value
                        value = ''
                    elif feature_value in row:
                        # Direct column reference
                        value = row[feature_value]
                    else:
                        # Default to empty string if column not found
                        value = ''
                        
                    base_data[f'feature_value[{i}]'] = value
                
                result_rows.append(base_data)
            
            # Create the result DataFrame
            result_df = pd.DataFrame(result_rows)
            
            # Generate filename
            filename = "IMPORTER_ARTICLE_CLASSIFICATION_Merkmale_Basis"
            if aid_list:
                filename += "_notinERP"
            if add_timestamp:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename += f"_{timestamp}"
            filename += ".csv"
            
            # Save to CSV with Windows-1252 encoding as per original code
            return self._save_to_csv(
                df=result_df,
                filename=filename,
                output_dir=output_dir,
                encoding='windows-1252',
                sep=';'
            )
            
        except Exception as e:
            self.logger.error(f"Error exporting article classification data: {e}", exc_info=True)
            raise
