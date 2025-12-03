"""
Business logic services for warehouse export.
"""
import logging
import pandas as pd
from datetime import datetime
from typing import Optional
from .database import DatabaseExporter

logger = logging.getLogger(__name__)

def fetch_article_numbers(exporter: DatabaseExporter) -> Optional[pd.DataFrame]:
    """
    Fetch and process article numbers using the provided exporter.
    
    Args:
        exporter: Configured DatabaseExporter instance
        
    Returns:
        pd.DataFrame: Processed DataFrame with article numbers or None if failed
    """
    try:
        # SQL query to join article proxy and article number tables
        sql_query = """
        SELECT distinct a.aid, n.NUMSTRING, u.NAME
        FROM fet_user.FET_ARTICLE_PROXY a 
        INNER JOIN fet_user.FET_ARTICLE_NUMBER n ON a.GUID=n.ARTICLE_PROXY_GUID
        INNER JOIN fet_user.V_UNIT u on n.UNIT_GUID=u.UNIT_GUID
        WHERE a.aid not like '%obsolet' and u.NAME not in('Unit')
        """
        
        df = exporter.execute_query(sql_query)
        
        if df is not None and not df.empty:
            # Rename NUMSTRING to EAN
            df.rename(columns={'NUMSTRING': 'EAN', 'NAME': 'unitname'}, inplace=True)
            
            # Map unitname to unit
            unit_mapping = {
                'Single Packed': 'SP',
                'Single Piece': 'SPC',
                'Stück': 'Stk',
                '5er Pack': '5er',
                '10er Pack': '10er'
            }
            df['unit'] = df['unitname'].map(unit_mapping).fillna(df['unitname'])

            # Add requested columns
            df['company'] = 0
            df['numbertype'] = 2
            df['valid_from'] = datetime.now().strftime('%Y%m%d')
            df['valid_to'] = ''
            df['purpose'] = 1
            
            logger.info(f"Successfully retrieved {len(df)} article records")
            return df
        else:
            logger.warning("No article data retrieved")
            return None
            
    except Exception as e:
        logger.error(f"Error retrieving article numbers: {str(e)}")
        return None
