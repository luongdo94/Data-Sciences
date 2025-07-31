"""
Article repository for database operations related to articles.

This module provides methods to interact with article data in the database.
"""
from typing import List, Optional, Dict, Any
import pandas as pd
from .base_repository import BaseRepository
from .sql_queries import ArticleQueries

class ArticleRepository(BaseRepository):
    """Repository for article-related database operations."""
    
    def __init__(self):
        """Initialize the article repository."""
        super().__init__("ArticleRepository")
    
    def get_articles(self, aid_list: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Retrieve articles from the database.
        
        Args:
            aid_list (List[str], optional): List of article IDs to filter by.
            
        Returns:
            pd.DataFrame: DataFrame containing article data.
        """
        try:
            query = ArticleQueries.get_base_articles(filter_aids=bool(aid_list))
            
            if aid_list:
                # Format the query with the list of AIDs
                aid_list_str = "','".join(str(aid) for aid in aid_list)
                query = query.format(aid_list_str)
            
            self.logger.info(f"Executing query for {len(aid_list) if aid_list else 'all'} articles")
            return self._execute_query(query)
            
        except Exception as e:
            self.logger.error(f"Error retrieving articles: {e}", exc_info=True)
            raise
    
    def get_article_flags(self) -> pd.DataFrame:
        """
        Retrieve article flags from the database.
        
        Returns:
            pd.DataFrame: DataFrame containing article flag data.
        """
        try:
            query = ArticleQueries.get_article_flags()
            self.logger.info("Retrieving article flags")
            return self._execute_query(query)
        except Exception as e:
            self.logger.error(f"Error retrieving article flags: {e}", exc_info=True)
            raise
    
    def get_article_classification_data(self, aid_list: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Retrieve article classification data.
        
        Args:
            aid_list (List[str], optional): List of article IDs to filter by.
            
        Returns:
            pd.DataFrame: DataFrame containing article classification data.
        """
        try:
            # Get base article data
            articles_df = self.get_articles(aid_list)
            
            # Get article flags
            flags_df = self.get_article_flags()
            
            # Merge the data
            if not articles_df.empty and not flags_df.empty:
                # Ensure we have a common column to merge on (ArtNr)
                if 'ArtNr' not in articles_df.columns and 'base_article_id' in articles_df.columns:
                    articles_df = articles_df.rename(columns={'base_article_id': 'ArtNr'})
                
                if 'ArtNr' in articles_df.columns and 'ArtNr' in flags_df.columns:
                    return pd.merge(articles_df, flags_df, on='ArtNr', how='left')
            
            return articles_df
            
        except Exception as e:
            self.logger.error(f"Error retrieving article classification data: {e}", exc_info=True)
            raise
