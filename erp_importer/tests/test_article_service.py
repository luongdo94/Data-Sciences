"""
Tests for the ArticleService class.
"""
import os
import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from pathlib import Path

from erp_importer.services.article_service import ArticleService

# Sample data for testing
SAMPLE_ARTICLE_DATA = pd.DataFrame({
    'aid': ['A001', 'A002'],
    'Ursprungsland': ['DE', 'FR'],
    'name': ['Test Article 1', 'Test Article 2']
})

SAMPLE_CLASSIFICATION_DATA = pd.DataFrame({
    'aid': ['A001', 'A002'],
    'Marke': ['Brand1', 'Brand2'],
    'Grammatur': ['180', '200'],
    'Gender': ['M', 'Kinder'],
    'ArtSort': ['1', '2'],
    'Materialart': ['Cotton', 'Polyester'],
    'Zusammensetzung': ['100% Cotton', '100% Polyester'],
    'workwear': [1, 0],
    'veredelung': [0, 1],
    'discharge': [0, 0],
    'dtg': [0, 0],
    'dyoj': [0, 0],
    'dyop': [0, 0],
    'flock': [0, 0],
    'siebdruck': [0, 0],
    'stick': [0, 0],
    'sublimation': [0, 0],
    'transfer': [0, 0],
    'premium': [0, 0],
    'extras': [0, 0],
    'erw': [1, 0],
    'outdoor': [0, 0],
    'oversize': [0, 0],
    'label': [0, 0],
    'Artikel_Partner': ['', '']
})


class TestArticleService:
    """Test cases for ArticleService."""
    
    @pytest.fixture
    def article_service(self):
        """Fixture to create an ArticleService instance for testing."""
        return ArticleService()
    
    @patch('erp_importer.repositories.article_repository.ArticleRepository.get_articles')
    def test_export_article_data(self, mock_get_articles, article_service, tmp_path):
        """Test exporting article data to CSV."""
        # Arrange
        mock_get_articles.return_value = SAMPLE_ARTICLE_DATA
        output_file = tmp_path / "test_output"
        output_file.mkdir()
        
        # Act
        result = article_service.export_article_data(
            aid_list=['A001', 'A002'],
            output_dir=str(output_file),
            add_timestamp=False
        )
        
        # Assert
        assert result is not None
        assert os.path.exists(result)
        assert "IMPORTER_ARTICLE_Neuanlage_Basis_notinERP.csv" in result
        
        # Verify the content
        df = pd.read_csv(result, sep=';', encoding='utf-8-sig')
        assert not df.empty
        assert 'aid' in df.columns
        assert 'name' in df.columns
        assert 'country_of_origin' in df.columns
    
    @patch('erp_importer.repositories.article_repository.ArticleRepository.get_article_classification_data')
    def test_export_article_classification_data(self, mock_get_classification, article_service, tmp_path):
        """Test exporting article classification data to CSV."""
        # Arrange
        mock_get_classification.return_value = SAMPLE_CLASSIFICATION_DATA
        output_file = tmp_path / "test_output"
        output_file.mkdir()
        
        # Act
        result = article_service.export_article_classification_data(
            aid_list=['A001', 'A002'],
            output_dir=str(output_path),
            add_timestamp=False
        )
        
        # Assert
        assert result is not None
        assert os.path.exists(result)
        assert "IMPORTER_ARTICLE_CLASSIFICATION_Merkmale_Basis_notinERP.csv" in result
        
        # Verify the content
        df = pd.read_csv(result, sep=';', encoding='windows-1252')
        assert not df.empty
        assert 'aid' in df.columns
        assert 'product_group_superior' in df.columns
        
        # Verify Gender transformation (Kinder -> '')
        assert df[df['aid'] == 'A002']['Geschlecht'].iloc[0] == ''
    
    def test_save_to_csv(self, article_service, tmp_path):
        """Test saving DataFrame to CSV."""
        # Arrange
        df = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        output_file = tmp_path / "test_output"
        output_file.mkdir()
        
        # Act
        result = article_service._save_to_csv(
            df=df,
            filename="test.csv",
            output_dir=str(output_file)
        )
        
        # Assert
        assert result is not None
        assert os.path.exists(result)
        
        # Verify the content
        df_read = pd.read_csv(result, sep=';', encoding='utf-8-sig')
        assert not df_read.empty
        assert 'col1' in df_read.columns
        assert 'col2' in df_read.columns
