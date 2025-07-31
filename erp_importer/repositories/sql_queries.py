"""
SQL query definitions for the ERP Importer.

This module contains all SQL queries used by the application,
organized by functionality.
"""

# Article-related queries
class ArticleQueries:
    """SQL queries for article data operations."""
    
    @staticmethod
    def get_base_articles(filter_aids: bool = False) -> str:
        """
        Get the base query for article data.
        
        Args:
            filter_aids (bool): Whether to include the AID filter placeholder.
            
        Returns:
            str: SQL query string.
        """
        query = """
            SELECT 
                m.ArtBasis as aid, 
                m.Ursprungsland, 
                t.ArtBem as name,
                m.Produktgruppe as product_group,
                m.Marke as brand,
                m.Grammatur as grammage,
                m.Artikel_Partner as partner_article,
                m.ArtSort as sort_order,
                m.Materialart as material_type,
                m.Zusammensetzung as composition,
                m.Gender as gender
            FROM 
                t_Art_MegaBase m 
                INNER JOIN t_Art_Text_DE t ON m.ArtNr = t.ArtNr 
            WHERE 
                m.Marke IN ('Corporate', 'EXCD', 'XO')
        """
        if filter_aids:
            query += " AND m.ArtBasis IN ('{}')"
            
        return query
    
    @staticmethod
    def get_article_flags() -> str:
        """Get the query for article flags."""
        return """
            SELECT 
                f.ArtNr,
                f.flag_workwear as workwear,
                f.flag_veredelung as processing,
                f.flag_discharge as discharge,
                f.flag_dtg as dtg,
                f.flag_dyoj as dyoj,
                f.flag_dyop as dyop,
                f.flag_flock as flock,
                f.flag_siebdruck as screen_printing,
                f.flag_stick as embroidery,
                f.flag_sublimation as sublimation,
                f.flag_transfer as transfer,
                f.flag_premium as premium,
                f.flag_extras as extras,
                f.flag_outdoor as outdoor,
                f.flag_plussize as plus_size,
                f.isNoLabel as no_label,
                f.isErw as is_adult
            FROM 
                t_Art_Flags f
        """

# SKU-related queries
class SkuQueries:
    """SQL queries for SKU data operations."""
    
    @staticmethod
    def get_sku_base(filter_aids: bool = False) -> str:
        """
        Get the base query for SKU data.
        
        Args:
            filter_aids (bool): Whether to include the AID filter placeholder.
            
        Returns:
            str: SQL query string.
        """
        query = """
            SELECT 
                sku.ArtikelCode AS aid,
                m.ArtBasis AS base_article_id,
                m.ArtNr AS article_number,
                m.Ursprungsland as country_of_origin,
                t.ArtBem AS name,
                sku.Größe AS size,
                sku.Größenspiegel AS size_range,
                sku.Hauptfarbe AS main_color_group,
                sku.FarbeNeu AS color,
                sku.isColorCombination AS is_color_combo,
                sku.Karton_Länge as package_length,
                sku.Karton_Breite as package_width,
                sku.Karton_Höhe as package_height,
                sku.Produktgewicht as product_weight,
                sku.WarenNr as article_number_alt,
                sku.isColorMelange as is_color_melange,
                sku.VZTA_gültig_bis as valid_until,
                sku.VZTA_gültig_von as valid_from_date
            FROM 
                t_Art_Mega_SKU sku
                INNER JOIN t_Art_Text_DE t ON sku.ArtNr = t.ArtNr
                INNER JOIN t_Art_MegaBase m ON sku.ArtNr = m.ArtNr
            WHERE 
                m.Marke IN ('Corporate', 'EXCD', 'XO')
        """
        if filter_aids:
            query += " AND sku.ArtikelCode IN ('{}')"
            
        return query
