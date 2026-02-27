
import pandas as pd
from pathlib import Path
from src.database import execute_query, save_fetcsv
from src.config import OUTPUT_DIR, SQL_DIR

class StockImporter:
    """
    Importer class for handling Stock/Lager data.
    Based on the logic from import_stock_lager in simple_article_importer.py.
    """

    def __init__(self, diff_areas=None):
        self.output_dir = OUTPUT_DIR
        self.sql_dir = SQL_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.diff_areas = self._resolve_diff_areas(diff_areas)

    def _resolve_diff_areas(self, areas):
        """Internal helper to load diff areas if not provided"""
        if areas is None:
            try:
                from run_comparison_standalone import diff_areas
                return list(diff_areas) if isinstance(diff_areas, set) else diff_areas
            except (ImportError, AttributeError):
                return None
        return list(areas) if isinstance(areas, set) else areas

    def _load_query(self, filename):
        """Helper to safely load SQL query from file"""
        sql_path = self.sql_dir / filename
        if not sql_path.exists():
            print(f"Warning: SQL file not found at {sql_path}")
            return None
        return sql_path.read_text(encoding='utf-8')

    def _save_csv(self, df, filename, data_type="STOCK"):
        """Standardized CSV export with FETCSV header"""
        if df is not None and not df.empty:
            out_path = self.output_dir / filename
            save_fetcsv(df, out_path, data_type)
            print(f"Exported {len(df)} records to: {out_path}")
            return out_path
        return None

    def import_stock_lager(self):
        """Import stock data and generate 3 output files."""
        query_template = self._load_query('get_lager.sql')
        if not query_template:
            return None, None, None

        # Prepare parameters
        params = ['Kommissionierungslager', -1]
        
        # Add area filter logic
        # Note: Filtering happens in Python to avoid SQL parameter limits with large lists
        sql_query = query_template.format(diff_areas_filter="")
        
        # Execute query
        df = pd.DataFrame(execute_query(sql_query, params))
        if df.empty:
            return None, None, None

        # Format area column
        df['area'] = df.apply(
            lambda row: f"{row['Reihe']}-{row['Regal']}-{int(row['Palette']):04d}",
            axis=1
        )

        # Apply diff_areas filter in Python
        if self.diff_areas and len(self.diff_areas) > 0:
            print(f"Filtering {len(self.diff_areas)} areas")
            df['area_lower'] = df['area'].str.lower()
            valid_areas_lower = {str(area).lower() for area in self.diff_areas}
            df = df[df['area_lower'].isin(valid_areas_lower)]
            df = df.drop(columns=['area_lower'])
        
        if df.empty:
            return None, None, None

        # Add constant columns
        df['company'] = 0
        df['factory'] = 'Düsseldorf'
        df['refilPoint'] = 25
        df['refilPointIsPercent'] = -1
        df['refilQuantity'] = 0
        df['unit'] = 'Stk'
        df['is_priority_area'] = 1
        df['isPriorityArea'] = 1 
        df['storage_area_type'] = 'PICKING' 

        # 1. Main Stock File
        main_columns = ['location', 'factory', 'area', 'is_priority_area']
        main_output_columns = [col for col in main_columns if col in df.columns]
        
        file1 = self._save_csv(df[main_output_columns], "STOCK - Lager.csv")

        # 2. Priority Area File
        priority_cols = ['aid', 'area', 'company', 'factory', 'location']
        # Selecting and reordering
        priority_df = df[priority_cols].copy()
        file2 = self._save_csv(priority_df, "STOCKARTICLE_PRIORITY_AREA - Prioritätsplätze.csv")

        # 3. Location Definition File
        loc_def_cols = [
            'aid', 'area', 'company', 'factory', 'location', 
            'quantity', 'refilPoint', 'refilPointIsPercent', 'unit'
        ]
        loc_def_df = df[loc_def_cols].copy()
        file3 = self._save_csv(loc_def_df, "Stockarticle_LocDef-Stellplatzdefinitionen.csv")

        return file1, file2, file3
