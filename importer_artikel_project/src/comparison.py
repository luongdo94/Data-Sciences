from pathlib import Path
import logging
from typing import Set, Tuple
from datetime import datetime
import pandas as pd
from .database import read_csv_file

def compare_columns(
    file_path: Path,
    col1: str,
    col2: str,
    output_dir: Path,
    output_filename: str = None, 
    delimiter: str = ',',
    encoding: str = 'windows-1252'
) -> Tuple[Set[str], Path]:

    logger = logging.getLogger(__name__)
    
    try:
        # Read CSV file using database function
        df = read_csv_file(
            file_path=file_path,
            delimiter=delimiter,
            encoding=encoding,
            required_columns=[col1, col2],
            dtype={col1: str, col2: str}
        )
            
        # Process data and convert to lowercase
        col1_values = df[col1].dropna().astype(str).str.strip()
        col2_values = df[col2].dropna().astype(str).str.strip()
        
        # Find differences (case insensitive comparison)
        diff = set(col1_values) - set(col2_values)
        
        # Create output directory if not exists
        output_dir.mkdir(parents=True, exist_ok=True)

        # Define output file path
        if output_filename:
            output_file = output_dir / output_filename
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = output_dir / f"{col1}_not_in_{col2}_{timestamp}.csv"

        # Save results if any differences found
        if diff:
            result_df = pd.DataFrame({
                f'{col1}_not_in_{col2}': sorted(diff)
            })
            result_df.to_csv(output_file, index=False, encoding=encoding, sep=delimiter)
            logger.info(f'Results saved to: {output_file}')
        else:
            logger.info('No differences found')
        
        return diff, output_file
        
    except Exception as e:
        logger.error(f'Error comparing columns: {e}')
        raise
