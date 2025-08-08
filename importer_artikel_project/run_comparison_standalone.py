from pathlib import Path
from src.comparison import compare_columns
from src.config import DATA_DIR, OUTPUT_DIR

# Define paths
INPUT_FILE = DATA_DIR / "comparison.csv"
INPUT_FILE1 = DATA_DIR / "comparison_artbasis.csv"
COMPARISON_OUTPUT_DIR = OUTPUT_DIR / "comparison_results"

# Ensure output directory exists
COMPARISON_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Run comparison
try:
    #compare SKU
    diff, result_file = compare_columns(
        file_path=INPUT_FILE,
        col1="aid_ew",
        col2="aid_erp",
        output_dir=COMPARISON_OUTPUT_DIR,
        output_filename="sku_differences.csv",
        delimiter=',',
        encoding='windows-1252'
    )
    print(f"Found {len(diff)} differences")
    print(f"Results saved to: {result_file}")
    #compare ArtBasis
    diff1, result_file1 = compare_columns(
        file_path=INPUT_FILE1,
        col1="aid_ew",
        col2="aid_erp",
        output_dir=COMPARISON_OUTPUT_DIR,
        output_filename="artbasis_differences.csv",
        delimiter=',',
        encoding='windows-1252'
    )
    print(f"Found {len(diff1)} differences")
    print(f"Results saved to: {result_file1}")
except Exception as e:
    print(f"Error: {e}")
    raise
