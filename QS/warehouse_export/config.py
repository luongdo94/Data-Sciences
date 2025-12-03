"""
Configuration settings for the warehouse export module.
"""

import os
from dataclasses import dataclass
from pathlib import Path

@dataclass
class Config:
    """Configuration class for database export settings."""
    credential_file: str = r"\\V-Processes\ERP\Credentials\fet_test_credentials.enc"
    server: str = 'PFDusSQL,50432'
    database: str = 'fet_test'
    query_file: str = r"\\V-Processes\ERP\ERP_Queries\SKU_Carton_4_QS.sql"
    output_file: str = r'\\pfduskommi\Promodoro.Export\QS\SKU_Carton_4_QS.csv'
    article_numbers_output_file: str = r'\\pfduskommi\Promodoro.Export\QS\SKU_EAN - Artikel-EAN.csv'

    @property
    def output_dir(self) -> str:
        """Get the directory of the output file."""
        return os.path.dirname(self.output_file)

    def validate(self) -> bool:
        """Validate the configuration."""
        if not os.path.exists(self.credential_file):
            raise FileNotFoundError(f"Credential file not found: {self.credential_file}")
        if not os.path.exists(self.query_file):
            raise FileNotFoundError(f"Query file not found: {self.query_file}")
        return True
