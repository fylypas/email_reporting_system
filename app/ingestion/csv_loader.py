import pandas as pd
from pathlib import Path
from app.utils.logger import logger
from app.config.loader import settings


class CsvLoader:
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)

    def load(self) -> pd.DataFrame:
        if not self.file_path.exists():
            logger.critical(f"Input file not found: {self.file_path}")
            raise FileNotFoundError(f"{self.file_path} does not exist.")

        try:
            logger.info(f"Loading data from {self.file_path}...")
            df = pd.read_csv(self.file_path)

            df.columns = df.columns.str.strip()

            numeric_cols = [
                settings.app_config["columns"]["grand_total"],
                settings.app_config["columns"]["store_credits"],
                settings.app_config["columns"]["total_discount"],
            ] + settings.app_config["columns"]["granular_discounts"]

            existing_numeric = [c for c in numeric_cols if c in df.columns]
            df[existing_numeric] = df[existing_numeric].fillna(0.0)

            str_cols = [
                settings.app_config["columns"]["platform_pivot"],
                settings.app_config["columns"]["landing_page"],
            ]
            df[str_cols] = df[str_cols].fillna("unknown")

            logger.info(f"Loaded {len(df)} rows successfully.")
            return df

        except Exception as e:
            logger.critical(f"Ingestion failed: {e}")
            raise
