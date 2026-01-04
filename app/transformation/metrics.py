import pandas as pd
from app.config.loader import settings


class MetricsCalculator:

    @staticmethod
    def calculate_total_sales(df: pd.DataFrame) -> float:
        col = settings.app_config["columns"]["grand_total"]
        return df[col].sum() if not df.empty else 0.0

    @staticmethod
    def calculate_store_credit_used(df: pd.DataFrame) -> float:
        col = settings.app_config["columns"]["store_credits"]
        return df[col].sum() if not df.empty else 0.0
