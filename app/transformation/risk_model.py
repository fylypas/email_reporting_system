import pandas as pd
import numpy as np
from app.config.loader import settings
from app.utils.logger import logger


class RiskModel:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.risk_scores = settings.app_config["risk_scoring"]
        self.status_col = settings.app_config["columns"]["risk_status"]

    def _get_risk_score(self, status: str) -> int:
        status = str(status).lower()
        if "confirmed" in status:
            return self.risk_scores["confirmed"]
        if "pending" in status:
            return self.risk_scores["pending"]
        if "failed" in status or "cancelled" in status:
            return self.risk_scores["failed"]
        return self.risk_scores["default"]

    def compute_risk_metrics(self) -> pd.DataFrame:
        """Adds RTO Score and Deciles to the Campaign aggregation."""
        # This will be called AFTER aggregation, so df is grouped by Campaign
        # BUT the requirement is "Compute average risk score per campaign. Apply NTILE(10)"
        # So we need access to the raw data first to map scores, then aggregate.
        pass

    @staticmethod
    def map_row_scores(df: pd.DataFrame) -> pd.DataFrame:
        model = RiskModel(df)
        df["__risk_score"] = df[model.status_col].apply(model._get_risk_score)
        return df
