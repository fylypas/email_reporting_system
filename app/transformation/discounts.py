import pandas as pd
from app.config.loader import settings


class DiscountCalculator:

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.cols = settings.app_config["columns"]

    def calculate_burn(self) -> pd.Series:
        if self.df.empty:
            return pd.Series(dtype=float)

        total_disc = self.df[self.cols["total_discount"]].fillna(0.0)

        granular_cols = [
            c for c in self.cols["granular_discounts"] if c in self.df.columns
        ]
        granular_sum = self.df[granular_cols].fillna(0.0).sum(axis=1)

        return total_disc.where(total_disc > 0, granular_sum)
