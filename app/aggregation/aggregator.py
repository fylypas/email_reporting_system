import pandas as pd
import numpy as np
from app.config.loader import settings
from app.transformation.risk_model import RiskModel


class Aggregator:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.cols = settings.app_config["columns"]

    def process(self) -> pd.DataFrame:
        # 1. Map Raw Risk Scores
        df_scored = RiskModel.map_row_scores(self.df)

        # 2. Determine Discount Burn (Logic: If Total exists use it, else sum parts)
        df_scored["__discount_burn"] = df_scored[self.cols["total_discount"]]

        # 3. Aggregate
        group_cols = [self.cols["platform_pivot"], self.cols["landing_page"]]

        agg_rules = {
            self.cols["grand_total"]: "sum",
            self.cols["store_credits"]: "sum",
            "__discount_burn": "sum",
            "__risk_score": "mean",  # Average risk score per campaign
        }

        report = df_scored.groupby(group_cols, as_index=False).agg(agg_rules)

        # 4. Rename columns for clean output
        report.rename(
            columns={
                self.cols["grand_total"]: "Total Sales",
                self.cols["store_credits"]: "Store Credit Used",
                "__discount_burn": "Discount Burn",
                "__risk_score": "Avg Risk Score",
            },
            inplace=True,
        )

        # 5. Compute RTO Deciles (1-10) on the aggregated dataset
        if not report.empty:
            try:
                report["RTO Decile"] = (
                    pd.qcut(
                        report["Avg Risk Score"], 10, labels=False, duplicates="drop"
                    )
                    + 1
                )
            except ValueError:
                # Fallback for small datasets
                report["RTO Decile"] = 1

        return report.sort_values(by="Total Sales", ascending=False)
