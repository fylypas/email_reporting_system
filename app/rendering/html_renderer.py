import pandas as pd
from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from typing import Dict, Any, List
from app.utils.logger import logger


class HtmlRenderer:
    def __init__(self):
        template_dir = Path(__file__).parent / "templates"
        self.env = Environment(loader=FileSystemLoader(str(template_dir)))

    def render_stakeholder_report(
        self,
        stakeholder_name: str,
        kpis: Dict[str, Any],
        summary_df: pd.DataFrame,
        breakdown_df: pd.DataFrame = None,
    ) -> str:
        display_summary = self._condense_dataframe(summary_df, top_n=4)

        display_breakdown = None
        if breakdown_df is not None:
            display_breakdown = self._condense_dataframe(breakdown_df, top_n=10)

        logger.info(
            f"Rendering HTML for {stakeholder_name}. Reduced summary from {len(summary_df)} to {len(display_summary)} rows."
        )

        try:
            template = self.env.get_template("report.html")

            context = {
                "title": f"Daily Performance Report for {stakeholder_name}",
                "kpis": kpis,
                "has_breakdown": display_breakdown is not None,
                "summary_table": self._df_to_html_dicts(display_summary),
                "summary_columns": display_summary.columns.tolist(),
            }

            if display_breakdown is not None:
                context["breakdown_table"] = self._df_to_html_dicts(display_breakdown)
                context["breakdown_columns"] = display_breakdown.columns.tolist()

            return template.render(**context)

        except Exception as e:
            logger.error(f"Rendering failed: {e}")
            raise

    def _condense_dataframe(self, df: pd.DataFrame, top_n: int = 15) -> pd.DataFrame:
        if df.empty or len(df) <= top_n:
            return df

        if "Total Sales" in df.columns:
            df = df.sort_values(by="Total Sales", ascending=False)

        top_df = df.iloc[:top_n].copy()
        bottom_df = df.iloc[top_n:].copy()

        others_row = {}
        for col in df.columns:
            if df[col].dtype.kind in "bifc":
                # Numeric types
                if "Risk" in col or "Score" in col:
                    others_row[col] = bottom_df[col].mean()
                else:
                    others_row[col] = bottom_df[col].sum()
            else:
                # String columns
                others_row[col] = "Others"

        others_df = pd.DataFrame([others_row])

        return pd.concat([top_df, others_df], ignore_index=True)

    def _df_to_html_dicts(self, df: pd.DataFrame) -> List[Dict]:
        fmt_df = df.copy()

        cols_to_fmt = ["Total Sales", "Store Credit Used", "Discount Burn"]
        for col in cols_to_fmt:
            if col in fmt_df.columns:
                fmt_df[col] = fmt_df[col].apply(
                    lambda x: f"₹{x:,.2f}" if isinstance(x, (int, float)) else x
                )

        # Format Risk Score
        if "Avg Risk Score" in fmt_df.columns:
            fmt_df["Avg Risk Score"] = fmt_df["Avg Risk Score"].apply(
                lambda x: f"{x:.2f}" if isinstance(x, (int, float)) else x
            )

        return fmt_df.to_dict(orient="records")
