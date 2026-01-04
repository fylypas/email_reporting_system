import pandas as pd
from typing import Dict
from app.config.loader import settings
from app.utils.logger import logger


class PlatformSplitter:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.platforms_config = settings.platform_config["platforms"]
        self.pivot_col = settings.app_config["columns"]["platform_pivot"]

    def _identify_platform(self, referrer: str) -> str:
        referrer = str(referrer).lower()

        # Meta
        for keyword in self.platforms_config["meta"]["keywords"]:
            if keyword in referrer:
                return "meta"

        # Google
        for keyword in self.platforms_config["google"]["keywords"]:
            if keyword in referrer:
                return "google"

        return "others"

    def split(self) -> Dict[str, pd.DataFrame]:
        logger.info("Segmenting data by platform...")

        self.df["__platform_key"] = self.df[self.pivot_col].apply(
            self._identify_platform
        )

        segments = {}
        for key in self.platforms_config.keys():
            segment_df = self.df[self.df["__platform_key"] == key].copy()
            if not segment_df.empty:
                segments[key] = segment_df
                logger.info(f"Platform '{key}': {len(segment_df)} rows identified.")

        return segments
