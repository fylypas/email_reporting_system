import pandas as pd
from app.utils.logger import logger
from app.config.loader import settings


class SchemaValidator:
    @staticmethod
    def validate(df: pd.DataFrame):
        required_cols = settings.app_config["columns"]["required"]
        missing = [col for col in required_cols if col not in df.columns]

        if missing:
            error_msg = f"Missing mandatory columns: {missing}"
            logger.critical(error_msg)
            raise ValueError(error_msg)

        logger.info("Schema validation passed.")
        return True
