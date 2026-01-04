import yaml
from pathlib import Path
from typing import Dict, Any
from app.utils.logger import logger


class ConfigLoader:
    def __init__(self, config_dir: str = "app/config"):
        self.config_dir = Path(config_dir)
        self.app_config = self.load_yaml("config.yaml")
        self.platform_config = self.load_yaml("platforms.yaml")

    def load_yaml(self, filename: str) -> Dict[str, Any]:
        file_path = self.config_dir / filename
        try:
            with open(file_path, "r") as f:
                config = yaml.safe_load(f)
                logger.info(f"Successfully loaded config: {filename}")
                return config
        except FileNotFoundError:
            logger.critical(f"Configuration file missing: {file_path}")
            raise
        except yaml.YAMLError as e:
            logger.critical(f"Error parsing YAML {file_path}: {e}")
            raise

    _load_yaml = load_yaml


settings = ConfigLoader()
