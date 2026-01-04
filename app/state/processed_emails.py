import json
import os
from pathlib import Path
from typing import Set
from app.utils.logger import logger


class ProcessedStateManager:
    def __init__(self, state_file: str):
        self.file_path = Path(state_file)
        self.processed_ids: Set[str] = self._load_state()

    def _load_state(self) -> Set[str]:
        if not self.file_path.exists():
            return set()
        try:
            with open(self.file_path, "r") as f:
                data = json.load(f)
                return set(data.get("message_ids", []))
        except Exception as e:
            logger.error(f"Failed to load state file: {e}")
            return set()

    def is_processed(self, message_id: str) -> bool:
        return message_id in self.processed_ids

    def mark_processed(self, message_id: str):
        self.processed_ids.add(message_id)
        self._save_state()

    def _save_state(self):
        try:
            self.file_path.parent.mkdir(parents=True, exist_ok=True)

            temp_path = self.file_path.with_suffix(".tmp")
            with open(temp_path, "w") as f:
                json.dump({"message_ids": list(self.processed_ids)}, f)

            # Atomic move
            os.replace(temp_path, self.file_path)
        except Exception as e:
            logger.critical(f"Failed to save state: {e}")
