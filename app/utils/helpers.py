import os
from pathlib import Path


def ensure_directories(paths: list):
    for path in paths:
        Path(path).mkdir(parents=True, exist_ok=True)


def get_project_root() -> Path:
    return Path(__file__).parent.parent.parent
