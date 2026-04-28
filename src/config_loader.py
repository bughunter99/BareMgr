#!/usr/bin/env python3
"""JSON/YAML 공통 설정 로더."""

from pathlib import Path
from typing import Any
import json


def load_config(path: str) -> dict[str, Any]:
    file_path = Path(path)
    suffix = file_path.suffix.lower()

    with file_path.open("r", encoding="utf-8") as f:
        if suffix in (".yaml", ".yml"):
            try:
                import yaml  # type: ignore[import]
            except ImportError as exc:
                raise RuntimeError(
                    "YAML config requires PyYAML. Install with: pip install pyyaml"
                ) from exc

            data = yaml.safe_load(f)
        else:
            data = json.load(f)

    if not isinstance(data, dict):
        raise ValueError(f"Config must be a mapping object: {path}")

    return data
