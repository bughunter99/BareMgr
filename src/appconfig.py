#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config_loader import load_config
from .db_registry import build_registry


@dataclass(frozen=True)
class RuntimeContext:
    config_file: str
    loaded_at_utc: str


class AppConfig:
    def __init__(self, raw: dict[str, Any], *, config_file: str = "") -> None:
        self._raw = raw
        self.runtime = RuntimeContext(
            config_file=str(config_file or "").strip(),
            loaded_at_utc=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        )

    @classmethod
    def from_file(cls, config_file: str) -> "AppConfig":
        raw = load_config(config_file)
        return cls(raw, config_file=config_file)

    @property
    def raw(self) -> dict[str, Any]:
        return self._raw

    @property
    def node_id(self) -> str:
        return str(self._raw.get("node_id", "")).strip()

    @property
    def logging_cfg(self) -> dict[str, Any]:
        return dict(self._raw.get("logging", {}) or {})

    @property
    def oracle_cfg(self) -> dict[str, Any]:
        return dict(self._raw.get("oracle", {}) or {})

    @property
    def collectors_cfg(self) -> dict[str, Any]:
        return dict(self._raw.get("collectors", {}) or {})

    @property
    def db_registry(self) -> dict[str, dict]:
        return build_registry(self._raw)

    def get(self, key: str, default: Any = None) -> Any:
        return self._raw.get(key, default)

    def section(self, *keys: str, default: Any = None) -> Any:
        current: Any = self._raw
        for key in keys:
            if not isinstance(current, dict):
                return default
            current = current.get(key)
        return default if current is None else current

    @property
    def config_file_name(self) -> str:
        if not self.runtime.config_file:
            return ""
        return Path(self.runtime.config_file).name
