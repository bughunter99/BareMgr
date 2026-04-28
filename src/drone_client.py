#!/usr/bin/env python3
from __future__ import annotations

import json
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest

from .logger import Logger


class DroneClient:
    def __init__(self, cfg: dict[str, Any], logger: Logger) -> None:
        self._logger = logger
        self.enabled = bool(cfg.get("enabled", False))
        self._dry_run = bool(cfg.get("dry_run", True))
        self._url = str(cfg.get("url", "")).strip()
        self._timeout_sec = float(cfg.get("timeout_sec", 5.0))
        self._auth_token = str(cfg.get("auth_token", "")).strip()
        self._extra_headers = cfg.get("headers", {}) or {}

    def send_many(self, results: list[dict[str, Any]]) -> int:
        if not self.enabled:
            return 0
        if self._dry_run:
            self._logger.info("[Processing] drone dry-run send count=%d", len(results))
            return len(results)
        if not self._url:
            self._logger.warning("[Processing] drone enabled but url missing")
            return 0

        sent = 0
        for item in results:
            body = json.dumps(item, ensure_ascii=False, default=str).encode("utf-8")
            headers = {
                "Content-Type": "application/json",
            }
            headers.update({str(k): str(v) for k, v in self._extra_headers.items()})
            if self._auth_token:
                headers["Authorization"] = f"Bearer {self._auth_token}"

            req = urlrequest.Request(self._url, data=body, headers=headers, method="POST")
            try:
                with urlrequest.urlopen(req, timeout=self._timeout_sec) as resp:
                    status = getattr(resp, "status", 200)
                    if 200 <= int(status) < 300:
                        sent += 1
                    else:
                        self._logger.warning(
                            "[Processing] drone send failed obj_id=%s status=%s",
                            item.get("obj_id"),
                            status,
                        )
            except (urlerror.URLError, TimeoutError):
                self._logger.warning(
                    "[Processing] drone send error obj_id=%s",
                    item.get("obj_id"),
                )
        return sent
