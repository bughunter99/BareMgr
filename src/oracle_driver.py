#!/usr/bin/env python3
from __future__ import annotations

import threading
from typing import Any


_INIT_LOCK = threading.Lock()
_INIT_DONE = False


def get_cx_oracle():
    try:
        import oracledb as cx_Oracle  # type: ignore[import-not-found]

        return cx_Oracle
    except ImportError:
        import cx_Oracle  # type: ignore[import-not-found]

        return cx_Oracle


def init_oracle_client_from_config(cfg: dict[str, Any], logger=None) -> bool:
    global _INIT_DONE

    with _INIT_LOCK:
        if _INIT_DONE:
            return False

        cx_Oracle = get_cx_oracle()
        init_fn = getattr(cx_Oracle, "init_oracle_client", None)
        if init_fn is None:
            _INIT_DONE = True
            return False

        oracle_cfg = cfg.get("oracle_client", {}) or {}
        kwargs: dict[str, Any] = {}
        for key in ("lib_dir", "config_dir", "driver_name"):
            value = str(oracle_cfg.get(key, "")).strip()
            if value:
                kwargs[key] = value

        try:
            init_fn(**kwargs)
            _INIT_DONE = True
            if logger is not None:
                logger.info("[Oracle] init_oracle_client completed kwargs=%s", sorted(kwargs.keys()))
            return True
        except Exception as exc:
            message = str(exc).lower()
            if "already been initialized" in message:
                _INIT_DONE = True
                return False
            if logger is not None:
                logger.warning("[Oracle] init_oracle_client skipped: %s", exc)
            return False