#!/usr/bin/env python3
"""
db_registry.py — 공유 Oracle 커넥션 레지스트리.

config의 oracle_connections 목록을 alias 기반 딕셔너리로 구축하고,
각 컴포넌트가 alias만으로 DSN 문자열 또는 pool 설정을 얻을 수 있도록 한다.

config 구조 예시:
        oracle_connections:
      - alias: DB_MAIN
        tns: 192.168.1.100:1521/ORCL
        user: user
        password: password
        pool:
          enabled: false
          min: 2
          max: 10
          increment: 1
          threaded: true
          getmode: wait
      - alias: DB_TARGET
        tns: 192.168.1.101:1521/ORCL
        user: user
        password: password
        pool:
          enabled: false
          min: 1
          max: 5
          increment: 1
          threaded: true
          getmode: wait
"""

from __future__ import annotations

from typing import Any


def build_registry(cfg: dict[str, Any]) -> dict[str, dict]:
    """oracle_connections 목록을 alias → entry 딕셔너리로 변환한다."""
    entries = cfg.get("oracle_connections", []) or cfg.get("db_connections", []) or []
    return {
        str(e["alias"]).strip(): e
        for e in entries
        if isinstance(e, dict) and "alias" in e
    }


def resolve_dsn(registry: dict[str, dict], alias: str, fallback: str = "") -> str:
    """alias → 'user/password@tns' DSN 문자열 반환. 없으면 fallback."""
    if not alias:
        return fallback
    entry = registry.get(str(alias).strip())
    if not entry:
        return fallback
    user = str(entry.get("user", "")).strip()
    password = str(entry.get("password", "")).strip()
    tns = str(entry.get("tns", "")).strip()
    if user and password and tns:
        return f"{user}/{password}@{tns}"
    return tns or fallback


def resolve_pool_cfg(registry: dict[str, dict], alias: str) -> dict[str, Any]:
    """alias → pool 설정 딕셔너리 (user/password/dsn 자동 채움). 없으면 {}."""
    if not alias:
        return {}
    entry = registry.get(str(alias).strip())
    if not entry:
        return {}
    pool: dict[str, Any] = dict(entry.get("pool", {}) or {})
    pool.setdefault("user", str(entry.get("user", "")).strip())
    pool.setdefault("password", str(entry.get("password", "")).strip())
    pool.setdefault("dsn", str(entry.get("tns", "")).strip())
    return pool
