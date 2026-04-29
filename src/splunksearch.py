#!/usr/bin/env python3
from __future__ import annotations

import json
from typing import Any
from urllib import parse as urlparse
from urllib import request as urlrequest


BASE_URL = ""
CREDENTIAL_KEY = ""
SPLUNK_TOKEN = ""
TIMEOUT_SEC = 60.0


def configure_splunk_search(
    *,
    base_url: str,
    credential_key: str,
    splunk_token: str,
    timeout_sec: float = 60.0,
) -> None:
    global BASE_URL, CREDENTIAL_KEY, SPLUNK_TOKEN, TIMEOUT_SEC

    BASE_URL = str(base_url).strip()
    CREDENTIAL_KEY = str(credential_key).strip()
    SPLUNK_TOKEN = str(splunk_token).strip()
    TIMEOUT_SEC = max(1.0, float(timeout_sec))


def _normalize_query(query: str) -> str:
    normalized = str(query).strip()
    if not normalized:
        raise ValueError("Splunk query is required")
    if normalized.lower().startswith("search "):
        return normalized
    return f"search {normalized}"


def SplunkSearch(query: str) -> list[dict[str, Any]]:
    if not BASE_URL:
        raise ValueError("SplunkSearch BASE_URL is not configured")
    if not SPLUNK_TOKEN:
        raise ValueError("SplunkSearch SPLUNK_TOKEN is not configured")

    endpoint = f"{BASE_URL.rstrip('/')}/services/search/jobs/export"
    payload = urlparse.urlencode(
        {
            "search": _normalize_query(query),
            "output_mode": "json",
            "exec_mode": "oneshot",
        }
    ).encode("utf-8")

    headers = {
        "Authorization": f"Splunk {SPLUNK_TOKEN}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    if CREDENTIAL_KEY:
        headers["X-Credential-Key"] = CREDENTIAL_KEY

    req = urlrequest.Request(endpoint, data=payload, headers=headers, method="POST")
    rows: list[dict[str, Any]] = []
    with urlrequest.urlopen(req, timeout=TIMEOUT_SEC) as resp:
        for raw_line in resp:
            line = raw_line.decode("utf-8").strip()
            if not line:
                continue
            parsed = json.loads(line)
            if isinstance(parsed, dict) and isinstance(parsed.get("result"), dict):
                rows.append(parsed["result"])
            elif isinstance(parsed, dict):
                rows.append(parsed)
    return rows