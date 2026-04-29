#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from .logger import Logger
from .processing import ProcessingBase
from .store import Store


class BusinessManager(ProcessingBase):
    """Business worker: query object list, then fan-out object-level processing."""

    def __init__(
        self,
        cfg: dict[str, Any],
        store: Store,
        logger: Logger,
        **_: Any,
    ) -> None:
        self._root_cfg = cfg
        self._store = store

        section = (cfg.get("pipeline", {}).get("business", {}) or cfg.get("pipeline", {}).get("processing", {}))
        self._input_tables = [str(t) for t in (section.get("input_tables", []) or [])]
        self._input_limit = max(1, int(section.get("input_limit_per_table", 200)))
        self._output_table = str(section.get("output_table", "pipeline_business_results"))

        super().__init__(job_name="business", section_cfg=section, logger=logger)

    def fetch_items(self, _ctx: dict[str, Any]) -> Iterable[dict[str, Any]]:
        if not self._input_tables:
            return []

        out: list[dict[str, Any]] = []
        for table in self._input_tables:
            rows = self._store.query(
                f'SELECT * FROM "{table}" ORDER BY _rowid DESC LIMIT ?',
                params=(self._input_limit,),
                table=table,
            )
            for row in rows:
                out.append({"table": table, "row": row})
        return out

    def process_item(self, item: dict[str, Any], _ctx: dict[str, Any]) -> None:
        table = str(item.get("table", ""))
        row = dict(item.get("row") or {})

        if table:
            self._set_status(current_table=table)

        if not row:
            return

        row["processed_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row["processed_by"] = "business"
        self._store.upsert_many(self._output_table, [row])




# Backward-compatible symbol for existing tests/call sites.
ProcessingPipeline = BusinessManager
