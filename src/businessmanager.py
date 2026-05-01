#!/usr/bin/env python3
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable

from .queryaaa import QueryAAA
from .querybbb import QueryBBB
from .queryccc import QueryCCC
from .queryddd import QueryDDD
from .queryeee import QueryEEE
from .db_registry import build_registry, resolve_dsn
from .logger import Logger
from .oracleconnectionmanager import OracleConnectionManager
from .processing import ProcessingBase
from .store import Store


class BusinessManager(ProcessingBase):
    """Business worker: query object list, then fan-out object-level processing."""

    def __init__(
        self,
        cfg: dict[str, Any],
        store: Store,
        logger: Logger,
        connection_manager: OracleConnectionManager | None = None,
        **_: Any,
    ) -> None:
        self._root_cfg = cfg
        self._store = store
        self._connection_manager = connection_manager

        section = cfg.get("pipeline", {}).get("business", {})
        self._section = section
        self._input_tables = [str(t) for t in (section.get("input_tables", []) or [])]
        self._input_limit = max(1, int(section.get("input_limit_per_table", 200)))
        self._output_table = str(section.get("output_table", "pipeline_business_results"))
        self._db_registry = build_registry(cfg)
        self._oracle_cfg = section.get("oracle", {}) or {}
        self._oracle_db_alias = str(self._oracle_cfg.get("db", "") or self._oracle_cfg.get("source_db", "")).strip()
        self._oracle_dsn = self._resolve_oracle_dsn(self._oracle_cfg)
        _targets = (self._oracle_cfg.get("targets", {}) or {})
        self._table_aaa = str(_targets.get("AAA", "pipeline_business_aaa"))
        self._table_bbb = str(_targets.get("BBB", "pipeline_business_bbb"))
        self._table_ccc = str(_targets.get("CCC", "pipeline_business_ccc"))
        self._table_ddd = str(_targets.get("DDD", "pipeline_business_ddd"))
        self._table_eee = str(_targets.get("EEE", "pipeline_business_eee"))
        self._aaa = QueryAAA(self._oracle_cfg.get("AAA") or {})
        self._bbb = QueryBBB(self._oracle_cfg.get("BBB") or {})
        self._ccc = QueryCCC(self._oracle_cfg.get("CCC") or {})
        self._ddd = QueryDDD(self._oracle_cfg.get("DDD") or {})
        self._eee = QueryEEE(self._oracle_cfg.get("EEE") or {})
        self._warned_missing_conn_manager = False

        super().__init__(job_name="business", section_cfg=section, logger=logger)

    def _resolve_oracle_dsn(self, oracle_cfg: dict[str, Any]) -> str:
        alias = str(oracle_cfg.get("db", "") or oracle_cfg.get("source_db", "")).strip()
        if alias:
            return resolve_dsn(self._db_registry, alias)
        return ""

    def _resolve_obj_id(self, row: dict[str, Any]) -> str:
        for key in ("obj_id", "id", "object_id", "pk"):
            value = row.get(key)
            if value is not None and str(value).strip():
                return str(value).strip()
        return ""

    def _enrich(
        self,
        rows: list[dict[str, Any]],
        source_table: str,
        obj_id: str,
        processed_at: str,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for r in rows:
            item = dict(r)
            item.setdefault("source_table", source_table)
            item.setdefault("obj_id", obj_id)
            item.setdefault("processed_at", processed_at)
            out.append(item)
        return out

    def _run_oracle_groups(self, *, source_table: str, row: dict[str, Any], processed_at: str) -> None:
        if not self._oracle_db_alias:
            return
        if self._connection_manager is None:
            if not self._warned_missing_conn_manager:
                self._logger.warning("[BusinessManager] oracle db alias is configured but connection_manager is missing")
                self._warned_missing_conn_manager = True
            return
        if not self._oracle_dsn:
            if not self._warned_missing_conn_manager:
                self._logger.warning("[BusinessManager] unresolved oracle db alias=%s", self._oracle_db_alias)
                self._warned_missing_conn_manager = True
            return

        obj_id = self._resolve_obj_id(row)
        def run_groups(cursor) -> None:
            # AAA domain
            aaa_rows = self._aaa.fetch_profile(cursor, obj_id) + self._aaa.fetch_detail(cursor, obj_id)
            if aaa_rows:
                self._store.upsert_many(self._table_aaa, self._enrich(aaa_rows, source_table, obj_id, processed_at))

            # BBB domain
            bbb_rows = self._bbb.fetch_status(cursor, obj_id) + self._bbb.fetch_flags(cursor, obj_id)
            if bbb_rows:
                self._store.upsert_many(self._table_bbb, self._enrich(bbb_rows, source_table, obj_id, processed_at))

            # CCC domain
            ccc_rows = self._ccc.fetch_metrics(cursor, obj_id) + self._ccc.fetch_aggregates(cursor, obj_id)
            if ccc_rows:
                self._store.upsert_many(self._table_ccc, self._enrich(ccc_rows, source_table, obj_id, processed_at))

            # DDD domain
            ddd_rows = self._ddd.fetch_rules(cursor, obj_id) + self._ddd.fetch_violations(cursor, obj_id)
            if ddd_rows:
                self._store.upsert_many(self._table_ddd, self._enrich(ddd_rows, source_table, obj_id, processed_at))

            # EEE domain
            eee_rows = self._eee.fetch_events(cursor, obj_id) + self._eee.fetch_history(cursor, obj_id)
            if eee_rows:
                self._store.upsert_many(self._table_eee, self._enrich(eee_rows, source_table, obj_id, processed_at))

        if self._oracle_db_alias:
            with self._connection_manager.cursor_by_alias(
                self._oracle_db_alias,
            ) as cursor:
                run_groups(cursor)
            return

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

        processed_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row["processed_at"] = processed_at
        row["processed_by"] = "business"
        self._store.upsert_many(self._output_table, [row])
        self._run_oracle_groups(source_table=table, row=row, processed_at=processed_at)




# Backward-compatible symbol for existing tests/call sites.
ProcessingPipeline = BusinessManager
