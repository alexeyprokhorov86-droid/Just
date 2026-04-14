"""
KM chunker — km_facts, km_decisions, km_tasks, km_policies → distilled chunks.
Этап 3 плана. Потоковая обработка через server-side cursor.
"""
import logging
from typing import List, Optional

from chunkers.base_chunker import BaseChunker, Chunk

logger = logging.getLogger(__name__)

FETCH_SIZE = 500

# ---------------------------------------------------------------------------
# SQL queries with entity name JOINs
# ---------------------------------------------------------------------------

SQL_FACTS = """
    SELECT f.id, f.fact_type, f.fact_text, f.confidence, f.created_at,
           subj.canonical_name AS subject_name
    FROM km_facts f
    LEFT JOIN km_entities subj ON f.subject_entity_id = subj.id
    WHERE f.verification_status NOT IN ('rejected', 'duplicate')
      {where_extra}
    ORDER BY f.id
"""

SQL_DECISIONS = """
    SELECT d.id, d.decision_text, d.scope_type, d.importance,
           d.confidence, d.decided_at, d.created_at,
           by_.canonical_name AS decided_by_name
    FROM km_decisions d
    LEFT JOIN km_entities by_ ON d.decided_by_entity_id = by_.id
    WHERE d.verification_status NOT IN ('rejected', 'duplicate')
      {where_extra}
    ORDER BY d.id
"""

SQL_TASKS = """
    SELECT t.id, t.task_text, t.deadline, t.status,
           t.confidence, t.created_at,
           a.canonical_name AS assignee_name
    FROM km_tasks t
    LEFT JOIN km_entities a ON t.assignee_entity_id = a.id
    WHERE t.verification_status NOT IN ('rejected', 'duplicate')
      {where_extra}
    ORDER BY t.id
"""

SQL_POLICIES = """
    SELECT p.id, p.policy_text, p.scope_type, p.status,
           p.confidence, p.created_at
    FROM km_policies p
    WHERE p.verification_status NOT IN ('rejected', 'duplicate')
      {where_extra}
    ORDER BY p.id
"""

# Table config: (sql, columns, source_kind, formatter)
KM_TABLES = [
    ("km_facts", SQL_FACTS,
     ["id", "fact_type", "fact_text", "confidence", "created_at", "subject_name"],
     "km_fact"),
    ("km_decisions", SQL_DECISIONS,
     ["id", "decision_text", "scope_type", "importance", "confidence",
      "decided_at", "created_at", "decided_by_name"],
     "km_decision"),
    ("km_tasks", SQL_TASKS,
     ["id", "task_text", "deadline", "status", "confidence",
      "created_at", "assignee_name"],
     "km_task"),
    ("km_policies", SQL_POLICIES,
     ["id", "policy_text", "scope_type", "status", "confidence", "created_at"],
     "km_policy"),
]


class KMChunker(BaseChunker):

    def __init__(self, dry_run: bool = False, batch_limit: int = 0):
        super().__init__(dry_run=dry_run)
        self.batch_limit = batch_limit

    def generate_chunks(self, full: bool = False) -> List[Chunk]:
        total = 0
        for table_name, sql, columns, source_kind in KM_TABLES:
            count = self._stream_table(table_name, sql, columns, source_kind, full)
            total += count
        logger.info(f"KMChunker done: {total} chunks total")
        return []

    def _stream_table(self, table_name: str, sql: str, columns: List[str],
                      source_kind: str, full: bool) -> int:
        logger.info(f"KMChunker: processing {table_name}")

        alias = sql.split("FROM")[1].strip().split()[1]  # e.g. 'f' from 'km_facts f'

        if full:
            where_extra = ""
        else:
            where_extra = (
                f"AND {alias}.id::text NOT IN "
                f"(SELECT parent_1c_ref FROM source_chunks "
                f"WHERE source_kind = '{source_kind}' AND parent_1c_ref IS NOT NULL)"
            )

        query = sql.format(where_extra=where_extra)
        if self.batch_limit > 0:
            query += f"\n LIMIT {self.batch_limit}"

        cursor_name = f"km_{source_kind.replace('km_', '')}_cursor"
        cur = self.conn.cursor(cursor_name)
        cur.itersize = FETCH_SIZE

        formatter = getattr(self, f"_format_{source_kind}")
        total = 0

        try:
            cur.execute(query)
            while True:
                rows = cur.fetchmany(FETCH_SIZE)
                if not rows:
                    break

                chunks = []
                for row in rows:
                    rec = dict(zip(columns, row))
                    chunk = formatter(rec, source_kind)
                    if chunk:
                        chunks.append(chunk)

                if chunks:
                    saved = self.save_chunks(chunks)
                    total += saved

                logger.info(
                    f"  {source_kind}: batch {len(rows)} → "
                    f"{len(chunks)} chunks (total: {total})"
                )
        finally:
            cur.close()

        return total

    # ------------------------------------------------------------------
    # Formatters
    # ------------------------------------------------------------------
    @staticmethod
    def _format_km_fact(rec: dict, source_kind: str) -> Optional[Chunk]:
        text = (rec.get("fact_text") or "").strip()
        if not text:
            return None
        fact_type = rec.get("fact_type") or "общий"
        subject = rec.get("subject_name") or ""
        conf = rec.get("confidence") or 0.5
        date_str = str(rec["created_at"].date()) if rec.get("created_at") else None

        subject_line = f"\nСубъект: {subject} | Достоверность: {conf:.2f}" if subject else ""
        chunk_text = f"Факт [{fact_type}]: {text}{subject_line}"

        return Chunk(
            chunk_text=chunk_text,
            chunk_type="distilled",
            source_kind=source_kind,
            parent_1c_ref=str(rec["id"]),
            chunk_date=date_str,
            confidence=float(conf),
            token_count=max(1, len(chunk_text) // 3),
        )

    @staticmethod
    def _format_km_decision(rec: dict, source_kind: str) -> Optional[Chunk]:
        text = (rec.get("decision_text") or "").strip()
        if not text:
            return None
        scope = rec.get("scope_type") or "общий"
        decided_by = rec.get("decided_by_name") or ""
        importance = rec.get("importance") or 0
        conf = rec.get("confidence") or 0.5
        decided_at = rec.get("decided_at")
        date_str = str(decided_at) if decided_at else (
            str(rec["created_at"].date()) if rec.get("created_at") else None
        )

        by_line = f"\nПринято: {decided_by} | Важность: {importance:.1f}" if decided_by else ""
        chunk_text = f"Решение [{scope}]: {text}{by_line}"

        return Chunk(
            chunk_text=chunk_text,
            chunk_type="distilled",
            source_kind=source_kind,
            parent_1c_ref=str(rec["id"]),
            chunk_date=date_str,
            confidence=float(conf),
            token_count=max(1, len(chunk_text) // 3),
        )

    @staticmethod
    def _format_km_task(rec: dict, source_kind: str) -> Optional[Chunk]:
        text = (rec.get("task_text") or "").strip()
        if not text:
            return None
        assignee = rec.get("assignee_name") or ""
        deadline = rec.get("deadline")
        status = rec.get("status") or ""
        conf = rec.get("confidence") or 0.5
        date_str = str(rec["created_at"].date()) if rec.get("created_at") else None

        meta_parts = []
        if assignee:
            meta_parts.append(f"Исполнитель: {assignee}")
        if deadline:
            meta_parts.append(f"Дедлайн: {deadline}")
        if status:
            meta_parts.append(f"Статус: {status}")
        meta_line = "\n" + " | ".join(meta_parts) if meta_parts else ""

        chunk_text = f"Задача: {text}{meta_line}"

        return Chunk(
            chunk_text=chunk_text,
            chunk_type="distilled",
            source_kind=source_kind,
            parent_1c_ref=str(rec["id"]),
            chunk_date=date_str,
            confidence=float(conf),
            token_count=max(1, len(chunk_text) // 3),
        )

    @staticmethod
    def _format_km_policy(rec: dict, source_kind: str) -> Optional[Chunk]:
        text = (rec.get("policy_text") or "").strip()
        if not text:
            return None
        scope = rec.get("scope_type") or "общий"
        conf = rec.get("confidence") or 0.5
        date_str = str(rec["created_at"].date()) if rec.get("created_at") else None

        chunk_text = f"Политика [{scope}]: {text}"

        return Chunk(
            chunk_text=chunk_text,
            chunk_type="distilled",
            source_kind=source_kind,
            parent_1c_ref=str(rec["id"]),
            chunk_date=date_str,
            confidence=float(conf),
            token_count=max(1, len(chunk_text) // 3),
        )
