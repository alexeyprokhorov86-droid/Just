"""
Email chunker — envelope + body чанки из email_messages.
Этап 1 плана. Потоковая обработка через server-side cursor.
"""
import logging
import sys
from typing import List, Optional

from chunkers.base_chunker import BaseChunker, Chunk
from chunkers.config import BODY_OVERLAP_TOKENS, BODY_MIN_PARAGRAPH_TOKENS

# clean_email_text из проекта
sys.path.insert(0, ".")
from email_text_processing import clean_email_text, html_to_text

logger = logging.getLogger(__name__)

ENVELOPE_CONFIDENCE = 0.85
BODY_CONFIDENCE = 0.80
BODY_MAX_TOKENS = 400  # <= 400 tokens → один body чанк
CHARS_PER_TOKEN = 4    # ~4 символа = 1 токен (грубая оценка для русского)
FETCH_SIZE = 500        # server-side cursor batch size

COLUMNS = [
    "id", "from_address", "to_addresses", "subject", "body_text",
    "body_html", "received_at", "direction", "category", "thread_id",
]

SQL_FULL = """
    SELECT em.id, em.from_address, em.to_addresses, em.subject,
           em.body_text, em.body_html, em.received_at,
           em.direction, em.category, em.thread_id
    FROM email_messages em
    WHERE em.category IN ('internal', 'external_business')
    ORDER BY em.id
"""

SQL_INCREMENTAL = """
    SELECT em.id, em.from_address, em.to_addresses, em.subject,
           em.body_text, em.body_html, em.received_at,
           em.direction, em.category, em.thread_id
    FROM email_messages em
    LEFT JOIN source_chunks sc
        ON sc.source_kind = 'email'
        AND sc.chunk_type = 'envelope'
        AND sc.parent_1c_ref = em.id::text
    WHERE em.category IN ('internal', 'external_business')
      AND sc.id IS NULL
    ORDER BY em.id
"""


class EmailChunker(BaseChunker):

    def __init__(self, dry_run: bool = False, batch_limit: int = 0):
        super().__init__(dry_run=dry_run)
        self.batch_limit = batch_limit  # 0 = все

    def generate_chunks(self, full: bool = False) -> List[Chunk]:
        """Потоковая обработка: fetch 500 → process → save → fetch next."""
        query = SQL_FULL if full else SQL_INCREMENTAL
        if self.batch_limit > 0:
            query += f"\n LIMIT {self.batch_limit}"

        total_emails = 0
        total_envelope = 0
        total_body = 0
        total_saved = 0

        # Named cursor = server-side cursor (не грузит всё в память)
        cur = self.conn.cursor("email_stream_cursor")
        cur.itersize = FETCH_SIZE
        try:
            cur.execute(query)

            while True:
                rows = cur.fetchmany(FETCH_SIZE)
                if not rows:
                    break

                batch = [dict(zip(COLUMNS, row)) for row in rows]
                email_ids = [e["id"] for e in batch]
                sd_map = self._build_source_doc_map(email_ids)

                chunks: List[Chunk] = []
                for em in batch:
                    eid = em["id"]
                    parent_doc_id = sd_map.get(eid)
                    date_str = em["received_at"].strftime("%Y-%m-%d") if em["received_at"] else None

                    env = self._make_envelope(em, date_str)
                    env.parent_document_id = parent_doc_id
                    chunks.append(env)
                    total_envelope += 1

                    body_chunks = self._make_body_chunks(em, date_str, parent_doc_id)
                    chunks.extend(body_chunks)
                    total_body += len(body_chunks)

                total_emails += len(batch)

                # Сохраняем batch сразу (не копим в памяти)
                saved = self.save_chunks(chunks)
                total_saved += saved

                logger.info(
                    f"  batch: {len(batch)} emails → {len(chunks)} chunks "
                    f"(running total: {total_emails} emails, {total_saved} saved)"
                )

        finally:
            cur.close()

        logger.info(
            f"EmailChunker done: {total_emails} emails → "
            f"{total_envelope} envelope + {total_body} body = "
            f"{total_envelope + total_body} chunks, {total_saved} saved"
        )
        # Возвращаем пустой список — всё уже сохранено в save_chunks()
        return []

    # ------------------------------------------------------------------
    # Source documents mapping
    # ------------------------------------------------------------------
    def _build_source_doc_map(self, email_ids: List[int]) -> dict:
        """Возвращает {email_id: source_documents.id}."""
        if not email_ids:
            return {}
        cur = self.conn.cursor()
        try:
            refs = [f"email:{eid}" for eid in email_ids]
            cur.execute(
                """
                SELECT source_ref, id FROM source_documents
                WHERE source_kind = 'email_message'
                  AND source_ref = ANY(%s)
                """,
                (refs,),
            )
            result = {}
            for source_ref, sd_id in cur.fetchall():
                eid = int(source_ref.split(":")[1])
                result[eid] = sd_id
            return result
        finally:
            cur.close()

    # ------------------------------------------------------------------
    # Envelope
    # ------------------------------------------------------------------
    def _make_envelope(self, em: dict, date_str: Optional[str]) -> Chunk:
        subject = (em["subject"] or "(без темы)").strip()
        from_addr = em["from_address"] or "?"
        to_addrs = ", ".join(em["to_addresses"]) if em["to_addresses"] else "?"
        direction = em["direction"] or "?"
        category = em["category"] or "?"
        thread_id = em["thread_id"] or "—"

        # Суть: первые 200 символов очищенного body
        body_raw = (em["body_text"] or "").strip()
        if not body_raw and em.get("body_html"):
            body_raw = html_to_text(em["body_html"])
        summary = clean_email_text(body_raw)[:200].strip()
        if summary:
            for sep in (". ", "! ", "? "):
                idx = summary.rfind(sep)
                if idx > 50:
                    summary = summary[:idx + 1]
                    break

        text = (
            f"Email: {subject}\n"
            f"От: {from_addr} → Кому: {to_addrs}\n"
            f"Дата: {date_str or '?'} | Направление: {direction}\n"
            f"Категория: {category} | Тред: #{thread_id}\n"
            f"Суть: {summary or subject}"
        )

        return Chunk(
            chunk_text=text,
            chunk_type="envelope",
            source_kind="email",
            parent_1c_ref=str(em["id"]),
            chunk_date=date_str,
            confidence=ENVELOPE_CONFIDENCE,
            document_id=0,
            chunk_no=0,
            token_count=self.estimate_tokens(text),
        )

    # ------------------------------------------------------------------
    # Body
    # ------------------------------------------------------------------
    def _make_body_chunks(self, em: dict, date_str: Optional[str],
                          parent_doc_id: Optional[int]) -> List[Chunk]:
        body_raw = (em["body_text"] or "").strip()
        if not body_raw and em.get("body_html"):
            body_raw = html_to_text(em["body_html"])

        body = clean_email_text(body_raw)
        if not body or len(body.strip()) < 20:
            return []

        prefix = (
            f"Email от {em['from_address'] or '?'}, "
            f"тема: {(em['subject'] or '').strip()}, "
            f"дата: {date_str or '?'}\n\n"
        )

        est_tokens = self.estimate_tokens(body)

        if est_tokens <= BODY_MAX_TOKENS:
            texts = [body]
        else:
            texts = self._split_body(body)

        chunks = []
        for i, text in enumerate(texts):
            full_text = prefix + text
            chunks.append(Chunk(
                chunk_text=full_text,
                chunk_type="body",
                source_kind="email",
                parent_document_id=parent_doc_id,
                parent_1c_ref=str(em["id"]),
                chunk_date=date_str,
                confidence=BODY_CONFIDENCE,
                document_id=parent_doc_id or 0,
                chunk_no=i + 1,  # 0 = envelope
                token_count=self.estimate_tokens(full_text),
            ))
        return chunks

    def _split_body(self, body: str) -> List[str]:
        """Разбивает body по абзацам, склеивает мелкие, добавляет overlap."""
        paragraphs = [p.strip() for p in body.split("\n\n") if p.strip()]
        if not paragraphs:
            return [body]

        # Склеиваем мелкие абзацы
        merged: List[str] = []
        buf = ""
        for p in paragraphs:
            if buf:
                candidate = buf + "\n\n" + p
            else:
                candidate = p
            if self.estimate_tokens(candidate) < BODY_MIN_PARAGRAPH_TOKENS and p != paragraphs[-1]:
                buf = candidate
            else:
                if buf and self.estimate_tokens(buf) >= BODY_MIN_PARAGRAPH_TOKENS:
                    merged.append(buf)
                    buf = p
                else:
                    buf = candidate
        if buf:
            merged.append(buf)

        if not merged:
            return [body]

        if len(merged) == 1:
            return merged

        # Добавляем overlap между соседними чанками
        overlap_chars = BODY_OVERLAP_TOKENS * CHARS_PER_TOKEN
        result: List[str] = []
        for i, chunk in enumerate(merged):
            if i > 0 and overlap_chars > 0:
                prev_tail = merged[i - 1][-overlap_chars:]
                chunk = prev_tail + "\n\n" + chunk
            result.append(chunk)

        return result
