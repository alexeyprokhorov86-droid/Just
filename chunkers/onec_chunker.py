"""
1С Structured chunker — чанки из c1_* таблиц.
Этап 2 плана. Потоковая обработка через server-side cursor.
Группа 1: простые документы (1 документ = 1 чанк).
"""
import logging
from typing import List, Dict, Optional

from chunkers.base_chunker import BaseChunker, Chunk

logger = logging.getLogger(__name__)

ONEC_CONFIDENCE = 0.95
FETCH_SIZE = 500


# ---------------------------------------------------------------------------
# SQL queries — header + items aggregated via lateral subquery
# ---------------------------------------------------------------------------

SQL_CUSTOMER_ORDERS = """
    SELECT co.ref_key, co.doc_number, co.doc_date, co.amount, co.status,
           co.shipment_date, co.comment,
           cl.name AS client_name,
           items.lines AS items_text
    FROM c1_customer_orders co
    LEFT JOIN clients cl ON co.partner_key = cl.id::text
    LEFT JOIN LATERAL (
        SELECT string_agg(
            n.name || ' ' || ci.quantity || ' шт × ' ||
            ci.price || ' руб = ' || ci.sum_total || ' руб',
            '; ' ORDER BY ci.line_number
        ) AS lines
        FROM c1_customer_order_items ci
        LEFT JOIN nomenclature n ON ci.nomenclature_key = n.id::text
        WHERE ci.order_key = co.ref_key
    ) items ON true
    WHERE co.is_deleted = false AND co.posted = true
      {where_extra}
    ORDER BY co.ref_key
"""

SQL_SUPPLIER_ORDERS = """
    SELECT so.ref_key, so.doc_number, so.doc_date, so.amount, so.status,
           so.comment,
           cl.name AS supplier_name,
           items.lines AS items_text
    FROM c1_supplier_orders so
    LEFT JOIN clients cl ON so.partner_key = cl.id::text
    LEFT JOIN LATERAL (
        SELECT string_agg(
            n.name || ' ' || si.quantity || ' шт × ' ||
            si.price || ' руб = ' || si.sum_total || ' руб',
            '; ' ORDER BY si.line_number
        ) AS lines
        FROM c1_supplier_order_items si
        LEFT JOIN nomenclature n ON si.nomenclature_key = n.id::text
        WHERE si.order_key = so.ref_key
    ) items ON true
    WHERE so.is_deleted = false AND so.posted = true
      {where_extra}
    ORDER BY so.ref_key
"""

SQL_PURCHASES = """
    SELECT p.ref_key, p.doc_number, p.doc_date, p.amount,
           p.incoming_number, p.incoming_date, p.comment,
           cl.name AS supplier_name,
           items.lines AS items_text
    FROM c1_purchases p
    LEFT JOIN clients cl ON p.partner_key = cl.id::text
    LEFT JOIN LATERAL (
        SELECT string_agg(
            n.name || ' ' || pi.quantity || ' шт × ' ||
            pi.price || ' руб = ' || pi.sum_total || ' руб',
            '; ' ORDER BY pi.line_number
        ) AS lines
        FROM c1_purchase_items pi
        LEFT JOIN nomenclature n ON pi.nomenclature_key = n.id::text
        WHERE pi.doc_key = p.ref_key
    ) items ON true
    WHERE p.is_deleted = false AND p.posted = true
      {where_extra}
    ORDER BY p.ref_key
"""

SQL_BANK_EXPENSES = """
    SELECT be.ref_key, be.doc_number, be.doc_date, be.amount,
           be.purpose, be.comment, be.operation,
           cl.name AS counterparty_name,
           cfi.name AS cash_flow_item_name
    FROM c1_bank_expenses be
    LEFT JOIN clients cl ON be.counterparty_key = cl.id::text
    LEFT JOIN c1_cash_flow_items cfi ON be.cash_flow_item_key = cfi.ref_key
    WHERE be.is_deleted = false AND be.posted = true
      {where_extra}
    ORDER BY be.ref_key
"""


class OneCChunker(BaseChunker):

    def __init__(self, dry_run: bool = False, batch_limit: int = 0):
        super().__init__(dry_run=dry_run)
        self.batch_limit = batch_limit

    def generate_chunks(self, full: bool = False) -> List[Chunk]:
        processors = [
            ("1c_customer_order", self._process_customer_orders),
            ("1c_supplier_order", self._process_supplier_orders),
            ("1c_purchase", self._process_purchases),
            ("1c_bank_expense", self._process_bank_expenses),
        ]
        total = 0
        for source_kind, proc in processors:
            count = proc(full, source_kind)
            total += count
        logger.info(f"OneCChunker done: {total} chunks total")
        return []

    # ------------------------------------------------------------------
    # Generic streaming processor
    # ------------------------------------------------------------------
    def _stream_and_save(self, sql: str, columns: List[str],
                         source_kind: str, full: bool,
                         row_to_chunk) -> int:
        """Потоковая обработка: cursor → format → save по batch."""
        if full:
            where_extra = ""
        else:
            alias = self._alias_from_sql(sql)
            where_extra = (
                f"AND {alias}.ref_key NOT IN "
                f"(SELECT parent_1c_ref FROM source_chunks "
                f"WHERE source_kind = '{source_kind}' AND parent_1c_ref IS NOT NULL)"
            )

        query = sql.format(where_extra=where_extra)
        if self.batch_limit > 0:
            query += f"\n LIMIT {self.batch_limit}"

        cursor_name = f"onec_{source_kind.replace('1c_', '')}_cursor"
        cur = self.conn.cursor(cursor_name)
        cur.itersize = FETCH_SIZE

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
                    chunk = row_to_chunk(rec, source_kind)
                    if chunk:
                        chunks.append(chunk)

                if chunks:
                    saved = self.save_chunks(chunks)
                    total += saved

                logger.info(
                    f"  {source_kind}: batch {len(rows)} docs → "
                    f"{len(chunks)} chunks (running total: {total})"
                )
        finally:
            cur.close()

        return total

    @staticmethod
    def _alias_from_sql(sql: str) -> str:
        """Extract main table alias from SQL (first FROM ... alias)."""
        # Pattern: FROM table_name alias WHERE
        import re
        m = re.search(r'FROM\s+\w+\s+(\w+)', sql)
        return m.group(1) if m else "co"

    # ------------------------------------------------------------------
    # Customer orders
    # ------------------------------------------------------------------
    COLS_CUSTOMER_ORDERS = [
        "ref_key", "doc_number", "doc_date", "amount", "status",
        "shipment_date", "comment", "client_name", "items_text",
    ]

    def _process_customer_orders(self, full: bool, source_kind: str) -> int:
        logger.info(f"OneCChunker: processing {source_kind}")
        return self._stream_and_save(
            SQL_CUSTOMER_ORDERS, self.COLS_CUSTOMER_ORDERS,
            source_kind, full, self._format_customer_order,
        )

    @staticmethod
    def _format_customer_order(rec: dict, source_kind: str) -> Optional[Chunk]:
        client = rec["client_name"] or "?"
        items = rec["items_text"] or "—"
        date_str = str(rec["doc_date"]) if rec["doc_date"] else "?"
        shipment = str(rec["shipment_date"]) if rec.get("shipment_date") else "—"

        text = (
            f"Заказ клиента №{rec['doc_number']} от {date_str}\n"
            f"Клиент: {client} | Статус: {rec.get('status') or '?'} | "
            f"Отгрузка: {shipment}\n"
            f"Сумма: {rec['amount']} руб.\n"
            f"Товары: {items}"
        )

        return Chunk(
            chunk_text=text,
            chunk_type="structured",
            source_kind=source_kind,
            parent_1c_ref=rec["ref_key"],
            chunk_date=date_str if date_str != "?" else None,
            confidence=ONEC_CONFIDENCE,
            token_count=max(1, len(text) // 3),
        )

    # ------------------------------------------------------------------
    # Supplier orders
    # ------------------------------------------------------------------
    COLS_SUPPLIER_ORDERS = [
        "ref_key", "doc_number", "doc_date", "amount", "status",
        "comment", "supplier_name", "items_text",
    ]

    def _process_supplier_orders(self, full: bool, source_kind: str) -> int:
        logger.info(f"OneCChunker: processing {source_kind}")
        return self._stream_and_save(
            SQL_SUPPLIER_ORDERS, self.COLS_SUPPLIER_ORDERS,
            source_kind, full, self._format_supplier_order,
        )

    @staticmethod
    def _format_supplier_order(rec: dict, source_kind: str) -> Optional[Chunk]:
        supplier = rec["supplier_name"] or "?"
        items = rec["items_text"] or "—"
        date_str = str(rec["doc_date"]) if rec["doc_date"] else "?"

        text = (
            f"Заказ поставщику №{rec['doc_number']} от {date_str}\n"
            f"Поставщик: {supplier} | Статус: {rec.get('status') or '?'}\n"
            f"Сумма: {rec['amount']} руб.\n"
            f"Товары: {items}"
        )

        return Chunk(
            chunk_text=text,
            chunk_type="structured",
            source_kind=source_kind,
            parent_1c_ref=rec["ref_key"],
            chunk_date=date_str if date_str != "?" else None,
            confidence=ONEC_CONFIDENCE,
            token_count=max(1, len(text) // 3),
        )

    # ------------------------------------------------------------------
    # Purchases
    # ------------------------------------------------------------------
    COLS_PURCHASES = [
        "ref_key", "doc_number", "doc_date", "amount",
        "incoming_number", "incoming_date", "comment",
        "supplier_name", "items_text",
    ]

    def _process_purchases(self, full: bool, source_kind: str) -> int:
        logger.info(f"OneCChunker: processing {source_kind}")
        return self._stream_and_save(
            SQL_PURCHASES, self.COLS_PURCHASES,
            source_kind, full, self._format_purchase,
        )

    @staticmethod
    def _format_purchase(rec: dict, source_kind: str) -> Optional[Chunk]:
        supplier = rec["supplier_name"] or "?"
        items = rec["items_text"] or "—"
        date_str = str(rec["doc_date"]) if rec["doc_date"] else "?"
        incoming = ""
        if rec.get("incoming_number"):
            incoming = f" (вх. №{rec['incoming_number']}"
            if rec.get("incoming_date"):
                incoming += f" от {rec['incoming_date']}"
            incoming += ")"

        text = (
            f"Приобретение №{rec['doc_number']} от {date_str}{incoming}\n"
            f"Поставщик: {supplier}\n"
            f"Сумма: {rec['amount']} руб.\n"
            f"Товары: {items}"
        )

        return Chunk(
            chunk_text=text,
            chunk_type="structured",
            source_kind=source_kind,
            parent_1c_ref=rec["ref_key"],
            chunk_date=date_str if date_str != "?" else None,
            confidence=ONEC_CONFIDENCE,
            token_count=max(1, len(text) // 3),
        )

    # ------------------------------------------------------------------
    # Bank expenses
    # ------------------------------------------------------------------
    COLS_BANK_EXPENSES = [
        "ref_key", "doc_number", "doc_date", "amount",
        "purpose", "comment", "operation",
        "counterparty_name", "cash_flow_item_name",
    ]

    def _process_bank_expenses(self, full: bool, source_kind: str) -> int:
        logger.info(f"OneCChunker: processing {source_kind}")
        return self._stream_and_save(
            SQL_BANK_EXPENSES, self.COLS_BANK_EXPENSES,
            source_kind, full, self._format_bank_expense,
        )

    @staticmethod
    def _format_bank_expense(rec: dict, source_kind: str) -> Optional[Chunk]:
        counterparty = rec["counterparty_name"] or "—"
        date_str = str(rec["doc_date"]) if rec["doc_date"] else "?"
        purpose = (rec.get("purpose") or "").strip() or "—"
        cfi = rec.get("cash_flow_item_name") or "—"

        text = (
            f"Списание ДС №{rec['doc_number']} от {date_str}\n"
            f"Контрагент: {counterparty} | Сумма: {rec['amount']} руб.\n"
            f"Назначение: {purpose}\n"
            f"Статья ДДС: {cfi}"
        )

        return Chunk(
            chunk_text=text,
            chunk_type="structured",
            source_kind=source_kind,
            parent_1c_ref=rec["ref_key"],
            chunk_date=date_str if date_str != "?" else None,
            confidence=ONEC_CONFIDENCE,
            token_count=max(1, len(text) // 3),
        )
