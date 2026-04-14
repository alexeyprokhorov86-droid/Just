"""
1С Structured chunker — чанки из c1_* таблиц.
Этап 2 плана.
"""
import logging
from typing import List

from chunkers.base_chunker import BaseChunker, Chunk
from chunkers.config import CONFIDENCE

logger = logging.getLogger(__name__)


class OneCChunker(BaseChunker):

    def generate_chunks(self, full: bool = False) -> List[Chunk]:
        chunks = []
        chunks.extend(self._customer_orders(full))
        chunks.extend(self._sales(full))
        chunks.extend(self._staff(full))
        chunks.extend(self._specifications(full))
        chunks.extend(self._bank_expenses(full))
        chunks.extend(self._periodic_reports(full))
        return chunks

    def _customer_orders(self, full: bool) -> List[Chunk]:
        """Заказы клиентов → structured chunks."""
        # TODO: SELECT из c1_customer_orders + шаблон из плана (4.1)
        logger.info("OneCChunker: customer_orders — NOT IMPLEMENTED YET")
        return []

    def _sales(self, full: bool) -> List[Chunk]:
        """Реализация товаров → structured chunks."""
        # TODO: SELECT из c1_sales + шаблон (4.3)
        logger.info("OneCChunker: sales — NOT IMPLEMENTED YET")
        return []

    def _staff(self, full: bool) -> List[Chunk]:
        """Кадровые события → structured chunks."""
        # TODO: SELECT из c1_staff_history + шаблон (4.6)
        logger.info("OneCChunker: staff — NOT IMPLEMENTED YET")
        return []

    def _specifications(self, full: bool) -> List[Chunk]:
        """Спецификации + BOM → structured chunks."""
        # TODO: SELECT из c1_specifications + c1_spec_materials + bom_expanded (4.7)
        logger.info("OneCChunker: specifications — NOT IMPLEMENTED YET")
        return []

    def _bank_expenses(self, full: bool) -> List[Chunk]:
        """Банковские расходы, агрегация по контрагенту × неделя."""
        # TODO: SELECT из c1_bank_balances + группировка (4.5)
        logger.info("OneCChunker: bank_expenses — NOT IMPLEMENTED YET")
        return []

    def _periodic_reports(self, full: bool) -> List[Chunk]:
        """Ежедневные/еженедельные/ежемесячные сводки из mart_*."""
        # TODO: SELECT из mart_sales + агрегация (4.9)
        logger.info("OneCChunker: periodic_reports — NOT IMPLEMENTED YET")
        return []
