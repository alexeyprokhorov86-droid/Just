"""
Tool: get_bom_report — форматированный BOM-отчёт по одному продукту.

Делегирует в bom_exploder.get_bom_report (авторитетная реализация).
"""
from __future__ import annotations

from pydantic import BaseModel, Field

from .registry import tool


class GetBomReportInput(BaseModel):
    product_key: str = Field(
        description=(
            "Ref_Key продукта из nomenclature (UUID, напр. "
            "'7dac702d-dab7-11ec-bf30-000c29247c35'). Берётся из последнего "
            "успешного расчёта (bom_calculations.status='completed')."
        ),
        min_length=1,
    )


@tool(
    name="get_bom_report",
    domain="bom",
    description=(
        "Возвращает человекочитаемый отчёт BOM (bill of materials) для одного "
        "готового продукта по product_key: состав материалов с группировкой по "
        "уровням (type_level_1/2/3), подитогами по уровням и ОБЩИМ ВЕСОМ в кг. "
        "Если есть ошибки разворачивания — добавляет раздел '⚠️ ОШИБКИ'. "
        "Использует последний завершённый расчёт из bom_calculations."
    ),
    input_model=GetBomReportInput,
)
def get_bom_report(product_key: str) -> str:
    # Импорт внутри функции: bom_exploder тянет тяжёлые зависимости (psycopg2
    # RealDictCursor + Decimal), нет смысла грузить при import tools.
    from bom_exploder import get_bom_report as _impl

    return _impl(product_key)
