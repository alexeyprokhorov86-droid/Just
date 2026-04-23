"""
tools/procurement_builder — сборка payload ПТУ из Заказа + УПД + шаблона.

Стратегия: **content-copy c fallback на предыдущее ПТУ того же Партнёра**.
Для первого POST нужно заполнить набор полей, которые 1С проверяет при проведении:
  - Общие (Партнёр/Организация/Соглашение/Склад/...)  → из Заказа
  - Данные «входящего документа»                       → из УПД
  - Аналитика учёта / банковские счета / руководство   → из шаблона (предыдущего ПТУ)
  - Принял                                              → от пользователя
  - Даты + комментарий                                  → now + автоген

Типичная проблема Post: payload неполный (РаздельныйУчетТоваровУСН,
АналитикаУчетаПоПартнерам) — эти поля не в документе, а в связанных регистрах.
1С заполняет их по Партнёру+Соглашению, если они стабильны. Шаблон от того же
партнёра гарантирует корректность.

Если предыдущего ПТУ от поставщика нет — возвращаем ошибку: создание первого
ПТУ нужно делать вручную в 1С (пока).
"""
from __future__ import annotations

import datetime
import logging
import os
import pathlib
from typing import Optional
from urllib.parse import quote

import psycopg2
import requests
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from requests.auth import HTTPBasicAuth

_REPO = pathlib.Path(__file__).resolve().parent.parent
load_dotenv(_REPO / ".env")

logger = logging.getLogger("procurement_builder")

BASE = os.environ["ODATA_BASE_URL"].rstrip("/")
AUTH = HTTPBasicAuth(os.environ["ODATA_USERNAME"], os.environ["ODATA_PASSWORD"])


def _db_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "172.20.0.2"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "knowledge_base"),
        user=os.getenv("DB_USER", "knowledge"),
        password=os.getenv("DB_PASSWORD", ""),
    )


def _odata_get(entity: str, ref_key: str) -> dict:
    url = f"{BASE}/{quote(entity, safe='_')}(guid'{ref_key}')?$format=json"
    r = requests.get(url, auth=AUTH, timeout=60)
    r.raise_for_status()
    return r.json()


# ─── Pydantic output ─────────────────────────────────────────────────────

class BuildWarning(BaseModel):
    code: str
    message: str


class BuildResult(BaseModel):
    ok: bool
    payload: Optional[dict] = None
    template_ref_key: Optional[str] = None
    template_number: Optional[str] = None
    warnings: list[BuildWarning] = Field(default_factory=list)
    errors: list[str] = Field(default_factory=list)


# ─── Поиск шаблона (предыдущее ПТУ того же партнёра) ─────────────────────

def _find_template_ptu(partner_key: str) -> Optional[dict]:
    """Берёт последнее проведённое непомеченное ПТУ от того же партнёра.
    Использует локальный c1_purchases для быстрого поиска ref_key,
    затем GET живого payload из OData."""
    with _db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ref_key FROM c1_purchases
                WHERE partner_key = %s
                  AND posted = TRUE
                  AND is_deleted = FALSE
                ORDER BY doc_date DESC
                LIMIT 1
                """,
                (partner_key,),
            )
            row = cur.fetchone()
    if not row:
        return None
    try:
        return _odata_get("Document_ПриобретениеТоваровУслуг", row[0])
    except Exception as e:
        logger.warning("failed to load template PTU %s: %s", row[0], e)
        return None


# ─── Копирование полей из шаблона ────────────────────────────────────────

# Поля которые берём ИЗ ШАБЛОНА (устойчивые для пары организация × партнёр).
# Избегаем очевидно-специфичных: Date, Number, Ref_Key, Посадочные (Принял...),
# ссылок на конкретный Заказ (ЗаказПоставщику_Key), УПД-полей.
TEMPLATE_COPY = [
    "БанковскийСчетОрганизации_Key",
    "БанковскийСчетКонтрагента_Key",
    "БанковскийСчетГрузоотправителя_Key",
    "Грузоотправитель_Key",
    "Руководитель_Key",
    "ГлавныйБухгалтер_Key",
    "ГруппаФинансовогоУчета_Key",
    "СтатьяДвиженияДенежныхСредств_Key",
    "ВалютаВзаиморасчетов_Key",
    "ФормаОплаты",
    "НалогообложениеНДС",
    "ХозяйственнаяОперация",
    "ПорядокРасчетов",
    "ПоступлениеПоЗаказам",
    "НоваяМеханикаСозданияЗаявленийОВвозе",
    "КурсЧислитель",
    "КурсЗнаменатель",
    # Административное:
    "Подразделение_Key",
    "Менеджер_Key",
]

# Поля которые берём ИЗ ЗАКАЗА (общие для Заказ↔ПТУ).
ORDER_COPY = [
    "Организация_Key",
    "Партнер_Key",
    "Контрагент_Key",
    "Склад_Key",
    "Валюта_Key",
    "Соглашение_Key",
    "ЦенаВключаетНДС",
    "ЗакупкаПодДеятельность",
    "ВариантПриемкиТоваров",
    "СпособДоставки",
    "НаправлениеДеятельности_Key",
]

# Строки табличной части: копируем из Заказа, добавляем ПТУ-специфичные из шаблона.
ORDER_ROW_COPY = [
    "Номенклатура_Key",
    "НоменклатураПартнера_Key",
    "Характеристика_Key",
    "Упаковка_Key",
    "ВидЦеныПоставщика_Key",
    "СтавкаНДС_Key",
    "СтатьяРасходов_Key",
    "Назначение_Key",
    "Подразделение_Key",
    "Склад_Key",
    "АналитикаРасходов_Type",
    "СписатьНаРасходы",
    "ПроцентРучнойСкидки",
    "СуммаРучнойСкидки",
]

# ПТУ-специфичные поля строки — из строки шаблона (по Номенклатура_Key).
TEMPLATE_ROW_COPY = [
    "АналитикаУчетаНоменклатуры_Key",
    "ВидЗапасов_Key",
    "НомерГТД_Key",
    "Сделка_Key",
    "Серия_Key",
    "СтатусУказанияСерий",
    "КоличествоПоРНПТ",
]


def _pick(src: dict, keys: list[str]) -> dict:
    return {k: src[k] for k in keys if k in src and src[k] is not None}


def _find_template_row(template: dict, nomenclature_key: str) -> Optional[dict]:
    """Ищет строку шаблона ПТУ с той же номенклатурой — для копирования аналитики."""
    for row in template.get("Товары") or []:
        if row.get("Номенклатура_Key") == nomenclature_key:
            return row
    # Если нет точного совпадения — вернём первую строку (как fallback, аналитика
    # обычно одинакова внутри одного партнёра).
    rows = template.get("Товары") or []
    return rows[0] if rows else None


# ─── Главная функция ─────────────────────────────────────────────────────

def build_ptu_payload(
    *,
    order_ref_key: str,
    upd,                          # UpdExtractResult
    accepter_ref_key: str = "00000000-0000-0000-0000-000000000000",
    accepter_position: str = "",
    labels: Optional[list] = None,  # list[LabelExtractResult]: этикетки для серий
    now: Optional[datetime.datetime] = None,
) -> BuildResult:
    """Собирает payload ПТУ для POST в 1С.

    Args:
      order_ref_key: Ref_Key выбранного Заказа поставщику.
      upd: результат extract_upd() — source of truth для «входящего документа»
        (номер, дата, сумма) + факт количеств позиций.
      accepter_ref_key: 1С-Ref_Key сотрудника «Груз принял».
      accepter_position: строковая должность для поля ПринялДолжность.
      now: датавремя документа; по умолчанию = datetime.now().

    Returns:
      BuildResult с payload (dict для передачи в tools.onec_write.create_document).
    """
    result = BuildResult(ok=False)
    now = now or datetime.datetime.now().replace(microsecond=0)

    # 1. Загрузка Заказа
    try:
        order = _odata_get("Document_ЗаказПоставщику", order_ref_key)
    except Exception as e:
        result.errors.append(f"Не удалось загрузить Заказ {order_ref_key}: {e}")
        return result
    partner_key = order.get("Партнер_Key")
    if not partner_key:
        result.errors.append("В Заказе не заполнен Партнер_Key.")
        return result

    # 2. Шаблон — последнее ПТУ от того же партнёра
    template = _find_template_ptu(partner_key)
    if template is None:
        result.errors.append(
            f"У партнёра ({partner_key}) нет предыдущих проведённых ПТУ — "
            "первое поступление нужно оформить вручную в 1С, чтобы зафиксировать "
            "аналитику учёта и банковские счета."
        )
        return result
    result.template_ref_key = template.get("Ref_Key")
    result.template_number = template.get("Number")

    # 3. Собираем верхний уровень payload
    payload: dict = {}
    payload.update(_pick(order, ORDER_COPY))
    payload.update(_pick(template, TEMPLATE_COPY))

    # Автор — тот кто инициирует через бот; берём из шаблона (system user OData)
    if template.get("Автор_Key"):
        payload["Автор_Key"] = template["Автор_Key"]

    # Ссылка на заказ
    payload["ЗаказПоставщику_Key"] = order_ref_key

    # Даты
    payload["Date"] = now.isoformat()
    payload["ДатаПоступления"] = now.replace(hour=0, minute=0, second=0).isoformat()

    # УПД → входящий документ
    doc = upd.document
    if doc.number:
        payload["НомерВходящегоДокумента"] = doc.number
    if doc.date:
        # upd.document.date — YYYY-MM-DD (строка); OData ждёт ISO datetime
        payload["ДатаВходящегоДокумента"] = f"{doc.date}T00:00:00"
    payload["НаименованиеВходящегоДокумента"] = doc.type or "УПД"
    if doc.total_amount is not None:
        payload["СуммаДокумента"] = doc.total_amount
        payload["СуммаВзаиморасчетов"] = doc.total_amount

    # Принял
    payload["Принял_Key"] = accepter_ref_key
    payload["ПринялДолжность"] = accepter_position or ""

    # Согласованность
    payload["Согласован"] = True

    # Комментарий
    payload["Комментарий"] = (
        f"Создано автоматически из Telegram-бота "
        f"по Заказу №{(order.get('Number') or '').strip()} "
        f"и УПД №{doc.number or '?'} от {doc.date or '?'}."
    )

    # 4. Строки табличной части
    order_rows = order.get("Товары") or []
    upd_items = upd.items or []
    if not order_rows:
        result.errors.append("В Заказе пустая табличная часть.")
        return result
    if not upd_items:
        result.errors.append("В УПД пустые позиции.")
        return result

    # MVP: совпадение по порядку строк. Расхождение по количеству строк → warning.
    if len(order_rows) != len(upd_items):
        result.warnings.append(BuildWarning(
            code="row_count_mismatch",
            message=(
                f"В Заказе {len(order_rows)} позиций, в УПД {len(upd_items)}. "
                "Строим по первой паре; при расхождении в количестве/номенклатуре "
                "проверьте вручную."
            ),
        ))

    rows = []
    n = min(len(order_rows), len(upd_items))
    for i in range(n):
        o_row = order_rows[i]
        u_item = upd_items[i]
        nomen_key = o_row.get("Номенклатура_Key")
        tmpl_row = _find_template_row(template, nomen_key) if nomen_key else None

        row_out = {"LineNumber": str(i + 1)}
        row_out.update(_pick(o_row, ORDER_ROW_COPY))
        if tmpl_row:
            row_out.update(_pick(tmpl_row, TEMPLATE_ROW_COPY))
        else:
            result.warnings.append(BuildWarning(
                code="no_template_row",
                message=(
                    f"Не нашли строку шаблонного ПТУ с номенклатурой {nomen_key}. "
                    "ПТУ-специфичные поля строки (АналитикаУчетаНоменклатуры, "
                    "ВидЗапасов) не заполнены — Post может упасть."
                ),
            ))

        # Количество — факт из УПД (отражаем реальную приёмку)
        qty = u_item.quantity if u_item.quantity is not None else o_row.get("Количество", 0)
        row_out["Количество"] = qty
        row_out["КоличествоУпаковок"] = qty

        # Цена в ПТУ должна совпасть с ценой в Заказе, иначе 1С не покажет
        # «Цена в заказе». Выбор правильной цены из УПД по флагу ЦенаВключаетНДС:
        #   True  → цена с НДС = sum_with_nds / quantity
        #   False → цена без НДС = sum_without_nds / quantity (= "price" УПД)
        price_order = float(o_row.get("Цена", 0) or 0)
        price_includes_vat = bool(payload.get("ЦенаВключаетНДС"))
        sum_nds = u_item.nds_sum or 0
        sum_with_nds = u_item.sum_with_nds if u_item.sum_with_nds is not None else (
            (u_item.sum_without_nds or 0) + sum_nds
        )
        if qty and qty > 0:
            if price_includes_vat:
                price_calc = round(float(sum_with_nds) / qty, 2)
            else:
                sum_no_nds = u_item.sum_without_nds if u_item.sum_without_nds is not None else (
                    (float(sum_with_nds) - sum_nds)
                )
                price_calc = round(float(sum_no_nds) / qty, 2)
        else:
            price_calc = u_item.price or price_order
        # Расхождение с Заказом → warning (больше 1 копейки)
        if price_order and abs(price_calc - price_order) > 0.01:
            result.warnings.append(BuildWarning(
                code="price_mismatch",
                message=(
                    f"Цена в УПД ({price_calc:.2f} ₽, "
                    f"{'с НДС' if price_includes_vat else 'без НДС'}) не совпадает с ценой "
                    f"в Заказе ({price_order:.2f} ₽). «Цена в заказе» не отобразится в ПТУ — "
                    "проверьте условия приёмки."
                ),
            ))
        row_out["Цена"] = price_calc

        # Серия — если для номенклатуры включены серии и есть этикетка
        if labels and nomen_key:
            from tools.onec_series import (
                resolve_vid_key_by_nomenclature, find_series,
            )
            # Для MVP берём первую этикетку (один УПД = одна партия)
            lbl = labels[0]
            if lbl.batch_number and lbl.production_date:
                vid_key = resolve_vid_key_by_nomenclature(nomen_key)
                if vid_key:
                    try:
                        existing = find_series(vid_key, lbl.batch_number, lbl.production_date)
                    except Exception as e:
                        existing = None
                        logger.warning("find_series failed: %s", e)
                    if existing:
                        row_out["Серия_Key"] = existing
                        row_out["СтатусУказанияСерий"] = 2  # полное указание
                    else:
                        result.warnings.append(BuildWarning(
                            code="series_not_found",
                            message=(
                                f"Серия № {lbl.batch_number} от {lbl.production_date} не найдена "
                                "в 1С. Создание серий через OData заблокировано "
                                "ОбработкойЗаполнения — создайте серию вручную в 1С "
                                "(Номенклатура → Серии → Создать) и повторите."
                            ),
                        ))
        # В 1С для ЦенаВключаетНДС=True: Сумма = сумма с НДС
        row_out["Сумма"] = sum_with_nds
        row_out["СуммаНДС"] = sum_nds
        row_out["СуммаНДСВзаиморасчетов"] = sum_nds  # было забыто → 0 в ПТУ
        row_out["СуммаСНДС"] = sum_with_nds
        row_out["СуммаВзаиморасчетов"] = sum_with_nds
        row_out["СуммаИтог"] = sum_with_nds

        rows.append(row_out)
    payload["Товары"] = rows

    result.payload = payload
    result.ok = True
    return result


__all__ = ["build_ptu_payload", "BuildResult", "BuildWarning"]
