"""
tools/onec_attach — прикрепление файлов (сканов УПД, фото этикеток) к документам в 1С.

Для ПТУ используется `Catalog_ПриобретениеТоваровУслугПрисоединенныеФайлы`.
У каждого типа документа — свой каталог. Добавляйте маппинг по мере
расширения (Заказы, Реализации и т.п.) в `_ATTACH_CATALOG`.

Структура записи файла в 1С:
  - Description          — имя файла (без расширения)
  - Расширение           — "jpg" / "pdf" / ...
  - Размер               — байты
  - ВладелецФайла_Key    — ref_key документа-владельца
  - ФайлХранилище_Base64Data — содержимое файла в base64
  - ДатаСоздания         — ISO datetime
"""
from __future__ import annotations

import base64
import datetime
import logging
import os
import pathlib
from typing import Optional
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

_REPO = pathlib.Path(__file__).resolve().parent.parent
load_dotenv(_REPO / ".env")

logger = logging.getLogger("onec_attach")

BASE = os.environ["ODATA_BASE_URL"].rstrip("/")
AUTH = HTTPBasicAuth(os.environ["ODATA_USERNAME"], os.environ["ODATA_PASSWORD"])

# Каталог присоединённых файлов для каждого типа документа
_ATTACH_CATALOG = {
    "Document_ПриобретениеТоваровУслуг": "Catalog_ПриобретениеТоваровУслугПрисоединенныеФайлы",
    "Document_ЗаказПоставщику": "Catalog_ЗаказПоставщикуПрисоединенныеФайлы",
}


def attach_file(
    *,
    doc_type: str,
    owner_ref_key: str,
    file_bytes: bytes,
    name: str,
    extension: str = "jpg",
    description: str = "",
) -> str:
    """Прикрепляет файл к документу. Возвращает Ref_Key записи присоед. файла.

    Args:
      doc_type: тип документа-владельца (напр. Document_ПриобретениеТоваровУслуг).
      owner_ref_key: Ref_Key документа.
      file_bytes: содержимое.
      name: имя файла (без расширения, человекочитаемое; будет Description).
      extension: расширение без точки ('jpg', 'pdf').
      description: доп. описание (поле «Описание» в справочнике).
    """
    catalog = _ATTACH_CATALOG.get(doc_type)
    if not catalog:
        raise ValueError(
            f"Нет маппинга attach-каталога для {doc_type}. "
            f"Добавьте в tools.onec_attach._ATTACH_CATALOG."
        )

    b64 = base64.b64encode(file_bytes).decode()
    now = datetime.datetime.now().replace(microsecond=0).isoformat()
    payload = {
        "Description": name,
        "Расширение": extension.lstrip("."),
        "Размер": len(file_bytes),
        "ВладелецФайла_Key": owner_ref_key,
        "ФайлХранилище_Base64Data": b64,
        "ДатаСоздания": now,
        "ДатаМодификацииУниверсальная": now,
        "ТипХраненияФайла": "ВИнформационнойБазе",
    }
    if description:
        payload["Описание"] = description

    url = f"{BASE}/{quote(catalog, safe='_')}?$format=json"
    r = requests.post(url, json=payload, auth=AUTH, timeout=120)
    if r.status_code >= 400:
        raise RuntimeError(
            f"attach_file failed ({catalog}): HTTP {r.status_code}: {r.text[:400]}"
        )
    data = r.json()
    ref = data.get("Ref_Key")
    if not ref:
        raise RuntimeError(f"no Ref_Key in response: {r.text[:300]}")
    logger.info(
        "attach_file: %s → %s (%s.%s, %d bytes) ref=%s",
        doc_type, owner_ref_key, name, extension, len(file_bytes), ref,
    )
    return ref


def attach_upd_photos(
    ptu_ref_key: str,
    photos: list[tuple[bytes, str]],
    upd_number: Optional[str] = None,
    upd_date: Optional[str] = None,
) -> list[str]:
    """Прикрепляет фото УПД к ПТУ. Именование по стандарту:
    «УПД № 303 от 23.04.2026 (1).jpg» — номер входящего + дата + порядковый индекс.

    Args:
      photos: list[(bytes, extension)] — каждое фото с расширением.
      upd_number, upd_date: для формирования имени.
    """
    prefix_parts = ["УПД"]
    if upd_number:
        prefix_parts.append(f"№ {upd_number}")
    if upd_date:
        # upd_date в формате YYYY-MM-DD → DD.MM.YYYY
        try:
            d = datetime.date.fromisoformat(upd_date[:10])
            prefix_parts.append(f"от {d.strftime('%d.%m.%Y')}")
        except Exception:
            prefix_parts.append(f"от {upd_date}")
    prefix = " ".join(prefix_parts)

    refs = []
    for idx, (blob, ext) in enumerate(photos, start=1):
        suffix = f" ({idx})" if len(photos) > 1 else ""
        name = f"{prefix}{suffix}"
        refs.append(attach_file(
            doc_type="Document_ПриобретениеТоваровУслуг",
            owner_ref_key=ptu_ref_key,
            file_bytes=blob,
            name=name,
            extension=ext,
            description=f"Скан УПД, прикреплён автоматически из Telegram-бота",
        ))
    return refs


__all__ = ["attach_file", "attach_upd_photos"]
