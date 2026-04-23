"""
tools/onec_write — низкоуровневый слой записи в 1С через OData.

Функции:
  - create_document(doc_type, payload) → ref_key
  - patch_document(doc_type, ref_key, patch) → dict
  - post_document(doc_type, ref_key, operational=False) → dict
  - unpost_document(doc_type, ref_key) → dict
  - mark_deleted(doc_type, ref_key) → dict         # наш rollback
  - create_and_post(doc_type, payload) → ref_key   # композит с авто-rollback при ошибке Post

Ключевые ограничения OData в 1С (проверены 23.04 smoke-тестом на
Document_ПриобретениеТоваровУслуг):

  - При POST на коллекцию 1С генерирует Ref_Key и Number из боевой серии.
    Не слать в payload: Ref_Key, Number, DeletionMark, Posted, DataVersion,
    МоментВремени, Проведен, ключи с '@' (navigationLinkUrl), поля с None.
    Строки табличных частей: убрать Ref_Key внутри строки.
  - Полное копирование payload существующего документа (~105 полей) → 500
    без деталей. Рабочий путь — content-copy: явный минимум ключевых полей
    + табличная часть с очищенными строками.
  - POST /Post падает если payload неполный (незаполнены обязательные
    для проведения поля: РаздельныйУчетТоваров, АналитикаУчетаПоПартнерам).
    Ответственность за полноту — у сборщика payload (procurement_builder).
  - **DELETE на документ → 500** «Не удалось записать: Реестр документов».
    Физическое удаление через OData в 1С не поддерживается.
    Rollback = PATCH DeletionMark=True → документ скрывается из форм,
    периодически чистится админом через «Удаление помеченных» в 1С UI.
"""
from __future__ import annotations

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

logger = logging.getLogger("onec_write")

BASE = os.environ["ODATA_BASE_URL"].rstrip("/")
AUTH = HTTPBasicAuth(os.environ["ODATA_USERNAME"], os.environ["ODATA_PASSWORD"])

DEFAULT_TIMEOUT = 60


# Поля которые 1С генерирует/валидирует — не слать в POST.
_STRIP_TOP = {
    "Ref_Key",
    "Number",
    "DeletionMark",
    "Posted",
    "DataVersion",
    "МоментВремени",
    "Проведен",
}
_STRIP_ROW = {"Ref_Key", "LineNumber_Type"}


class ODataError(RuntimeError):
    """Ошибка OData 1С. Содержит статус и тело ответа для диагностики."""
    def __init__(self, op: str, status: int, body: str):
        self.op = op
        self.status = status
        self.body = body
        super().__init__(f"OData {op} failed: HTTP {status} — {body[:400]}")


def _url(path: str) -> str:
    # Кириллица в path должна быть pre-encoded (CLAUDE.md § OData quirks)
    return f"{BASE}/{path}"


def _entity_ref(doc_type: str, ref_key: str) -> str:
    # Document_ПриобретениеТоваровУслуг(guid'...') — имя Entity квотится,
    # Ref_Key — нет (GUID ASCII).
    return f"{quote(doc_type, safe='_')}(guid'{ref_key}')"


def sanitize_payload(payload: dict) -> dict:
    """Очищает payload перед POST: снимает generated/readonly поля и None-значения.
    Рекурсивно чистит строки табличных частей."""
    out: dict = {}
    for k, v in payload.items():
        if k in _STRIP_TOP:
            continue
        if k.startswith("odata") or "@" in k:
            continue
        if v is None:
            continue
        if isinstance(v, list) and v and isinstance(v[0], dict):
            rows = []
            for row in v:
                clean = {}
                for rk, rv in row.items():
                    if rk in _STRIP_ROW or rk.startswith("odata") or "@" in rk:
                        continue
                    if rv is None:
                        continue
                    clean[rk] = rv
                rows.append(clean)
            out[k] = rows
        else:
            out[k] = v
    return out


# ─── CRUD ────────────────────────────────────────────────────────────────

def create_document(doc_type: str, payload: dict, *, sanitize: bool = True) -> str:
    """POST на коллекцию документов. Возвращает Ref_Key созданного.
    1С присваивает номер из боевой серии (если payload содержательный) или
    из отдельной тестовой серии (если не хватает ключевых атрибутов)."""
    body = sanitize_payload(payload) if sanitize else payload
    url = _url(f"{quote(doc_type, safe='_')}?$format=json")
    r = requests.post(url, json=body, auth=AUTH, timeout=DEFAULT_TIMEOUT)
    if r.status_code >= 400:
        raise ODataError("create_document", r.status_code, r.text)
    data = r.json()
    ref = data.get("Ref_Key")
    if not ref:
        raise ODataError("create_document", 500, f"no Ref_Key in response: {r.text[:300]}")
    logger.info("create_document %s → Ref_Key=%s Number=%s", doc_type, ref, data.get("Number"))
    return ref


def get_document(doc_type: str, ref_key: str) -> dict:
    r = requests.get(_url(f"{_entity_ref(doc_type, ref_key)}?$format=json"),
                     auth=AUTH, timeout=DEFAULT_TIMEOUT)
    if r.status_code == 404:
        raise ODataError("get_document", 404, "not found")
    if r.status_code >= 400:
        raise ODataError("get_document", r.status_code, r.text)
    return r.json()


def patch_document(doc_type: str, ref_key: str, patch: dict) -> dict:
    """PATCH — частичное обновление. Основной способ исправить поля после create."""
    r = requests.patch(_url(f"{_entity_ref(doc_type, ref_key)}?$format=json"),
                       json=patch, auth=AUTH, timeout=DEFAULT_TIMEOUT)
    if r.status_code >= 400:
        raise ODataError("patch_document", r.status_code, r.text)
    return r.json()


def post_document(doc_type: str, ref_key: str, *, operational: bool = False) -> dict:
    """POST /Post — провести документ. Падает на неполном payload
    (1С валидирует бизнес-правила при проведении, не при create)."""
    params_str = f"$format=json&PostingModeOperational={str(operational).lower()}"
    r = requests.post(_url(f"{_entity_ref(doc_type, ref_key)}/Post?{params_str}"),
                      auth=AUTH, timeout=DEFAULT_TIMEOUT)
    if r.status_code >= 400:
        raise ODataError("post_document", r.status_code, r.text)
    return r.json() if r.text.strip() else {"ok": True}


def unpost_document(doc_type: str, ref_key: str) -> dict:
    """POST /Unpost — распровести. OK даже на непроведённом."""
    r = requests.post(_url(f"{_entity_ref(doc_type, ref_key)}/Unpost?$format=json"),
                      auth=AUTH, timeout=DEFAULT_TIMEOUT)
    if r.status_code >= 400:
        raise ODataError("unpost_document", r.status_code, r.text)
    return r.json() if r.text.strip() else {"ok": True}


def mark_deleted(doc_type: str, ref_key: str) -> dict:
    """PATCH DeletionMark=True — наш rollback вместо физического DELETE.
    Документ скрывается из форм; админ периодически чистит в 1С UI."""
    return patch_document(doc_type, ref_key, {"DeletionMark": True})


# ─── Композит: create + post с авто-rollback ────────────────────────────

def create_and_post(
    doc_type: str,
    payload: dict,
    *,
    operational: bool = False,
    on_create: Optional[callable] = None,
) -> dict:
    """Создать + провести. При ошибке Post документ помечается на удаление.

    Возвращает:
      {"ref_key": str, "number": str, "posted": bool, "rolled_back": bool,
       "error": Optional[str]}

    Поведение:
      - create_document → получили ref_key (документ в базе, Posted=False).
      - (опц.) on_create(ref_key) — колбэк для доп. действий (прикрепить файлы и т.п.)
        Если он кидает — всё равно пытаемся Post, но ошибка колбэка пойдёт в лог.
      - post_document → если OK, возвращаем posted=True.
      - Если Post упал → mark_deleted (rollback), возвращаем rolled_back=True с error.
      - Если Post упал И mark_deleted упал → критическое, но ref_key возвращаем
        чтобы в логе было за что зацепиться.
    """
    ref_key = create_document(doc_type, payload)
    out = {"ref_key": ref_key, "number": None, "posted": False,
           "rolled_back": False, "error": None}
    try:
        got = get_document(doc_type, ref_key)
        out["number"] = got.get("Number")
    except ODataError as e:
        logger.warning("create_and_post: verify-get failed: %s", e)

    if on_create is not None:
        try:
            on_create(ref_key)
        except Exception as e:
            logger.exception("on_create callback failed for %s: %s", ref_key, e)
            # Не блокируем Post — файлы можно прикрепить потом вручную

    try:
        post_document(doc_type, ref_key, operational=operational)
        out["posted"] = True
        logger.info("create_and_post OK: %s %s posted", doc_type, ref_key)
        return out
    except ODataError as e:
        logger.warning("create_and_post: Post failed for %s: %s", ref_key, e)
        out["error"] = str(e)
        try:
            mark_deleted(doc_type, ref_key)
            out["rolled_back"] = True
            logger.info("create_and_post: rolled back %s (DeletionMark=True)", ref_key)
        except ODataError as e2:
            logger.error("create_and_post: rollback FAILED for %s: %s", ref_key, e2)
            out["error"] = f"{out['error']} | rollback failed: {e2}"
        return out


__all__ = [
    "ODataError",
    "sanitize_payload",
    "create_document",
    "get_document",
    "patch_document",
    "post_document",
    "unpost_document",
    "mark_deleted",
    "create_and_post",
]
