#!/usr/bin/env python3
"""Фаза 3.0 — OData CREATE smoke-test.

Копирует payload существующего проведённого ПТУ (НФ00-000531, 3500 руб),
создаёт новый документ, проводит, откатывает (Unpost + DELETE).

Порядок:
  1. GET НФ00-000531 (живое проведённое ПТУ).
  2. Sanitize payload: снять Ref_Key, Number (автоген), DeletionMark, Posted.
  3. POST на коллекцию → получить новый Ref_Key.
  4. GET нового → проверка Posted=false (1С создаёт непроведённым).
  5. POST /Post → провести.
  6. GET → проверка Posted=true.
  7. POST /Unpost → распровести.
  8. DELETE → окончательно удалить.
  9. GET → должно быть 404 или DeletionMark=true.

Если падает на шаге 3 — всё ок, ничего не создалось.
Если падает после 3 — в логах stderr будет Ref_Key созданного, ручной DELETE
через `python3 scripts/odata_create_probe.py --cleanup <ref>`.

Запуск: python3 scripts/odata_create_probe.py
"""

from __future__ import annotations

import argparse
import copy
import datetime
import json
import os
import pathlib
import sys

import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

REPO = pathlib.Path(__file__).resolve().parent.parent
load_dotenv(REPO / ".env")

BASE = os.environ["ODATA_BASE_URL"].rstrip("/")
AUTH = HTTPBasicAuth(os.environ["ODATA_USERNAME"], os.environ["ODATA_PASSWORD"])
DOC = "Document_ПриобретениеТоваровУслуг"

SOURCE_REF = "414b838f-3e4c-11f1-8e2f-000c299cc968"  # НФ00-000531, 3500 руб


def _url(path: str) -> str:
    return f"{BASE}/{path}"


def get_doc(ref: str) -> dict:
    r = requests.get(
        _url(f"{DOC}(guid'{ref}')"),
        params={"$format": "json"},
        auth=AUTH,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def post_collection(payload: dict) -> dict:
    r = requests.post(
        _url(DOC),
        params={"$format": "json"},
        json=payload,
        auth=AUTH,
        timeout=60,
    )
    if r.status_code >= 400:
        print(f"  ❌ POST {r.status_code}: {r.text[:800]}", file=sys.stderr)
        r.raise_for_status()
    return r.json()


def post_action(ref: str, action: str, operational: bool = False) -> dict:
    params = {"$format": "json"}
    if action == "Post":
        params["PostingModeOperational"] = str(operational).lower()
    r = requests.post(
        _url(f"{DOC}(guid'{ref}')/{action}"),
        params=params,
        auth=AUTH,
        timeout=60,
    )
    if r.status_code >= 400:
        print(f"  ❌ {action} {r.status_code}: {r.text[:500]}", file=sys.stderr)
        r.raise_for_status()
    return r.json() if r.text.strip() else {"ok": True}


def delete_doc(ref: str) -> None:
    r = requests.delete(
        _url(f"{DOC}(guid'{ref}')"),
        auth=AUTH,
        timeout=60,
    )
    if r.status_code >= 400:
        print(f"  ❌ DELETE {r.status_code}: {r.text[:500]}", file=sys.stderr)
        r.raise_for_status()


# Поля которые нельзя копировать (1С генерирует / смотрит на них при POST)
STRIP_KEYS_SELF = {
    "Ref_Key",
    "Number",
    "DeletionMark",
    "Posted",
    "DataVersion",
    "МоментВремени",
    "Проведен",
}
# Поля которые нельзя копировать внутри строк табличных частей
STRIP_KEYS_ROW = {
    "Ref_Key",        # 1С генерирует для строки
    "LineNumber",     # можно оставить, но 1С пересчитает
    "LineNumber_Type",
}
# Ключи вида "X_Type" для Composite Pointer — копируем как есть.


def sanitize(payload: dict) -> dict:
    """Глубокая копия без служебных/генерируемых полей и null-значений."""
    out: dict = {}
    for k, v in payload.items():
        if k in STRIP_KEYS_SELF:
            continue
        if k.startswith("odata") or "@" in k:
            continue
        if v is None:
            continue
        # Табличная часть = list[dict]
        if isinstance(v, list) and v and isinstance(v[0], dict):
            rows = []
            for row in v:
                clean = {}
                for rk, rv in row.items():
                    if rk in STRIP_KEYS_ROW or rk.startswith("odata") or "@" in rk:
                        continue
                    if rv is None:
                        continue
                    clean[rk] = rv
                rows.append(clean)
            out[k] = rows
        else:
            out[k] = v
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=SOURCE_REF, help="Ref_Key образца ПТУ")
    ap.add_argument("--keep", action="store_true", help="Не удалять созданный ПТУ в конце")
    ap.add_argument("--cleanup", help="Только удалить указанный Ref_Key (rollback) и выйти")
    args = ap.parse_args()

    if args.cleanup:
        print(f"Cleanup: Unpost + DELETE {args.cleanup}")
        try:
            post_action(args.cleanup, "Unpost")
        except Exception as e:
            print(f"  (Unpost skip: {e})")
        delete_doc(args.cleanup)
        print("  ✓ deleted")
        return 0

    print(f"== OData CREATE probe ==\nbase: {BASE}\nsource: {args.source}\n")

    # ── 1. GET образца ────────────────────────────────────────────────
    print("[1/9] GET source")
    src = get_doc(args.source)
    print(
        f"  ✓ Number={src.get('Number')} Date={src.get('Date')} "
        f"Posted={src.get('Posted')} Сумма={src.get('СуммаДокумента')}"
    )
    goods = src.get("Товары") or []
    services = src.get("Услуги") or []
    print(f"  Табличные части: Товары={len(goods)} Услуги={len(services)}")
    if not src.get("Posted"):
        print("  ⚠ Образец НЕ проведён — выбери другой через --source.")
        return 2

    # ── 2. Sanitize ──────────────────────────────────────────────────
    print("\n[2/9] Sanitize payload")
    payload = sanitize(src)
    # Меняем дату на сегодня, чтобы не пересекаться с реальным периодом
    today = datetime.datetime.now().replace(microsecond=0).isoformat()
    payload["Date"] = today
    # Комментарий — явная метка что это тест
    payload["Комментарий"] = f"[TEST] OData create_probe {today}"
    print(f"  ✓ snapshot: {len(payload)} полей верхнего уровня, Date={today}")

    # ── 3. POST на коллекцию ──────────────────────────────────────────
    print("\n[3/9] POST на коллекцию (create)")
    try:
        created = post_collection(payload)
    except requests.HTTPError as e:
        print(f"  ❌ Не удалось создать: {e}")
        return 1
    new_ref = created.get("Ref_Key")
    new_number = created.get("Number")
    print(f"  ✓ создан: Ref_Key={new_ref} Number={new_number}")
    # Лог в файл чтобы при падении скрипта можно было найти и удалить вручную
    pending = REPO / "scripts" / ".odata_create_pending.log"
    pending.write_text(f"{new_ref}\t{new_number}\t{today}\n")
    print(f"  ℹ pending log: {pending}")

    try:
        # ── 4. GET проверка ──────────────────────────────────────────
        print("\n[4/9] GET created (verify Posted=false)")
        fresh = get_doc(new_ref)
        print(
            f"  ✓ Number={fresh.get('Number')} Date={fresh.get('Date')} "
            f"Posted={fresh.get('Posted')} "
            f"DeletionMark={fresh.get('DeletionMark')}"
        )

        # ── 5. Post ──────────────────────────────────────────────────
        print("\n[5/9] POST /Post")
        post_action(new_ref, "Post", operational=False)
        print("  ✓ Post OK")

        # ── 6. GET Posted=true ───────────────────────────────────────
        fresh = get_doc(new_ref)
        print(f"[6/9] Posted={fresh.get('Posted')}")
        if not fresh.get("Posted"):
            print("  ⚠ не провёлся — идём к rollback")

        # ── 7. Unpost ────────────────────────────────────────────────
        print("\n[7/9] POST /Unpost")
        post_action(new_ref, "Unpost")
        print("  ✓ Unpost OK")

        # ── 8. DELETE ────────────────────────────────────────────────
        if args.keep:
            print("\n[8/9] --keep: пропускаем DELETE, документ остался.")
            return 0
        print("\n[8/9] DELETE")
        delete_doc(new_ref)
        print("  ✓ DELETE OK")

        # ── 9. Verify 404 / DeletionMark ─────────────────────────────
        print("\n[9/9] GET (verify gone)")
        try:
            final = get_doc(new_ref)
            dm = final.get("DeletionMark")
            print(f"  Документ всё ещё в базе: DeletionMark={dm}")
            if not dm:
                print("  ⚠ DELETE не отработал как ожидалось")
                return 3
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                print("  ✓ 404 — документ удалён")
            else:
                raise

    except Exception as e:
        print(f"\n🚨 СБОЙ ПОСЛЕ СОЗДАНИЯ: {e}")
        print(f"   Ref_Key={new_ref} остался в базе. Откат:")
        print(f"   python3 scripts/odata_create_probe.py --cleanup {new_ref}")
        return 1
    finally:
        if pending.exists():
            pending.unlink()

    print("\n✅ CREATE probe OK — write-слой работает.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
