#!/usr/bin/env python3
"""Фаза 0.1 — OData write smoke-test.

Берём свежий проведённый ПТУ (НФ00-000531, 3500 руб, 2026-04-22), пробуем:
  1. GET — прочитать (проверка read).
  2. POST /Unpost — распровести.
  3. GET — убедиться что posted=false.
  4. POST /Post?PostingModeOperational=false — провести обратно.
  5. GET — убедиться что posted=true.

После прогона документ в исходном состоянии. Если Post на шаге 4 упадёт —
документ останется непроведённым, нужен ручной Post в 1С.

Запуск: python3 scripts/odata_probe.py
Флаги: --dry-run (только шаг 1), --ref <guid> (другой документ).
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import sys
import time

import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

REPO = pathlib.Path(__file__).resolve().parent.parent
load_dotenv(REPO / ".env")

BASE = os.environ["ODATA_BASE_URL"].rstrip("/")
AUTH = HTTPBasicAuth(os.environ["ODATA_USERNAME"], os.environ["ODATA_PASSWORD"])
DOC = "Document_ПриобретениеТоваровУслуг"

# Дефолт — маленький свежий ПТУ НФ00-000531 на 3500 руб.
DEFAULT_REF = "414b838f-3e4c-11f1-8e2f-000c299cc968"


def _url(path: str) -> str:
    return f"{BASE}/{path}"


def odata_get(ref: str) -> dict:
    r = requests.get(
        _url(f"{DOC}(guid'{ref}')"),
        params={"$format": "json"},
        auth=AUTH,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def odata_unpost(ref: str) -> dict:
    r = requests.post(
        _url(f"{DOC}(guid'{ref}')/Unpost"),
        params={"$format": "json"},
        auth=AUTH,
        timeout=60,
    )
    r.raise_for_status()
    return r.json() if r.text.strip() else {"ok": True}


def odata_post_doc(ref: str, operational: bool = False) -> dict:
    r = requests.post(
        _url(f"{DOC}(guid'{ref}')/Post"),
        params={
            "$format": "json",
            "PostingModeOperational": str(operational).lower(),
        },
        auth=AUTH,
        timeout=60,
    )
    r.raise_for_status()
    return r.json() if r.text.strip() else {"ok": True}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ref", default=DEFAULT_REF, help="Ref_Key ПТУ")
    ap.add_argument("--dry-run", action="store_true", help="Только GET")
    args = ap.parse_args()

    ref = args.ref
    print(f"== OData probe ==\nbase: {BASE}\nref: {ref}\n")

    # ── Шаг 1: GET ───────────────────────────────────────────────────
    print("[1/5] GET", DOC, f"(guid'{ref}')")
    try:
        data = odata_get(ref)
    except Exception as e:
        print(f"  ❌ GET failed: {e}")
        return 1
    print(
        f"  ✓ Ref_Key={data.get('Ref_Key')} "
        f"Number={data.get('Number')} "
        f"Date={data.get('Date')} "
        f"Posted={data.get('Posted')} "
        f"СуммаДокумента={data.get('СуммаДокумента')}"
    )
    posted_initial = data.get("Posted")
    if not posted_initial:
        print("  ⚠ Документ НЕ проведён изначально — smoke-test требует проведённый. Abort.")
        return 2

    if args.dry_run:
        print("\n--dry-run: stop after GET.")
        return 0

    # ── Шаг 2: Unpost ────────────────────────────────────────────────
    print("\n[2/5] POST /Unpost")
    try:
        resp = odata_unpost(ref)
    except Exception as e:
        print(f"  ❌ Unpost failed: {e}")
        return 1
    print(f"  ✓ response: {json.dumps(resp, ensure_ascii=False)[:200]}")

    # ── Шаг 3: Verify Unpost ─────────────────────────────────────────
    print("\n[3/5] GET (verify Posted=false)")
    data = odata_get(ref)
    posted_after_unpost = data.get("Posted")
    print(f"  Posted={posted_after_unpost}")
    if posted_after_unpost:
        print("  ⚠ Unpost не сработал (Posted всё ещё true). Продолжаем к Post на всякий.")

    # ── Шаг 4: Post back ─────────────────────────────────────────────
    print("\n[4/5] POST /Post?PostingModeOperational=false")
    try:
        resp = odata_post_doc(ref, operational=False)
    except Exception as e:
        print(f"  ❌ Post failed: {e}")
        print(f"  🚨 Документ остался непроведённым — ручной Post в 1С!")
        return 1
    print(f"  ✓ response: {json.dumps(resp, ensure_ascii=False)[:200]}")

    # ── Шаг 5: Verify final ──────────────────────────────────────────
    print("\n[5/5] GET (verify Posted=true)")
    data = odata_get(ref)
    posted_final = data.get("Posted")
    print(f"  Posted={posted_final}")
    if not posted_final:
        print("  🚨 Post не провёл документ обратно — ручной Post в 1С!")
        return 1

    print("\n✅ OData write smoke-test OK. Документ в исходном состоянии.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
