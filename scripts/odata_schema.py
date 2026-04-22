#!/usr/bin/env python3
"""Фаза 0.2 — вытянуть $metadata и разобрать схему Document_ПриобретениеТоваровУслуг.

Сохраняет:
  - docs/odata_metadata.xml (raw)
  - docs/odata_schema_purchase.md (человеко-читаемый разбор)

Запуск: python3 scripts/odata_schema.py
"""

from __future__ import annotations

import os
import pathlib
import xml.etree.ElementTree as ET

import requests
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

REPO = pathlib.Path(__file__).resolve().parent.parent
load_dotenv(REPO / ".env")

BASE = os.environ["ODATA_BASE_URL"].rstrip("/")
AUTH = HTTPBasicAuth(os.environ["ODATA_USERNAME"], os.environ["ODATA_PASSWORD"])

DOCS = REPO / "docs"
DOCS.mkdir(exist_ok=True)
RAW_FILE = DOCS / "odata_metadata.xml"
MD_FILE = DOCS / "odata_schema_purchase.md"

INTERESTED = {
    "StandardODATA.Document_ПриобретениеТоваровУслуг",
    "StandardODATA.Document_ПриобретениеТоваровУслуг_Товары",
    "StandardODATA.Document_ЗаказПоставщику",
    "StandardODATA.Document_ЗаказПоставщику_Товары",
    "StandardODATA.Catalog_СоглашенияСПоставщиками",
    "StandardODATA.Catalog_СерииНоменклатуры",
    "StandardODATA.Catalog_ВидыНоменклатуры",
    "StandardODATA.Catalog_ПрисоединенныеФайлы",
}


def fetch_metadata() -> str:
    print(f"GET {BASE}/$metadata ...")
    r = requests.get(f"{BASE}/$metadata", auth=AUTH, timeout=120)
    r.raise_for_status()
    RAW_FILE.write_text(r.text, encoding="utf-8")
    print(f"  ✓ saved {len(r.text)} bytes → {RAW_FILE}")
    return r.text


def parse_entity_types(xml_text: str) -> dict[str, list[dict]]:
    """Возвращает {FullName: [{'name': ..., 'type': ..., 'nullable': ...}, ...]}."""
    # Удаляем namespace для простоты парсинга
    # ET requires workaround: register namespaces
    root = ET.fromstring(xml_text)
    # Ищем все EntityType под Schema
    ns = {
        "edm": "http://schemas.microsoft.com/ado/2009/11/edm",
        "edmx": "http://schemas.microsoft.com/ado/2007/06/edmx",
    }
    result: dict[str, list[dict]] = {}
    for schema in root.iter("{http://schemas.microsoft.com/ado/2009/11/edm}Schema"):
        ns_name = schema.get("Namespace", "")
        for et in schema.findall("{http://schemas.microsoft.com/ado/2009/11/edm}EntityType"):
            name = et.get("Name")
            full = f"{ns_name}.{name}"
            props = []
            for p in et.findall("{http://schemas.microsoft.com/ado/2009/11/edm}Property"):
                props.append({
                    "name": p.get("Name"),
                    "type": p.get("Type"),
                    "nullable": p.get("Nullable", "true"),
                })
            # NavigationProperty
            navs = []
            for p in et.findall(
                "{http://schemas.microsoft.com/ado/2009/11/edm}NavigationProperty"
            ):
                navs.append({
                    "name": p.get("Name"),
                    "relationship": p.get("Relationship"),
                })
            result[full] = {"props": props, "navs": navs}
    return result


def format_entity(full: str, data: dict) -> str:
    lines = [f"## `{full}`\n", f"Полей: {len(data['props'])}\n"]
    lines.append("| Поле | Тип | Nullable |")
    lines.append("|---|---|---|")
    for p in data["props"]:
        lines.append(f"| `{p['name']}` | `{p['type']}` | {p['nullable']} |")
    if data.get("navs"):
        lines.append(f"\n**Navigation** ({len(data['navs'])}):")
        for n in data["navs"]:
            lines.append(f"- `{n['name']}` → `{n['relationship']}`")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    xml_text = fetch_metadata()
    print("\nParsing…")
    entities = parse_entity_types(xml_text)
    print(f"  ✓ {len(entities)} EntityTypes")

    found = {k: v for k, v in entities.items() if k in INTERESTED}
    missing = INTERESTED - set(found)
    print(f"  targets found: {len(found)}, missing: {len(missing)}")
    for m in missing:
        print(f"    ✗ NOT FOUND: {m}")

    md = [
        "# OData Schema — ключевые сущности для Procurement UPD\n",
        f"Источник: {BASE}/$metadata\n",
        f"Всего EntityTypes в схеме: {len(entities)}\n",
    ]
    for full in sorted(found):
        md.append(format_entity(full, found[full]))
    MD_FILE.write_text("\n".join(md), encoding="utf-8")
    print(f"\n✓ wrote {MD_FILE}")

    # Краткая summary в stdout
    print("\n── Summary props count ──")
    for full in sorted(found):
        print(f"  {full}: {len(found[full]['props'])} props, {len(found[full]['navs'])} navs")

    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
