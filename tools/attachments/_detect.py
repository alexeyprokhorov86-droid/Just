"""
Magic-byte детектор формата вложения.

Никогда не доверяем расширению файла / mime_type от Telegram — отправитель
мог прислать XML с именем УПД.pdf, или PDF без расширения. Магические байты
показывают что реально лежит в файле.
"""
from __future__ import annotations

from typing import Literal

Format = Literal[
    "pdf",
    "xml_upd",       # XML-УПД (ФНС-схема) или просто XML от ЭДО
    "image_jpeg",
    "image_png",
    "image_webp",
    "image_gif",
    "zip_ooxml",     # docx/xlsx/pptx/любой ZIP — дальше по имени/содержимому
    "ole_legacy",    # старые .doc/.xls (CFB format)
    "unknown",
]


def detect_format(data: bytes) -> Format:
    """Определить формат по первым байтам. Никаких предположений по имени."""
    if len(data) < 4:
        return "unknown"

    # PDF
    if data[:4] == b"%PDF":
        return "pdf"

    # XML (с BOM или без). ЭДО-УПД всегда с BOM UTF-8.
    if data[:5] == b"<?xml" or (data[:3] == b"\xef\xbb\xbf" and data[3:8] == b"<?xml"):
        return "xml_upd"

    # Images
    if data[:3] == b"\xff\xd8\xff":
        return "image_jpeg"
    if data[:8] == b"\x89PNG\r\n\x1a\n":
        return "image_png"
    if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image_webp"
    if data[:6] in (b"GIF87a", b"GIF89a"):
        return "image_gif"

    # ZIP — может быть docx/xlsx/pptx (OOXML). Точный тип определяется по
    # содержимому, но для диспатча tool'а достаточно знать что это ZIP.
    if data[:4] == b"PK\x03\x04":
        return "zip_ooxml"

    # OLE Compound File (legacy .doc/.xls)
    if data[:8] == b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1":
        return "ole_legacy"

    return "unknown"


def mime_for_format(fmt: Format) -> str:
    """Канонический mime type по распознанному формату."""
    return {
        "pdf": "application/pdf",
        "xml_upd": "application/xml",
        "image_jpeg": "image/jpeg",
        "image_png": "image/png",
        "image_webp": "image/webp",
        "image_gif": "image/gif",
        "zip_ooxml": "application/zip",
        "ole_legacy": "application/octet-stream",
        "unknown": "application/octet-stream",
    }[fmt]
