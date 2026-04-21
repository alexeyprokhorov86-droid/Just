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

    # XML: несколько паттернов которые встречаются в реальных ЭДО-файлах
    #   - <?xml (стандарт)
    #   - BOM + <?xml (1С СчетаПК edi_stnd/109)
    #   - \r\n<Файл ... (ФНС XML без декларации, часто windows-1251)
    #   - <Файл ... (тот же ФНС, без CRLF)
    head = data[:256]
    if head[:5] == b"<?xml" or (head[:3] == b"\xef\xbb\xbf" and head[3:8] == b"<?xml"):
        return "xml_upd"
    # ФНС-паттерн: корневой <Файл ...> (cp1251 байты: d0a4d0b0d0b9d0bb = «Файл» в UTF-8,
    # либо d4e0e9eb в cp1251). Детектим обе кодировки.
    stripped = head.lstrip(b"\r\n\t ")
    if stripped.startswith(b"<\xd0\xa4\xd0\xb0\xd0\xb9\xd0\xbb") or stripped.startswith(b"<\xd4\xe0\xe9\xeb"):
        return "xml_upd"
    # Любой XML начинающийся с < и содержащий xmlns= в первых 256 байтах —
    # агрессивный фоллбек (xmlns может быть только в XML).
    if stripped.startswith(b"<") and b"xmlns" in head:
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
