"""
XML handler — универсальный для ЭДО/1С/ФНС документов.

Работает на любой XML-схеме: 1С-реестры зарплат (edi_stnd/109 СчетаПК),
ФНС-УПД, акты, счета-фактуры. Подход: strip BOM, lxml parse, плоская
human-readable сериализация дерева → LLM извлекает реквизиты.

Намеренно НЕ специализируемся на одной схеме: в Априори по факту идут
реестры зарплат от НФ в Райффайзен/Сбер, а не УПД (проверено на 7 файлах
2026-04-21). Специализация под конкретную схему, если нужна, добавляется
сверху через детект root.tag.
"""
from __future__ import annotations

from lxml import etree

from .._prompts import build_analysis_prompt


def _strip_bom(data: bytes) -> bytes:
    if data[:3] == b"\xef\xbb\xbf":
        return data[3:]
    return data


def _detect_encoding(data: bytes) -> str:
    """Угадать кодировку XML: XML-декларация (encoding=...) → utf-8 → cp1251.

    ФНС-файлы часто приходят в windows-1251 без BOM; без этой фоллбэк-логики
    lxml бросает XMLSyntaxError на любой кириллический символ.
    """
    head = data[:200].decode("ascii", errors="ignore").lower()
    if "encoding=" in head:
        # lxml сам разберётся по XML-декларации
        return ""
    # Пробуем utf-8 strict, если падает — cp1251
    try:
        data[:2000].decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        return "windows-1251"


def _clean_tag(tag: str) -> str:
    """Убрать namespace-префикс {ns}Tag → Tag."""
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _flatten(element, lines: list[str], indent: int = 0, max_lines: int = 400) -> None:
    """Human-readable сериализация дерева XML.

    Формат:
      Tag [attr=val, attr2=val2]: text_if_any
        ChildTag [..]: ...

    Лимит строк защищает от мегабайтных XML-ов (например реестров на 1000 строк
    сотрудников — дальше 400 строк LLM уже не прочтёт осмысленно).
    """
    if len(lines) >= max_lines:
        if len(lines) == max_lines:
            lines.append("  " * indent + "... [truncated, see extracted_text for full XML]")
        return

    tag = _clean_tag(element.tag)
    attrs = ", ".join(f"{_clean_tag(k)}={v!r}" for k, v in element.attrib.items())
    text = (element.text or "").strip()

    header = f"{'  ' * indent}{tag}"
    if attrs:
        header += f" [{attrs}]"
    if text:
        header += f": {text}"
    lines.append(header)

    for child in element:
        _flatten(child, lines, indent + 1, max_lines)


def _extract_root_fields(root) -> dict:
    """Вытаскиваем атрибуты корня + тип документа — это почти всегда
    ключевые реквизиты (для СчетаПК: номер реестра, дата, организация, ИНН)."""
    return {
        "root_tag": _clean_tag(root.tag),
        "namespace": root.tag.split("}")[0].lstrip("{") if "}" in root.tag else "",
        "attributes": {_clean_tag(k): v for k, v in root.attrib.items()},
        "children_count": len(list(root)),
    }


def analyze_xml(
    *,
    file_bytes: bytes,
    filename: str,
    chat_context: str,
    gpt_client,
    company_profile: str,
    model: str = "openai/gpt-4.1",
    max_tokens: int = 2000,
) -> dict:
    """Обработать XML-вложение.

    Возвращает: {document_type, extracted_text, structured_fields, summary,
    confidence, errors}. На ошибке парсинга возвращает errors + пустой summary
    (без галлюцинаций).
    """
    errors: list[str] = []
    data = _strip_bom(file_bytes)

    # Если XML без декларации и не UTF-8 — lxml нужна явная перекодировка.
    enc_hint = _detect_encoding(data)
    if enc_hint and enc_hint != "utf-8":
        try:
            data = data.decode(enc_hint).encode("utf-8")
        except UnicodeDecodeError:
            pass

    try:
        root = etree.fromstring(data)
    except etree.XMLSyntaxError as e:
        return {
            "document_type": "xml",
            "extracted_text": data[:5000].decode("utf-8", errors="replace"),
            "structured_fields": {},
            "summary": f"Не удалось распарсить XML: {e}",
            "confidence": 0.0,
            "errors": [f"XMLSyntaxError: {e}"],
        }

    structured = _extract_root_fields(root)

    lines: list[str] = []
    _flatten(root, lines, 0, max_lines=400)
    flat_text = "\n".join(lines)

    # LLM-суммаризация с anti-hallucination промптом.
    summary = ""
    if gpt_client is not None:
        prompt = build_analysis_prompt(
            company_profile=company_profile,
            doc_type=f"XML-документ ({structured['root_tag']}, ЭДО)",
            doc_content=flat_text,
            chat_context=chat_context,
            filename=filename,
        )
        try:
            response = gpt_client.chat.completions.create(
                model=model,
                max_tokens=max_tokens,
                messages=[{"role": "user", "content": prompt}],
            )
            summary = response.choices[0].message.content or ""
        except Exception as e:
            errors.append(f"LLM analysis failed: {e}")
            summary = f"[Не удалось получить LLM-анализ: {e}]"

    return {
        "document_type": "xml",
        "extracted_text": flat_text,
        "structured_fields": structured,
        "summary": summary,
        "confidence": 1.0 if summary and not errors else 0.3,
        "errors": errors,
    }
