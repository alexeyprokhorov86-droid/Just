# email_text_processing.py
from __future__ import annotations

import re
import html as _html
from typing import List, Tuple

try:
    from bs4 import BeautifulSoup
except Exception:
    BeautifulSoup = None  # fallback

# --- HTML -> text ---

def html_to_text(html: str) -> str:
    if not html:
        return ""

    if BeautifulSoup:
        soup = BeautifulSoup(html, "html.parser")
        # удаляем очевидный мусор
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text("\n")
        text = _html.unescape(text)
        return _normalize_whitespace(text)

    # fallback без bs4 (хуже, но лучше чем ничего)
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = _html.unescape(text)
    return _normalize_whitespace(text)


def _normalize_whitespace(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


# --- Очистка email ---

_QUOTED_MARKERS = [
    r"^-{2,}\s*Original Message\s*-{2,}$",
    r"^-----\s*Original Message\s*-----$",
    r"^From:\s.*$",
    r"^Sent:\s.*$",
    r"^To:\s.*$",
    r"^Cc:\s.*$",
    r"^Subject:\s.*$",
    r"^On\s.+wrote:\s*$",
]

_SIGNATURE_MARKERS = [
    r"^--\s*$",
    r"^С уважением[,!].*$",
    r"^С наилучшими пожеланиями[,!].*$",
    r"^Best regards[,!].*$",
    r"^Kind regards[,!].*$",
]

_DISCLAIMER_MARKERS = [
    r"^This email and any attachments.*$",
    r"^CONFIDENTIALITY NOTICE.*$",
]


def clean_email_text(text: str) -> str:
    """
    1) выкидываем цитирование (> ...)
    2) обрезаем тред по маркерам quoted-reply (From:/Sent:/Original Message/On .. wrote:)
    3) обрезаем подпись
    4) пытаемся убрать дисклеймер
    """
    if not text:
        return ""

    text = _normalize_whitespace(text)
    lines = text.split("\n")

    # 1) Удаляем строки цитирования
    lines = [ln for ln in lines if not ln.lstrip().startswith(">")]

    # 2) Обрезаем по quoted markers
    out: List[str] = []
    for ln in lines:
        s = ln.strip()
        if any(re.match(p, s, flags=re.IGNORECASE) for p in _QUOTED_MARKERS):
            break
        out.append(ln)

    # 3) Обрезаем по подписи
    out2: List[str] = []
    for ln in out:
        s = ln.strip()
        if any(re.match(p, s, flags=re.IGNORECASE) for p in _SIGNATURE_MARKERS):
            break
        out2.append(ln)

    # 4) Убираем дисклеймеры (грубо: если встретили — отрезаем)
    final: List[str] = []
    for ln in out2:
        s = ln.strip()
        if any(re.match(p, s, flags=re.IGNORECASE) for p in _DISCLAIMER_MARKERS):
            break
        final.append(ln)

    return _normalize_whitespace("\n".join(final))


# --- Чанкинг ---

def chunk_text(text: str, max_chars: int = 2200, overlap: int = 200) -> List[str]:
    """
    Простой стабильный чанкер по символам (без токенизации).
    Для email этого достаточно как первый шаг.
    """
    text = _normalize_whitespace(text)
    if not text:
        return []

    if len(text) <= max_chars:
        return [text]

    chunks: List[str] = []
    i = 0
    while i < len(text):
        chunk = text[i:i + max_chars]
        chunk = chunk.strip()
        if chunk:
            chunks.append(chunk)
        i += max_chars - overlap

    return chunks


def build_email_chunks(subject: str, body_text: str, body_html: str) -> List[str]:
    """
    Берём body_text (если пусто — делаем text из html),
    чистим, затем чанкаем.
    Subject добавляем в каждый chunk как короткий якорь.
    """
    subj = (subject or "").strip()
    body = (body_text or "").strip()

    if not body and body_html:
        body = html_to_text(body_html)

    body = clean_email_text(body)

    if not body and subj:
        # иногда письмо = только тема
        body = subj

    chunks = chunk_text(body, max_chars=2200, overlap=200)

    # добавляем тему как контекстный якорь
    if subj:
        chunks = [f"Subject: {subj}\n\n{ch}" for ch in chunks]

    return [c for c in chunks if len(c.strip()) >= 10]
