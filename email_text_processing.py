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

# --- Quality checks (decoding issues) ---

REPLACEMENT_CHAR = "�"  # U+FFFD

def replacement_ratio(text: str) -> float:
    """
    Доля replacement-символов (�) в тексте.
    Если декодирование прошло с errors='replace', вместо битых байтов появляется �.
    """
    if not text:
        return 0.0
    return text.count(REPLACEMENT_CHAR) / max(len(text), 1)

def is_text_decoding_bad(text: str, max_ratio: float = 0.002, min_count: int = 3) -> bool:
    """
    Определяет, что текст подозрительно "битый".

    Порог по умолчанию:
    - max_ratio=0.002 -> 0.2% символов заменены на �
    - min_count=3     -> чтобы не реагировать на 1 случайный символ

    Для коротких email это адекватно:
    3+ replacement-символа почти всегда означает проблему кодировки.
    """
    if not text:
        return False
    cnt = text.count(REPLACEMENT_CHAR)
    if cnt < min_count:
        return False
    return (cnt / max(len(text), 1)) > max_ratio


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
    1) берём body_text (если пусто — text из html)
    2) чистим quoted replies/подписи/дисклеймеры
    3) чанкаем
    4) добавляем Subject как якорь ТОЛЬКО если subject не битый
    5) фильтруем чанки с сильными признаками битого декодирования (�)
    6) fallback: если всё плохое — индексируем только Subject, если он нормальный
    """
    subj = (subject or "").strip()
    body = (body_text or "").strip()

    # если нет plain-текста — берём из HTML
    if not body and body_html:
        body = html_to_text(body_html)

    # чистим тело письма
    body = clean_email_text(body)

    # если письмо фактически пустое — пробуем хотя бы subject
    if not body and subj:
        body = subj

    # чанки тела (без subject)
    chunks = chunk_text(body, max_chars=2200, overlap=200)

    # решаем, можно ли использовать subject как якорь
    subject_ok = bool(subj) and (not is_text_decoding_bad(subj))

    if subject_ok:
        chunks = [f"Subject: {subj}\n\n{ch}" for ch in chunks]
    else:
        # если subject битый — лучше не добавлять его в чанки
        # иначе он будет портить embedding даже у хорошего body
        pass

    # выкидываем слишком короткие чанки
    chunks = [c for c in chunks if len(c.strip()) >= 10]

    # фильтрация чанков с явной порчей декодирования
    good = [c for c in chunks if not is_text_decoding_bad(c)]

    if good:
        return good

    # fallback: если все чанки плохие, но subject нормальный — индексируем только subject
    if subject_ok:
        return [f"Subject: {subj}"]

    # иначе лучше не индексировать вообще
    return []

