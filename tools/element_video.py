"""
Tool: generate_element_onboarding_video — slideshow с инструкцией по входу в Element X.

v2 (2026-04-21): Nano Banana оказалась слабой в генерации кириллицы на
изображениях (Бухгалерия/Произгвотво/Сосаловано). Теперь:
  * Slide 1 и 4 — полностью через PIL (dark-theme schematic UI с
    минималистичной графикой + кириллический текст через DejaVu Sans).
  * Slide 2 и 3 — Nano Banana для базового UI (реалистичный скриншот
    Element X login/server screens на английском), текст frumelad.ru
    накладывается PIL поверх координат.

Silero TTS (v4_ru, speaker=aidar) озвучивает инструкцию на русском,
ffmpeg собирает: каждая картинка 5.5 сек → concat → audio overlay.
"""
from __future__ import annotations

import base64
import logging
import os
import re
import subprocess
import tempfile
from pathlib import Path

import soundfile as sf
import torch
from openai import OpenAI
from PIL import Image, ImageDraw, ImageFont
from pydantic import BaseModel, Field

from .registry import tool

log = logging.getLogger("tools.element_video")

_FONT_REG = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf"
_FONT_BOLD = "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"

# Цвета Element-style темы
_BG = "#17191c"
_CARD = "#212327"
_FG = "#ffffff"
_MUTED = "#8e9297"
_ACCENT = "#0dbd8b"  # Element green
_BLUE = "#1f6bff"

# Финальный размер видео 720×1280 (9:16 portrait).
_W, _H = 720, 1280

_STEPS_CAPTIONS = [
    "Шаг 1. Скачайте приложение Element X из магазина App Store или Google Play.",
    "Шаг 2. Откройте приложение, выберите пункт Изменить сервер. Введите адрес: фрумелад точка ру.",
    "Шаг 3. Введите логин и пароль, которые прислал вам бот в личных сообщениях.",
    "Шаг 4. Готово! Откройте пространство Фрумелад — там собраны все рабочие чаты.",
]


# ──────────────────────────────────────────────────────────────────────
# SLIDE GENERATORS
# ──────────────────────────────────────────────────────────────────────

def _font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    return ImageFont.truetype(_FONT_BOLD if bold else _FONT_REG, size)


def _text_w(draw: ImageDraw.ImageDraw, text: str, font: ImageFont.FreeTypeFont) -> int:
    bbox = draw.textbbox((0, 0), text, font=font)
    return bbox[2] - bbox[0]


def _slide_1_download(path: str) -> None:
    """Слайд 1 — призыв скачать Element X. Полностью PIL."""
    img = Image.new("RGB", (_W, _H), _BG)
    d = ImageDraw.Draw(img)

    # Шапка — большой шаг
    d.text((_W // 2 - _text_w(d, "Шаг 1", _font(52, bold=True)) // 2, 120),
           "Шаг 1", fill=_ACCENT, font=_font(52, bold=True))
    d.text((_W // 2 - _text_w(d, "Скачайте Element X", _font(44, bold=True)) // 2, 200),
           "Скачайте Element X", fill=_FG, font=_font(44, bold=True))

    # Element X логотип (схематичная стилизованная E)
    cx, cy = _W // 2, 500
    r = 90
    d.rounded_rectangle(
        (cx - r, cy - r, cx + r, cy + r),
        radius=28, fill=_BLUE,
    )
    # Буква E белым
    d.text((cx - _text_w(d, "E", _font(140, bold=True)) // 2, cy - 100),
           "E", fill=_FG, font=_font(140, bold=True))

    # Две кнопки магазинов
    btn_y = 800
    btn_w, btn_h = 260, 100
    left_x = (_W - btn_w * 2 - 40) // 2

    # Google Play
    d.rounded_rectangle(
        (left_x, btn_y, left_x + btn_w, btn_y + btn_h),
        radius=16, fill=_CARD, outline=_MUTED, width=2,
    )
    d.text((left_x + 30, btn_y + 20), "▶", fill=_ACCENT, font=_font(48, bold=True))
    d.text((left_x + 80, btn_y + 24), "Google", fill=_MUTED, font=_font(22))
    d.text((left_x + 80, btn_y + 50), "Play", fill=_FG, font=_font(28, bold=True))

    # App Store
    ax = left_x + btn_w + 40
    d.rounded_rectangle(
        (ax, btn_y, ax + btn_w, btn_y + btn_h),
        radius=16, fill=_CARD, outline=_MUTED, width=2,
    )
    d.text((ax + 30, btn_y + 20), "", fill=_FG, font=_font(48))
    d.text((ax + 80, btn_y + 24), "App", fill=_MUTED, font=_font(22))
    d.text((ax + 80, btn_y + 50), "Store", fill=_FG, font=_font(28, bold=True))

    # Подпись внизу
    caption = "Найдите «Element X» и нажмите «Установить»"
    d.text((_W // 2 - _text_w(d, caption, _font(26)) // 2, 1020),
           caption, fill=_MUTED, font=_font(26))

    img.save(path)


def _nano_banana_if_possible(prompt: str, fallback: callable, out_path: str) -> None:
    """Попытка Nano Banana; при неудаче — fallback PIL."""
    try:
        client = OpenAI(
            api_key=os.getenv("ROUTERAI_API_KEY"),
            base_url=os.getenv("ROUTERAI_BASE_URL", "https://routerai.ru/api/v1"),
        )
        r = client.chat.completions.create(
            model="google/gemini-2.5-flash-image",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=4000,
        )
        images = getattr(r.choices[0].message, "images", None) or []
        if not images:
            raise RuntimeError("no image in response")
        url = images[0].get("image_url", {}).get("url", "")
        m = re.match(r"data:image/\w+;base64,(.*)", url)
        if not m:
            raise RuntimeError("bad url format")
        png = base64.b64decode(m.group(1))
        with open(out_path, "wb") as f:
            f.write(png)
        # Нормализуем размер
        img = Image.open(out_path).convert("RGB")
        img = _resize_with_padding(img, _W, _H, _BG)
        img.save(out_path)
    except Exception as e:
        log.warning(f"Nano Banana failed, using PIL fallback: {e}")
        fallback(out_path)


def _resize_with_padding(img: Image.Image, target_w: int, target_h: int, pad_color: str) -> Image.Image:
    """Вписать изображение в target_w×target_h с padding пад-цветом."""
    ratio = min(target_w / img.width, target_h / img.height)
    new_w, new_h = int(img.width * ratio), int(img.height * ratio)
    img = img.resize((new_w, new_h), Image.LANCZOS)
    bg = Image.new("RGB", (target_w, target_h), pad_color)
    bg.paste(img, ((target_w - new_w) // 2, (target_h - new_h) // 2))
    return bg


def _slide_2_server(path: str) -> None:
    """Слайд 2 — экран выбора сервера. PIL рисует UI, frumelad.ru в поле ввода."""
    img = Image.new("RGB", (_W, _H), _BG)
    d = ImageDraw.Draw(img)

    # Шапка
    d.text((_W // 2 - _text_w(d, "Шаг 2", _font(52, bold=True)) // 2, 80),
           "Шаг 2", fill=_ACCENT, font=_font(52, bold=True))
    d.text((_W // 2 - _text_w(d, "Адрес сервера", _font(40, bold=True)) // 2, 160),
           "Адрес сервера", fill=_FG, font=_font(40, bold=True))

    # Условное phone frame
    px, py, pw, ph = 90, 280, 540, 900
    d.rounded_rectangle((px, py, px + pw, py + ph), radius=48, fill=_CARD)

    # Лого Element (E)
    logo_y = py + 60
    d.text((_W // 2 - _text_w(d, "E", _font(84, bold=True)) // 2, logo_y),
           "E", fill=_BLUE, font=_font(84, bold=True))
    d.text((_W // 2 - _text_w(d, "Element X", _font(34, bold=True)) // 2, logo_y + 110),
           "Element X", fill=_FG, font=_font(34, bold=True))

    # Label
    d.text((px + 40, logo_y + 200), "Homeserver", fill=_MUTED, font=_font(22))

    # Input field с frumelad.ru
    input_y = logo_y + 240
    d.rounded_rectangle(
        (px + 40, input_y, px + pw - 40, input_y + 90),
        radius=14, fill=_BG, outline=_ACCENT, width=3,
    )
    server_text = "frumelad.ru"
    d.text((px + 60, input_y + 26), server_text, fill=_FG, font=_font(34, bold=True))

    # Continue button
    btn_y = input_y + 130
    d.rounded_rectangle(
        (px + 40, btn_y, px + pw - 40, btn_y + 90),
        radius=14, fill=_BLUE,
    )
    btn_txt = "CONTINUE"
    d.text((_W // 2 - _text_w(d, btn_txt, _font(30, bold=True)) // 2, btn_y + 28),
           btn_txt, fill=_FG, font=_font(30, bold=True))

    # Hint
    hint = "Нажмите «Изменить сервер», введите frumelad.ru"
    d.text((_W // 2 - _text_w(d, hint, _font(22)) // 2, py + ph + 30),
           hint, fill=_MUTED, font=_font(22))

    img.save(path)


def _slide_3_login(path: str) -> None:
    """Слайд 3 — логин/пароль. PIL."""
    img = Image.new("RGB", (_W, _H), _BG)
    d = ImageDraw.Draw(img)

    d.text((_W // 2 - _text_w(d, "Шаг 3", _font(52, bold=True)) // 2, 80),
           "Шаг 3", fill=_ACCENT, font=_font(52, bold=True))
    d.text((_W // 2 - _text_w(d, "Логин и пароль", _font(40, bold=True)) // 2, 160),
           "Логин и пароль", fill=_FG, font=_font(40, bold=True))

    px, py, pw, ph = 90, 280, 540, 900
    d.rounded_rectangle((px, py, px + pw, py + ph), radius=48, fill=_CARD)

    d.text((_W // 2 - _text_w(d, "Element X", _font(32, bold=True)) // 2, py + 70),
           "Element X", fill=_FG, font=_font(32, bold=True))
    d.text((_W // 2 - _text_w(d, "Вход", _font(40, bold=True)) // 2, py + 180),
           "Вход", fill=_FG, font=_font(40, bold=True))

    # Username field
    f_y = py + 290
    d.text((px + 40, f_y), "Имя пользователя", fill=_MUTED, font=_font(22))
    d.rounded_rectangle(
        (px + 40, f_y + 30, px + pw - 40, f_y + 110),
        radius=14, fill=_BG, outline=_MUTED, width=2,
    )
    d.text((px + 60, f_y + 52), "aleksandranisimov", fill=_FG, font=_font(28))

    # Password field
    p_y = f_y + 170
    d.text((px + 40, p_y), "Пароль", fill=_MUTED, font=_font(22))
    d.rounded_rectangle(
        (px + 40, p_y + 30, px + pw - 40, p_y + 110),
        radius=14, fill=_BG, outline=_MUTED, width=2,
    )
    d.text((px + 60, p_y + 52), "• • • • • • • • • •", fill=_FG, font=_font(28, bold=True))

    # Sign in
    btn_y = p_y + 170
    d.rounded_rectangle(
        (px + 40, btn_y, px + pw - 40, btn_y + 90),
        radius=14, fill=_BLUE,
    )
    d.text((_W // 2 - _text_w(d, "Войти", _font(30, bold=True)) // 2, btn_y + 28),
           "Войти", fill=_FG, font=_font(30, bold=True))

    hint = "Данные для входа: напишите боту /element"
    d.text((_W // 2 - _text_w(d, hint, _font(22)) // 2, py + ph + 30),
           hint, fill=_MUTED, font=_font(22))

    img.save(path)


def _slide_4_chats(path: str) -> None:
    """Слайд 4 — список чатов с правильной кириллицей. PIL."""
    img = Image.new("RGB", (_W, _H), _BG)
    d = ImageDraw.Draw(img)

    d.text((_W // 2 - _text_w(d, "Шаг 4", _font(52, bold=True)) // 2, 80),
           "Шаг 4", fill=_ACCENT, font=_font(52, bold=True))
    d.text((_W // 2 - _text_w(d, "Готово — все чаты на месте", _font(34, bold=True)) // 2, 160),
           "Готово — все чаты на месте", fill=_FG, font=_font(34, bold=True))

    # Top navbar
    nav_y = 240
    d.rounded_rectangle((60, nav_y, _W - 60, nav_y + 80), radius=12, fill=_CARD)
    d.text((90, nav_y + 22), "Пространство «Фрумелад»", fill=_FG, font=_font(28, bold=True))

    # Список чатов с реальной русской кириллицей.
    chats = [
        ("#0dbd8b", "Фрумелад", "Завтра в 10:00 общий сбор", "10:30"),
        ("#ff9500", "Бухгалтерия", "Отчёты за квартал сданы", "10:29"),
        ("#ffcc00", "Производство", "Новая партия готова к отгрузке", "10:25"),
        ("#af52de", "Закупки", "Сахар от Агросервер — OK", "10:20"),
        ("#5856d6", "Отгрузки", "Машина вышла на клиента", "10:15"),
        ("#ff3b30", "HR-Фрумелад/НФ", "Новый договор на подпись", "10:10"),
    ]
    row_y = 350
    row_h = 130
    for color, title, preview, ts in chats:
        # Card
        d.rounded_rectangle((60, row_y, _W - 60, row_y + row_h - 10), radius=14, fill=_CARD)
        # Avatar circle
        ax, ay, ar = 105, row_y + 30, 30
        d.ellipse((ax - ar, ay, ax + ar, ay + ar * 2), fill=color)
        initial = title[0]
        d.text((ax - _text_w(d, initial, _font(32, bold=True)) // 2, ay + 10),
               initial, fill=_FG, font=_font(32, bold=True))
        # Title + preview
        d.text((165, row_y + 20), title, fill=_FG, font=_font(26, bold=True))
        d.text((165, row_y + 60), preview, fill=_MUTED, font=_font(20))
        # Timestamp справа
        ts_w = _text_w(d, ts, _font(20))
        d.text((_W - 80 - ts_w, row_y + 22), ts, fill=_MUTED, font=_font(20))
        row_y += row_h

    img.save(path)


_SLIDE_BUILDERS = [_slide_1_download, _slide_2_server, _slide_3_login, _slide_4_chats]


# ──────────────────────────────────────────────────────────────────────
# TTS
# ──────────────────────────────────────────────────────────────────────

_silero_model = None


def _get_silero():
    global _silero_model
    if _silero_model is None:
        log.info("Loading Silero TTS ru v4…")
        _silero_model, _ = torch.hub.load(
            repo_or_dir="snakers4/silero-models",
            model="silero_tts",
            language="ru",
            speaker="v4_ru",
            trust_repo=True,
        )
    return _silero_model


def _tts(text: str, wav_path: str, speaker: str, sample_rate: int = 48000) -> float:
    model = _get_silero()
    audio = model.apply_tts(text=text, speaker=speaker, sample_rate=sample_rate)
    sf.write(wav_path, audio.numpy(), sample_rate)
    return len(audio) / sample_rate


# ──────────────────────────────────────────────────────────────────────
# ASSEMBLY
# ──────────────────────────────────────────────────────────────────────

def _build_mp4(slides: list[dict], output_path: str, seconds_per_slide: float) -> None:
    tmpdir = Path(tempfile.mkdtemp(prefix="element_video_"))
    try:
        slide_mp4s: list[str] = []
        for i, s in enumerate(slides):
            mp4 = str(tmpdir / f"slide_{i}.mp4")
            subprocess.run(
                [
                    "ffmpeg", "-y", "-loglevel", "error",
                    "-loop", "1", "-i", s["image"],
                    "-c:v", "libx264", "-t", f"{seconds_per_slide}",
                    "-pix_fmt", "yuv420p",
                    "-vf", f"scale={_W}:{_H}:force_original_aspect_ratio=decrease,pad={_W}:{_H}:(ow-iw)/2:(oh-ih)/2:color=black",
                    "-r", "25",
                    mp4,
                ], check=True,
            )
            slide_mp4s.append(mp4)

        concat_list = tmpdir / "concat.txt"
        concat_list.write_text("\n".join(f"file '{p}'" for p in slide_mp4s))
        video_only = str(tmpdir / "video_only.mp4")
        subprocess.run(
            ["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0",
             "-i", str(concat_list), "-c", "copy", video_only], check=True,
        )

        audio_concat = tmpdir / "audio_concat.txt"
        audio_files: list[str] = []
        for i, s in enumerate(slides):
            padded = str(tmpdir / f"audio_{i}_padded.wav")
            subprocess.run(
                ["ffmpeg", "-y", "-loglevel", "error", "-i", s["audio"],
                 "-af", f"apad=whole_dur={seconds_per_slide}", padded], check=True,
            )
            audio_files.append(padded)
        audio_concat.write_text("\n".join(f"file '{p}'" for p in audio_files))
        audio_full = str(tmpdir / "audio_full.wav")
        subprocess.run(
            ["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0",
             "-i", str(audio_concat), "-c", "copy", audio_full], check=True,
        )

        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["ffmpeg", "-y", "-loglevel", "error", "-i", video_only, "-i", audio_full,
             "-c:v", "copy", "-c:a", "aac", "-shortest", output_path], check=True,
        )
    finally:
        log.info(f"element video build tmpdir: {tmpdir}")


# ──────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ──────────────────────────────────────────────────────────────────────

class GenerateElementVideoInput(BaseModel):
    output_path: str = Field(
        default="/home/admin/telegram_logger_bot/assets/element_onboarding.mp4",
    )
    seconds_per_slide: float = Field(default=5.5, ge=2.0, le=15.0)
    speaker: str = Field(default="aidar")


@tool(
    name="generate_element_onboarding_video",
    domain="element_video",
    description=(
        "Генерирует обучающее mp4 (~22 сек) с 4 шагами подключения к Element X. "
        "v2: рендер через PIL (контроль орфографии русского текста — ключевое, "
        "Nano Banana 2.5 не умеет кириллицу). Silero TTS озвучивает на русском, "
        "ffmpeg склеивает slideshow."
    ),
    input_model=GenerateElementVideoInput,
)
def generate_element_onboarding_video(
    output_path: str,
    seconds_per_slide: float,
    speaker: str,
) -> dict:
    tmpdir = Path(tempfile.mkdtemp(prefix="element_vid_src_"))
    slides: list[dict] = []

    for i, (caption, builder) in enumerate(zip(_STEPS_CAPTIONS, _SLIDE_BUILDERS)):
        img_path = str(tmpdir / f"slide_{i}.png")
        wav_path = str(tmpdir / f"audio_{i}.wav")
        log.info(f"[slide {i+1}/{len(_STEPS_CAPTIONS)}] PIL render…")
        builder(img_path)
        log.info(f"[slide {i+1}/{len(_STEPS_CAPTIONS)}] Silero TTS…")
        dur = _tts(caption, wav_path, speaker=speaker)
        slides.append({"image": img_path, "audio": wav_path, "caption": caption, "dur": dur})

    log.info(f"Building mp4 → {output_path}")
    _build_mp4(slides, output_path, seconds_per_slide=seconds_per_slide)

    return {
        "path": output_path,
        "slides_count": len(_STEPS_CAPTIONS),
        "total_duration_sec": len(_STEPS_CAPTIONS) * seconds_per_slide,
        "seconds_per_slide": seconds_per_slide,
        "tmpdir": str(tmpdir),
    }
