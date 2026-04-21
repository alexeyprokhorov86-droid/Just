"""
Tool: generate_element_onboarding_video — slideshow с инструкцией по входу в Element X.

Pipeline:
1. Nano Banana (`google/gemini-2.5-flash-image` через RouterAI) генерирует 4 кадра
   каждого шага (app-иконка, выбор сервера, логин, список чатов).
2. Silero TTS (v4_ru, speaker=aidar) озвучивает инструкцию на русском.
3. ffmpeg собирает: каждая картинка 5 сек → concat → наложить аудио.

Результат: mp4 ~20 сек, прикладывается к reminder'ам для not_joined user-ов.

Почему не Playwright/эмулятор: Element X это мобильное приложение, в браузере
адекватно не снимается; Android emulator на VPS требует KVM и много места.
Nano Banana генерирует правдоподобные мобильные screenshots по описанию.
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
from pydantic import BaseModel, Field

from .registry import tool

log = logging.getLogger("tools.element_video")

# Шаги инструкции: (caption для TTS, Nano-Banana prompt).
_STEPS = [
    (
        "Шаг 1. Скачайте приложение Element X из App Store или Google Play Маркет.",
        (
            "A realistic mobile phone screenshot showing the Google Play Store "
            "page for 'Element X' messenger app, with the green INSTALL button "
            "prominently visible. The phone is an Android device in portrait "
            "orientation. The Element X logo (blue hexagonal icon) is visible "
            "at the top. No text overlays, just the native Play Store UI."
        ),
    ),
    (
        "Шаг 2. Откройте приложение, выберите пункт Изменить сервер. Введите адрес: "
        "frumelad точка ру.",
        (
            "A realistic mobile phone screenshot of the Element X messenger "
            "server selection screen. There is an input field labeled 'Homeserver' "
            "with the text 'frumelad.ru' clearly entered. Below is a prominent "
            "blue 'Continue' button. Portrait orientation, dark theme, iOS/Android "
            "neutral modern style. The Element X logo is at the top."
        ),
    ),
    (
        "Шаг 3. Введите логин и пароль, которые вам прислал бот в личных сообщениях.",
        (
            "A realistic mobile phone screenshot of Element X login screen. "
            "Two input fields are visible: 'Username' with a username filled in, "
            "and 'Password' showing dots (masked). A blue 'Sign in' button below. "
            "Portrait orientation, clean modern messenger UI, dark theme."
        ),
    ),
    (
        "Шаг 4. Готово! Откройте пространство Фрумелад — там собраны все рабочие чаты.",
        (
            "A realistic mobile phone screenshot of Element X main chat list screen "
            "showing several chat rooms with Russian names like 'Фрумелад', "
            "'Бухгалтерия', 'Производство'. Portrait orientation, clean messenger "
            "interface with avatars and last-message previews. Dark theme."
        ),
    ),
]


class GenerateElementVideoInput(BaseModel):
    output_path: str = Field(
        default="/home/admin/telegram_logger_bot/assets/element_onboarding.mp4",
        description="Куда сохранить mp4. Директория создаётся если не существует.",
    )
    seconds_per_slide: float = Field(default=5.5, ge=2.0, le=15.0)
    speaker: str = Field(
        default="aidar",
        description="Silero voice: aidar (муж.), baya (жен.), kseniya (жен.), xenia (жен.)",
    )


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
    """Возвращает длительность в секундах."""
    model = _get_silero()
    audio = model.apply_tts(text=text, speaker=speaker, sample_rate=sample_rate)
    sf.write(wav_path, audio.numpy(), sample_rate)
    return len(audio) / sample_rate


def _nano_banana(prompt: str, out_png: str) -> None:
    client = OpenAI(
        api_key=os.getenv("ROUTERAI_API_KEY"),
        base_url=os.getenv("ROUTERAI_BASE_URL", "https://routerai.ru/api/v1"),
    )
    response = client.chat.completions.create(
        model="google/gemini-2.5-flash-image",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=4000,
    )
    msg = response.choices[0].message
    images = getattr(msg, "images", None) or []
    if not images:
        raise RuntimeError(f"Nano Banana не вернула изображение. text={msg.content[:200]}")
    url = images[0].get("image_url", {}).get("url", "")
    # Формат: data:image/png;base64,...
    m = re.match(r"data:image/\w+;base64,(.*)", url)
    if not m:
        raise RuntimeError(f"Не смогли распарсить image url: {url[:80]}")
    png = base64.b64decode(m.group(1))
    with open(out_png, "wb") as f:
        f.write(png)


def _build_mp4(
    slides: list[dict],
    output_path: str,
    seconds_per_slide: float,
) -> None:
    """Собирает slideshow через ffmpeg: каждый image → video N сек, concat → audio."""
    tmpdir = Path(tempfile.mkdtemp(prefix="element_video_"))
    try:
        # Шаг A: каждый PNG → MP4 фиксированной длительности.
        slide_mp4s: list[str] = []
        for i, s in enumerate(slides):
            mp4 = str(tmpdir / f"slide_{i}.mp4")
            # Важно: -pix_fmt yuv420p для совместимости TG video player.
            subprocess.run(
                [
                    "ffmpeg", "-y", "-loglevel", "error",
                    "-loop", "1", "-i", s["image"],
                    "-c:v", "libx264", "-t", f"{seconds_per_slide}",
                    "-pix_fmt", "yuv420p",
                    "-vf", "scale=720:-2:force_original_aspect_ratio=decrease,pad=720:1280:(ow-iw)/2:(oh-ih)/2:color=black",
                    "-r", "25",
                    mp4,
                ],
                check=True,
            )
            slide_mp4s.append(mp4)

        # Шаг B: конкатенация видео-частей.
        concat_list = tmpdir / "concat.txt"
        concat_list.write_text("\n".join(f"file '{p}'" for p in slide_mp4s))
        video_only = str(tmpdir / "video_only.mp4")
        subprocess.run(
            ["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0",
             "-i", str(concat_list), "-c", "copy", video_only],
            check=True,
        )

        # Шаг C: собрать общий аудио-трек (concat wav-файлов), с тишиной в конце слайда.
        audio_concat = tmpdir / "audio_concat.txt"
        audio_files: list[str] = []
        for i, s in enumerate(slides):
            # паддинг тишиной до seconds_per_slide если TTS короче
            padded = str(tmpdir / f"audio_{i}_padded.wav")
            subprocess.run(
                ["ffmpeg", "-y", "-loglevel", "error", "-i", s["audio"],
                 "-af", f"apad=whole_dur={seconds_per_slide}",
                 padded],
                check=True,
            )
            audio_files.append(padded)
        audio_concat.write_text("\n".join(f"file '{p}'" for p in audio_files))
        audio_full = str(tmpdir / "audio_full.wav")
        subprocess.run(
            ["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0",
             "-i", str(audio_concat), "-c", "copy", audio_full],
            check=True,
        )

        # Шаг D: склеить video + audio.
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(
            ["ffmpeg", "-y", "-loglevel", "error", "-i", video_only, "-i", audio_full,
             "-c:v", "copy", "-c:a", "aac", "-shortest", output_path],
            check=True,
        )
    finally:
        # Оставляем tmp для debug; cleanup можно добавить позже.
        log.info(f"element video build tmpdir: {tmpdir}")


@tool(
    name="generate_element_onboarding_video",
    domain="element_video",
    description=(
        "Генерирует обучающее mp4-видео (~22 сек) с 4 шагами подключения к "
        "Element X. Каждый шаг: Nano Banana рисует screenshot + Silero озвучивает "
        "инструкцию на русском. Финальное видео собирается ffmpeg. Путь "
        "сохранения по умолчанию assets/element_onboarding.mp4. Возвращает "
        "{path, slides_count, total_duration_sec, seconds_per_slide}."
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

    for i, (caption, image_prompt) in enumerate(_STEPS):
        img_path = str(tmpdir / f"slide_{i}.png")
        wav_path = str(tmpdir / f"audio_{i}.wav")
        log.info(f"[slide {i+1}/{len(_STEPS)}] Nano Banana…")
        _nano_banana(image_prompt, img_path)
        log.info(f"[slide {i+1}/{len(_STEPS)}] Silero TTS: {caption[:40]}…")
        dur = _tts(caption, wav_path, speaker=speaker)
        slides.append({"image": img_path, "audio": wav_path, "caption": caption, "dur": dur})

    log.info(f"Building mp4 → {output_path}")
    _build_mp4(slides, output_path, seconds_per_slide=seconds_per_slide)

    total = len(_STEPS) * seconds_per_slide
    return {
        "path": output_path,
        "slides_count": len(_STEPS),
        "total_duration_sec": total,
        "seconds_per_slide": seconds_per_slide,
        "tmpdir": str(tmpdir),
    }
