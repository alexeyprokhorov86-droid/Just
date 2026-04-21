"""
Video handler — транскрипт + adaptive frame sampling + LLM summary.

Цепочка:
  1. Whisper → транскрипт (основа для talking-heads видео).
  2. `_frame_sampler.probe` → scene-detection + опорные кадры.
  3. LLM-classifier: смотрит probe-set + кусок транскрипта → вердикт
     static/mixed/dynamic + диапазоны для углубления.
  4. Если mixed/dynamic — `_frame_sampler.deep_scan` в hot-диапазонах
     (cap общий 200 кадров).
  5. Final summary: full transcript + до FINAL_FRAMES_CAP кадров (равномерно
     subsample если больше), anti-hallucination prompt.

Работает с **путём к файлу**, не с bytes — длинное видео (2 GB) в RAM не
хранится. Временные кадры живут в tempdir, очищаются после.
"""
from __future__ import annotations

import base64
import json
import logging
import os
import shutil
import subprocess
import tempfile

from .. import _frame_sampler as sampler
from .._prompts import ANTI_HALLUCINATION_HEADER, build_video_prompt

log = logging.getLogger("tools.attachments.video")

FINAL_FRAMES_CAP = 40            # сколько кадров максимум идёт в финальный LLM-call
TRANSCRIPT_MAX_CHARS = 80000     # транскрипт может быть длинным (3-часовое совещание ~50k)
CLASSIFIER_TRANSCRIPT_HEAD = 3000  # сколько транскрипта видит классификатор
WHISPER_MODEL = "base"           # см. bot.extract_transcript_from_audio
FINAL_MAX_TOKENS = 8000          # потолок ответа LLM (для детального конспекта 30-мин совещания)


def _extract_audio_wav(video_path: str, wav_path: str) -> bool:
    cmd = [
        "ffmpeg", "-v", "error", "-y",
        "-i", video_path,
        "-vn", "-acodec", "pcm_s16le",
        "-ar", "16000", "-ac", "1",
        wav_path,
    ]
    try:
        subprocess.run(cmd, check=True, capture_output=True, timeout=3600)
        return os.path.exists(wav_path) and os.path.getsize(wav_path) > 0
    except Exception as e:
        log.warning("audio extract failed: %s", e)
        return False


def _whisper_transcript(video_path: str) -> str:
    """Транскрипт через Whisper base, ru. Возвращает '' при ошибке."""
    wav_path = video_path + ".wav"
    try:
        if not _extract_audio_wav(video_path, wav_path):
            return ""
        import whisper
        model = whisper.load_model(WHISPER_MODEL)
        result = model.transcribe(wav_path, language="ru", fp16=False)
        transcript = (result.get("text") or "").strip()
        log.info("whisper: %d символов", len(transcript))
        return transcript
    except Exception as e:
        log.warning("whisper failed: %s", e)
        return ""
    finally:
        if os.path.exists(wav_path):
            try:
                os.unlink(wav_path)
            except OSError:
                pass


def _b64_image(path: str) -> str | None:
    try:
        with open(path, "rb") as f:
            return base64.standard_b64encode(f.read()).decode()
    except OSError as e:
        log.debug("read frame failed %s: %s", path, e)
        return None


def _classify(
    probe_frames: list[sampler.Frame],
    transcript_head: str,
    duration_sec: float,
    gpt_client,
    model: str,
) -> dict:
    """LLM классифицирует probe-set. Возвращает:
    {
        "visual_density": "static"|"mixed"|"dynamic",
        "need_deeper_scan_ranges": [{"start": sec, "end": sec, "reason": str}],
        "note": str,
    }
    """
    if not probe_frames or gpt_client is None:
        return {"visual_density": "static", "need_deeper_scan_ranges": [], "note": "no probe frames"}

    content = []
    for f in probe_frames:
        b64 = _b64_image(f.path)
        if b64 is None:
            continue
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
        })
    content.append({
        "type": "text",
        "text": (
            f"Это probe-кадры видео длиной {duration_sec:.0f} сек, отобранные "
            f"scene-detection + равномерно. Таймстемпы (в сек): "
            f"{[round(f.timestamp_sec, 1) for f in probe_frames]}. "
            f"Кусок транскрипта аудио (начало):\n\n"
            f"{transcript_head}\n\n"
            "Классифицируй визуальную плотность видео:\n"
            "- 'static' — talking heads, совещание, собеседование, экран с текстом; "
            "кадры почти не меняются, визуал не добавит смысла к аудио-транскрипту.\n"
            "- 'mixed' — чередуются разговорные участки и показ чего-то (демо, "
            "презентация с переключением слайдов, экскурсия).\n"
            "- 'dynamic' — производство, замер, настройка оборудования, динамичные "
            "события; визуальные детали критичны, звук второстепенен.\n\n"
            "Если 'mixed' или 'dynamic' — укажи временные диапазоны (start_sec, "
            "end_sec) где нужно больше кадров; 1-3 диапазона, каждый <= 10 минут. "
            "Для 'static' — пустой список.\n\n"
            "Ответь СТРОГО JSON:\n"
            '{"visual_density": "static|mixed|dynamic", '
            '"need_deeper_scan_ranges": [{"start": 0, "end": 120, "reason": "..."}], '
            '"note": "краткое пояснение"}'
        ),
    })

    try:
        response = gpt_client.chat.completions.create(
            model=model,
            max_tokens=800,
            messages=[{"role": "user", "content": content}],
        )
        raw = (response.choices[0].message.content or "").strip()
        # JSON может быть обёрнут в ```json ... ```
        if raw.startswith("```"):
            raw = raw.strip("`")
            if raw.lower().startswith("json"):
                raw = raw[4:].strip()
        start_idx = raw.find("{")
        end_idx = raw.rfind("}")
        if start_idx >= 0 and end_idx > start_idx:
            raw = raw[start_idx:end_idx + 1]
        parsed = json.loads(raw)
        parsed.setdefault("visual_density", "static")
        parsed.setdefault("need_deeper_scan_ranges", [])
        parsed.setdefault("note", "")
        log.info(
            "classifier: density=%s, ranges=%d, note=%r",
            parsed["visual_density"], len(parsed["need_deeper_scan_ranges"]),
            parsed.get("note", "")[:80],
        )
        return parsed
    except Exception as e:
        log.warning("classifier failed, fallback to static: %s", e)
        return {"visual_density": "static", "need_deeper_scan_ranges": [], "note": f"parse_error: {e}"}


def _subsample(frames: list[sampler.Frame], cap: int) -> list[sampler.Frame]:
    """Равномерно уменьшает список до cap, сохраняя порядок по времени."""
    if len(frames) <= cap:
        return frames
    step = len(frames) / cap
    picked = [frames[int(i * step)] for i in range(cap)]
    return picked


def _final_summary(
    frames: list[sampler.Frame],
    transcript: str,
    chat_context: str,
    focus_query: str,
    filename: str,
    duration_sec: float,
    visual_density: str,
    gpt_client,
    company_profile: str,
    model: str,
) -> tuple[str, list[str]]:
    errors: list[str] = []
    if gpt_client is None:
        return "", ["gpt_client is None"]

    transcript_trimmed = transcript or ""
    if len(transcript_trimmed) > TRANSCRIPT_MAX_CHARS:
        transcript_trimmed = (
            transcript_trimmed[:TRANSCRIPT_MAX_CHARS]
            + f"\n\n[... транскрипт обрезан, всего {len(transcript)} символов]"
        )

    prompt_text = build_video_prompt(
        company_profile=company_profile,
        filename=filename,
        duration_sec=duration_sec,
        transcript=transcript_trimmed,
        chat_context=chat_context,
        focus_query=focus_query,
        frame_timestamps=[f.timestamp_sec for f in frames],
        visual_density=visual_density,
    )

    content: list = []
    for f in frames:
        b64 = _b64_image(f.path)
        if b64 is None:
            continue
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/jpeg;base64,{b64}"},
        })
    content.append({"type": "text", "text": prompt_text})

    try:
        response = gpt_client.chat.completions.create(
            model=model,
            max_tokens=FINAL_MAX_TOKENS,
            messages=[{"role": "user", "content": content}],
        )
        return (response.choices[0].message.content or "").strip(), errors
    except Exception as e:
        errors.append(f"final summary LLM failed: {e}")
        return "", errors


def analyze_video(
    *,
    video_path: str,
    filename: str,
    chat_context: str,
    focus_query: str,
    gpt_client,
    company_profile: str,
    model: str = "openai/gpt-4.1",
) -> dict:
    """Главная точка входа. Синхронная — вызывающий оборачивает в asyncio.to_thread.

    video_path — уже скачанный файл на диске (mtproto или Bot API).
    Временные файлы чистятся в любом случае (finally).
    """
    errors: list[str] = []
    tmp_dir = tempfile.mkdtemp(prefix="video_frames_")
    try:
        log.info("analyze_video: %s (%.1f MB)", filename, os.path.getsize(video_path) / 1024 / 1024)

        transcript = _whisper_transcript(video_path)

        probe_frames, duration = sampler.probe(video_path, tmp_dir)
        if duration <= 0 and probe_frames:
            duration = max(f.timestamp_sec for f in probe_frames)

        classification = _classify(
            probe_frames=probe_frames,
            transcript_head=transcript[:CLASSIFIER_TRANSCRIPT_HEAD],
            duration_sec=duration,
            gpt_client=gpt_client,
            model=model,
        )

        all_frames = list(probe_frames)
        density = classification.get("visual_density", "static")
        if density in ("mixed", "dynamic"):
            raw_ranges = classification.get("need_deeper_scan_ranges") or []
            clean_ranges: list[tuple[float, float]] = []
            for r in raw_ranges:
                try:
                    s = float(r.get("start", 0))
                    e = float(r.get("end", 0))
                    if e > s and s < duration:
                        clean_ranges.append((max(0.0, s), min(duration, e)))
                except (TypeError, ValueError):
                    continue
            if not clean_ranges and duration > 0:
                clean_ranges = [(0.0, duration)]
            deep = sampler.deep_scan(
                video_path=video_path,
                out_dir=tmp_dir,
                ranges=clean_ranges,
                existing_count=len(all_frames),
            )
            all_frames = sorted(all_frames + deep, key=lambda f: f.timestamp_sec)

        final_frames = _subsample(all_frames, FINAL_FRAMES_CAP)
        log.info(
            "video frames: probe=%d, after_deep=%d, final=%d",
            len(probe_frames), len(all_frames), len(final_frames),
        )

        summary, llm_errors = _final_summary(
            frames=final_frames,
            transcript=transcript,
            chat_context=chat_context,
            focus_query=focus_query,
            filename=filename,
            duration_sec=duration,
            visual_density=density,
            gpt_client=gpt_client,
            company_profile=company_profile,
            model=model,
        )
        errors.extend(llm_errors)

        return {
            "document_type": "video",
            "extracted_text": transcript,
            "structured_fields": {
                "duration_sec": round(duration, 1),
                "probe_frames": len(probe_frames),
                "total_frames_sampled": len(all_frames),
                "final_frames_in_summary": len(final_frames),
                "visual_density": density,
                "classifier_note": classification.get("note", ""),
            },
            "summary": summary,
            "confidence": 1.0 if summary and not errors else 0.5 if summary else 0.0,
            "errors": errors,
        }
    finally:
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
