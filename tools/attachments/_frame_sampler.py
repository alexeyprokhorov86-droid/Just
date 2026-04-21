"""
Adaptive frame sampling для длинных видео.

Две фазы:
  1. probe(video_path) → N кадров = scene-detection + M опорных (равномерно).
  2. deep_scan(ranges) → доп. кадры 1/Ns в указанных временных диапазонах.

Логика «когда дополнять» живёт снаружи (video_handler) — LLM смотрит probe-set
и решает, нужен ли deep-scan. Здесь — чистая механика ffmpeg + files.

Cap: `MAX_FRAMES` (default 200), сверх этого warning + stop.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger("tools.attachments.sampler")

MAX_FRAMES = 200
PROBE_ANCHOR_COUNT = 6          # опорных кадров (равномерно) в пробе
PROBE_SCENE_THRESHOLD = 0.3     # ffmpeg scene filter: чем ниже, тем чувствительнее
DEEP_SCAN_INTERVAL_SEC = 7      # 1 кадр каждые N секунд в диапазонах deep-scan
JPEG_QUALITY = 4                # ffmpeg -q:v, 2-5 хорошо (меньше = лучше)


@dataclass
class Frame:
    path: str
    timestamp_sec: float

    def __repr__(self) -> str:
        return f"Frame(t={self.timestamp_sec:.1f}s, {os.path.basename(self.path)})"


def probe_video_duration(video_path: str) -> float:
    """Длительность через ffprobe, в секундах. 0.0 если не удалось определить."""
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "json", video_path],
            capture_output=True, text=True, timeout=30,
        )
        data = json.loads(result.stdout or "{}")
        return float(data.get("format", {}).get("duration", 0.0))
    except Exception as e:
        log.warning("ffprobe duration failed for %s: %s", video_path, e)
        return 0.0


def _ffmpeg_extract(video_path: str, out_pattern: str, vf: str, extra_args: list | None = None) -> list[str]:
    """Запускает ffmpeg, возвращает отсортированный список созданных файлов."""
    cmd = [
        "ffmpeg", "-v", "error", "-y",
        "-i", video_path,
        "-vf", vf,
        "-vsync", "vfr",
        "-q:v", str(JPEG_QUALITY),
    ]
    if extra_args:
        cmd.extend(extra_args)
    cmd.append(out_pattern)

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=900)
    except subprocess.CalledProcessError as e:
        log.warning("ffmpeg failed: %s\nstderr: %s", " ".join(cmd), e.stderr[:500] if e.stderr else "")
        return []
    except subprocess.TimeoutExpired:
        log.warning("ffmpeg timeout: %s", " ".join(cmd))
        return []

    out_dir = os.path.dirname(out_pattern) or "."
    prefix = os.path.basename(out_pattern).split("%")[0]
    return sorted(
        os.path.join(out_dir, f)
        for f in os.listdir(out_dir)
        if f.startswith(prefix) and f.endswith(".jpg")
    )


def _scene_frames(video_path: str, out_dir: str, threshold: float = PROBE_SCENE_THRESHOLD, max_count: int = 40) -> list[Frame]:
    """Сцены через `select='gt(scene,N)'`. Возвращает max_count первых."""
    pattern = os.path.join(out_dir, "scene_%04d.jpg")
    vf = f"select='gt(scene,{threshold})',showinfo"
    cmd = [
        "ffmpeg", "-v", "info", "-y",
        "-i", video_path,
        "-vf", vf,
        "-vsync", "vfr",
        "-q:v", str(JPEG_QUALITY),
        "-frames:v", str(max_count),
        pattern,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=900)
    except subprocess.TimeoutExpired:
        log.warning("scene-detection timeout on %s", video_path)
        return []

    stderr = result.stderr or ""
    # Парсим timestamps из showinfo (pts_time:NN.NN)
    import re
    timestamps: list[float] = []
    for m in re.finditer(r"pts_time:(\d+(?:\.\d+)?)", stderr):
        timestamps.append(float(m.group(1)))

    files = sorted(
        os.path.join(out_dir, f)
        for f in os.listdir(out_dir)
        if f.startswith("scene_") and f.endswith(".jpg")
    )
    frames = []
    for i, path in enumerate(files):
        ts = timestamps[i] if i < len(timestamps) else 0.0
        frames.append(Frame(path=path, timestamp_sec=ts))
    log.info("scene-detection: %d кадров (порог=%.2f)", len(frames), threshold)
    return frames


def _anchor_frames(video_path: str, duration_sec: float, out_dir: str, count: int = PROBE_ANCHOR_COUNT) -> list[Frame]:
    """Равномерно распределённые опорные кадры — гарантия минимального покрытия."""
    if duration_sec <= 0 or count <= 0:
        return []
    # Выбираем моменты в 5%, 20%, 40%, 60%, 80%, 95% (для count=6) — избегаем самых краёв.
    # Универсально: равномерно между 5% и 95%.
    if count == 1:
        times = [duration_sec * 0.5]
    else:
        times = [duration_sec * (0.05 + (0.9 * i / (count - 1))) for i in range(count)]

    frames: list[Frame] = []
    for i, t in enumerate(times):
        path = os.path.join(out_dir, f"anchor_{i:02d}.jpg")
        cmd = [
            "ffmpeg", "-v", "error", "-y",
            "-ss", f"{t:.2f}", "-i", video_path,
            "-frames:v", "1", "-q:v", str(JPEG_QUALITY),
            path,
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, timeout=60)
            if os.path.exists(path):
                frames.append(Frame(path=path, timestamp_sec=t))
        except Exception as e:
            log.debug("anchor frame @%.1fs failed: %s", t, e)
    return frames


def probe(video_path: str, out_dir: str) -> tuple[list[Frame], float]:
    """Probe-фаза: scene-detection + anchor кадры. Возвращает (frames, duration_sec)."""
    os.makedirs(out_dir, exist_ok=True)
    duration = probe_video_duration(video_path)

    scene = _scene_frames(video_path, out_dir, max_count=40)
    anchors = _anchor_frames(video_path, duration, out_dir)

    # Мёрджим + сортируем по времени + дедуп близких (< 2s разница)
    all_frames = sorted(scene + anchors, key=lambda f: f.timestamp_sec)
    deduped: list[Frame] = []
    for f in all_frames:
        if deduped and abs(f.timestamp_sec - deduped[-1].timestamp_sec) < 2.0:
            try:
                os.unlink(f.path)
            except OSError:
                pass
            continue
        deduped.append(f)

    log.info(
        "probe: duration=%.1fs, scene=%d, anchors=%d, deduped=%d",
        duration, len(scene), len(anchors), len(deduped),
    )
    return deduped, duration


def deep_scan(
    video_path: str,
    out_dir: str,
    ranges: list[tuple[float, float]],
    interval_sec: float = DEEP_SCAN_INTERVAL_SEC,
    existing_count: int = 0,
) -> list[Frame]:
    """Доп. кадры 1/interval_sec в указанных (start,end) диапазонах.

    existing_count — сколько кадров уже есть, для соблюдения MAX_FRAMES cap.
    """
    if not ranges:
        return []

    os.makedirs(out_dir, exist_ok=True)
    budget = MAX_FRAMES - existing_count
    if budget <= 0:
        log.warning("deep_scan: cap %d кадров достигнут, пропускаем", MAX_FRAMES)
        return []

    frames: list[Frame] = []
    for ri, (start, end) in enumerate(ranges):
        if budget <= 0:
            log.warning("deep_scan: cap достигнут на range #%d", ri)
            break
        if end <= start:
            continue
        segment_frames = int((end - start) / interval_sec) + 1
        segment_frames = min(segment_frames, budget)

        pattern = os.path.join(out_dir, f"deep_{ri:02d}_%04d.jpg")
        cmd = [
            "ffmpeg", "-v", "error", "-y",
            "-ss", f"{start:.2f}", "-to", f"{end:.2f}",
            "-i", video_path,
            "-vf", f"fps=1/{interval_sec}",
            "-q:v", str(JPEG_QUALITY),
            "-frames:v", str(segment_frames),
            pattern,
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=True, timeout=900)
        except Exception as e:
            log.warning("deep_scan range (%.1f,%.1f) failed: %s", start, end, e)
            continue

        extracted = sorted(
            os.path.join(out_dir, f)
            for f in os.listdir(out_dir)
            if f.startswith(f"deep_{ri:02d}_") and f.endswith(".jpg")
        )
        for i, path in enumerate(extracted):
            ts = start + i * interval_sec
            frames.append(Frame(path=path, timestamp_sec=ts))
        budget -= len(extracted)

    log.info(
        "deep_scan: %d новых кадров в %d диапазонах (осталось бюджета %d)",
        len(frames), len(ranges), budget,
    )
    return frames
