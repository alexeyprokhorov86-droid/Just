"""fix_worker — обработчик очереди /fix-задач.

Polls /tmp/fix_queue/*.json. Для каждого:
  1. Pre-обрабатывает видео через tools.attachments.handlers.video_handler
     (whisper transcript + frame summary), кладёт результат в промпт.
  2. Строит промпт + указывает пути к фото/документам (Claude сам прочитает).
  3. Запускает claude -p с full toolset (как /ask).
  4. Шлёт результат пользователю в DM через main bot Telegram API.
  5. Чистит файлы сессии, переносит JSON в done/.

Запускается фоновой задачей из claude_tg_bridge.bot (post_init hook).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import pathlib
import shutil
import sys
from datetime import datetime

import httpx

_REPO = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

logger = logging.getLogger("claude_bridge.fix_worker")

QUEUE_DIR = pathlib.Path("/tmp/fix_queue")
PROCESSING_DIR = QUEUE_DIR / "processing"
DONE_DIR = QUEUE_DIR / "done"

POLL_INTERVAL_SEC = 10
ASK_TIMEOUT_SEC = 1800  # 30 мин — реальные задачи могут быть тяжёлыми
CLAUDE_BIN = "/home/admin/.local/bin/claude"


def _ensure_dirs():
    QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSING_DIR.mkdir(parents=True, exist_ok=True)
    DONE_DIR.mkdir(parents=True, exist_ok=True)


def _recover_orphans():
    if not PROCESSING_DIR.exists():
        return
    for f in PROCESSING_DIR.glob("*.json"):
        try:
            f.rename(QUEUE_DIR / f.name)
            logger.info("recovered orphan from processing/: %s", f.name)
        except Exception as e:
            logger.warning("can't recover %s: %s", f.name, e)


def _build_env() -> dict:
    """OAuth + Privoxy — точно как в /ask."""
    env = os.environ.copy()
    for var in (
        "CLAUDECODE",
        "CLAUDE_CODE_SSE_PORT",
        "CLAUDE_CODE_ENTRYPOINT",
        "CLAUDE_CODE_EXECPATH",
        "ANTHROPIC_API_KEY",
    ):
        env.pop(var, None)
    env["CLAUDE_CODE_OAUTH_TOKEN"] = os.getenv("CLAUDE_CODE_OAUTH_TOKEN", "")
    env["HTTPS_PROXY"] = "http://127.0.0.1:8118"
    env["HTTP_PROXY"] = "http://127.0.0.1:8118"
    return env


def _chunks(text: str, size: int = 3800) -> list[str]:
    if len(text) <= size:
        return [text]
    parts, rest = [], text
    while len(rest) > size:
        cut = rest.rfind("\n\n", 0, size)
        if cut <= 0:
            cut = rest.rfind("\n", 0, size)
        if cut <= 0:
            cut = size
        parts.append(rest[:cut])
        rest = rest[cut:].lstrip("\n")
    if rest:
        parts.append(rest)
    return parts


async def _send_dm(main_bot_token: str, chat_id: int, text: str) -> bool:
    """Отправить пользователю DM через main bot Bot API.

    Telegram API.telegram.org доступен напрямую (без прокси) — main bot и так
    через него работает.
    """
    url = f"https://api.telegram.org/bot{main_bot_token}/sendMessage"
    proxy = os.getenv("PROXY_URL")
    client_kwargs: dict = {"timeout": 30}
    if proxy:
        client_kwargs["proxy"] = proxy
    async with httpx.AsyncClient(**client_kwargs) as client:
        for part in _chunks(text):
            try:
                r = await client.post(url, json={"chat_id": chat_id, "text": part})
                if r.status_code != 200:
                    logger.warning("sendMessage rc=%d: %s", r.status_code, r.text[:300])
                    return False
            except Exception as e:
                logger.error("sendMessage exc: %s", e)
                return False
    return True


def _get_gpt_client():
    """Локальная инициализация OpenAI клиента через RouterAI — без импорта bot.py."""
    from openai import OpenAI
    api_key = os.getenv("ROUTERAI_API_KEY")
    if not api_key:
        return None
    base_url = os.getenv("ROUTERAI_BASE_URL", "https://routerai.ru/api/v1")
    return OpenAI(api_key=api_key, base_url=base_url, timeout=400)


async def _preprocess_videos(videos: list[dict]) -> list[dict]:
    """Pre-обработка видео через video_handler.analyze_video (path-based).

    Возвращает список с {filename, summary, transcript, errors}.
    """
    if not videos:
        return []

    gpt_client = _get_gpt_client()
    if gpt_client is None:
        return [{
            "filename": v.get("filename", "video"),
            "summary": "",
            "transcript": "",
            "errors": ["ROUTERAI_API_KEY не задан — preprocess недоступен"],
        } for v in videos]

    from tools.attachments.handlers import video_handler
    from company_context import get_company_profile
    company_profile = get_company_profile()

    results = []
    for v in videos:
        path = v.get("path")
        filename = v.get("filename", "video.mp4")
        if not path or not os.path.exists(path):
            results.append({
                "filename": filename,
                "summary": "",
                "transcript": "",
                "errors": [f"file not found: {path}"],
            })
            continue
        try:
            res = await asyncio.to_thread(
                video_handler.analyze_video,
                video_path=path,
                filename=filename,
                chat_context="",
                focus_query=(
                    "Опиши проблему/контекст из видео: что происходит, какие "
                    "визуальные детали важны, что говорят. Это материал для "
                    "разработчика, который будет чинить."
                ),
                gpt_client=gpt_client,
                company_profile=company_profile,
            )
            results.append({
                "filename": filename,
                "summary": res.get("summary", "") or "",
                "transcript": res.get("extracted_text", "") or "",
                "errors": res.get("errors", []) or [],
            })
        except Exception as e:
            logger.exception("video preprocess failed: %s", path)
            results.append({
                "filename": filename,
                "summary": "",
                "transcript": "",
                "errors": [f"{type(e).__name__}: {e}"],
            })
    return results


def _build_prompt(sess: dict, video_results: list[dict]) -> str:
    user_label = sess.get("first_name") or sess.get("username") or str(sess.get("user_id"))
    lines = [
        f"Задача на исправление от пользователя: {user_label} "
        f"(@{sess.get('username','')}, id={sess.get('user_id')}).",
        "",
        "Перед началом прочитай для контекста:",
        "1. CLAUDE.md в корне проекта",
        "2. Последние 3 файла в logs/sessions/ (по mtime)",
        "",
        "Правила работы:",
        "- Режим acceptEdits, доступны Bash, Edit, Read, Write, Grep, Glob, Task.",
        "- Действуй самостоятельно, не задавай уточняющих вопросов.",
        "- Если задача про скрипты в репо — найди корень проблемы и исправь.",
        "- В конце: короткий отчёт что сделал, какие файлы тронул, нужны ли действия пользователя.",
        "",
        "─── Текстовое описание ───",
    ]
    if sess.get("texts"):
        for i, t in enumerate(sess["texts"], 1):
            lines.append(f"\n[Сообщение {i}]\n{t}")
    else:
        lines.append("(нет текста)")

    if sess.get("photos"):
        lines.append("\n─── Прикреплённые скриншоты ───")
        lines.append("Прочитай каждый файл через Read tool — он умеет смотреть PNG/JPG:")
        for p in sess["photos"]:
            lines.append(f"  • {p}")

    if video_results:
        lines.append("\n─── Видео (pre-processed transcript+summary) ───")
        for v in video_results:
            lines.append(f"\nФайл: {v['filename']}")
            if v["summary"]:
                lines.append(f"Summary: {v['summary']}")
            if v["transcript"]:
                lines.append(f"Транскрипт:\n{v['transcript'][:3000]}")
            if v["errors"]:
                lines.append(f"Ошибки обработки: {'; '.join(map(str, v['errors']))}")

    if sess.get("documents"):
        lines.append("\n─── Прикреплённые документы ───")
        lines.append("Используй Read для текстовых форматов или нужный handler из tools/attachments:")
        for d in sess["documents"]:
            lines.append(f"  • {d['path']} (mime={d.get('mime_type','?')}, name={d.get('filename','?')})")

    return "\n".join(lines)


async def _run_claude(prompt: str, session_id: str) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        CLAUDE_BIN,
        "-p", prompt,
        "--session-id", session_id,
        "--permission-mode", "acceptEdits",
        "--allowedTools", "Bash,Edit,Read,Write,Grep,Glob,Task",
        cwd=str(_REPO),
        env=_build_env(),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=ASK_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return -1, "", f"timeout после {ASK_TIMEOUT_SEC}s"
    return (
        proc.returncode,
        stdout.decode("utf-8", "replace"),
        stderr.decode("utf-8", "replace"),
    )


async def _process_one(queue_file: pathlib.Path, main_bot_token: str):
    proc_file = PROCESSING_DIR / queue_file.name
    queue_file.rename(proc_file)
    sess = json.loads(proc_file.read_text())
    sid = sess["session_id"]
    short = sid[:8]
    dm_chat_id = sess.get("dm_chat_id") or sess.get("user_id")

    logger.info("fix processing: %s by user=%s", short, sess.get("user_id"))
    t0 = datetime.now()
    try:
        video_results = await _preprocess_videos(sess.get("videos", []))
        prompt = _build_prompt(sess, video_results)
        logger.info("fix %s: prompt=%d chars, calling claude -p", short, len(prompt))
        rc, stdout, stderr = await _run_claude(prompt, sid)
        elapsed = (datetime.now() - t0).total_seconds()
        logger.info("fix %s: rc=%d elapsed=%.0fs stdout=%d", short, rc, elapsed, len(stdout))

        if rc != 0:
            err_text = (stderr or stdout or "пустой вывод")[-2500:]
            await _send_dm(
                main_bot_token,
                dm_chat_id,
                f"❌ /fix {short} упал (rc={rc}, {elapsed:.0f}s)\n\n{err_text}",
            )
        else:
            answer = (stdout or "(пустой ответ)").strip()
            await _send_dm(
                main_bot_token,
                dm_chat_id,
                f"✅ /fix {short} готов (за {elapsed:.0f}s)\n\n{answer}",
            )
    except Exception as e:
        logger.exception("fix %s handler crashed", short)
        try:
            await _send_dm(
                main_bot_token,
                dm_chat_id,
                f"❌ /fix {short} ошибка обработчика: {type(e).__name__}: {e}",
            )
        except Exception:
            pass
    finally:
        try:
            proc_file.rename(DONE_DIR / proc_file.name)
        except Exception as e:
            logger.warning("can't move %s to done/: %s", proc_file.name, e)
        sess_dir = pathlib.Path(sess.get("session_dir", ""))
        if sess_dir.exists():
            shutil.rmtree(sess_dir, ignore_errors=True)


async def fix_worker_loop(main_bot_token: str):
    _ensure_dirs()
    _recover_orphans()
    logger.info("fix_worker started, polling %s every %ds", QUEUE_DIR, POLL_INTERVAL_SEC)
    while True:
        try:
            files = sorted(QUEUE_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime)
            if files:
                await _process_one(files[0], main_bot_token)
            else:
                await asyncio.sleep(POLL_INTERVAL_SEC)
        except Exception as e:
            logger.exception("fix_worker loop error: %s", e)
            await asyncio.sleep(POLL_INTERVAL_SEC)
