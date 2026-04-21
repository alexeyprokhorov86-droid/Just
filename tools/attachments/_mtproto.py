"""
Pyrogram user-client для скачивания больших файлов из Telegram (> 20 MB).

Bot API отдаёт максимум 20 MB. Через user-client (mtproto.session, user id
805598873) лимит — 2 GB/файл. Используется для совещаний/собеседований/
обучающих роликов которые бот получает как document или video.

Session-файл: `{repo_root}/mtproto.session` (создаётся через
setup_mtproto_session.py, в .gitignore).

Контракт:
    await download_from_telegram(chat_id, message_id, dest_path)
        → (path, size_bytes)   # файл сохранён, size подтверждён
    Бросает MtprotoUnavailable если session отсутствует или user не в чате.
"""
from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

log = logging.getLogger("tools.attachments.mtproto")

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_SESSION_NAME = "mtproto"   # → mtproto.session в workdir
_PROXY = {"scheme": "socks5", "hostname": "127.0.0.1", "port": 1080}

_client = None
_client_lock = asyncio.Lock()


class MtprotoUnavailable(RuntimeError):
    """Session отсутствует, пользователь не в чате, или другая ошибка доступа."""


def _session_path() -> Path:
    return _REPO_ROOT / f"{_SESSION_NAME}.session"


async def _get_client():
    """Lazy singleton. Pyrogram 2.0 async, использует тот же event loop что и bot."""
    global _client
    if _client is not None and _client.is_connected:
        return _client

    async with _client_lock:
        if _client is not None and _client.is_connected:
            return _client

        if not _session_path().exists():
            raise MtprotoUnavailable(
                f"mtproto.session не найден в {_REPO_ROOT}. "
                "Запусти setup_mtproto_session.py для авторизации."
            )

        api_id = os.environ.get("TELEGRAM_API_ID")
        api_hash = os.environ.get("TELEGRAM_API_HASH")
        if not api_id or not api_hash:
            raise MtprotoUnavailable("TELEGRAM_API_ID / TELEGRAM_API_HASH не заданы в .env")

        from pyrogram import Client
        app = Client(
            _SESSION_NAME,
            api_id=int(api_id),
            api_hash=api_hash,
            proxy=_PROXY,
            workdir=str(_REPO_ROOT),
            no_updates=True,   # не слушаем апдейты — только скачиваем по запросу
        )
        await app.start()
        _client = app
        log.info("pyrogram user-client started (session=%s)", _session_path())
        await _warm_peer_cache(app)
        return _client


async def _warm_peer_cache(app) -> None:
    """Прогоняем get_dialogs один раз — без этого после свежей session
    peer-cache пустой и get_messages(chat_id) падает на PeerIdInvalid.

    ~2-5s для пользователя с ~100 диалогами. Делается один раз за жизнь клиента.
    """
    count = 0
    try:
        async for _ in app.get_dialogs():
            count += 1
        log.info("peer cache warmed via get_dialogs: %d dialogs", count)
    except Exception as e:
        log.warning("peer cache warm-up failed (продолжим, отдельные download'ы могут упасть): %s", e)


async def download_from_telegram(
    chat_id: int,
    message_id: int,
    dest_path: str,
) -> tuple[str, int]:
    """Скачивает media из сообщения (chat_id, message_id) в dest_path.

    Ограничение: user 805598873 должен быть участником chat_id. Если не
    участник — pyrogram вернёт PEER_ID_INVALID → оборачиваем в MtprotoUnavailable.
    """
    from pyrogram.errors import RPCError

    try:
        app = await _get_client()
    except MtprotoUnavailable:
        raise
    except Exception as e:
        raise MtprotoUnavailable(f"не удалось запустить pyrogram client: {e}") from e

    async def _get():
        return await app.get_messages(chat_id=chat_id, message_ids=message_id)

    try:
        msg = await _get()
    except (KeyError, ValueError) as e:
        # Peer not cached → прогреваем и повторяем.
        log.info("peer cache miss for chat_id=%s, re-warming", chat_id)
        await _warm_peer_cache(app)
        try:
            msg = await _get()
        except Exception as e2:
            raise MtprotoUnavailable(
                f"get_messages({chat_id}, {message_id}) не удался даже после warm-up: {e2}. "
                "Скорее всего user-account не в этом чате."
            ) from e2
    except RPCError as e:
        # PEER_ID_INVALID и подобные — pyrogram эту ошибку бросает как
        # обычный ValueError с текстом 'Peer id invalid: ...', поэтому обычно
        # в ветку выше попадает. Но на всякий случай.
        if "peer id invalid" in str(e).lower():
            log.info("peer id invalid for chat_id=%s, re-warming", chat_id)
            await _warm_peer_cache(app)
            try:
                msg = await _get()
            except Exception as e2:
                raise MtprotoUnavailable(
                    f"get_messages({chat_id}, {message_id}) после warm-up: {e2}. "
                    "user-account не в этом чате."
                ) from e2
        else:
            raise MtprotoUnavailable(
                f"get_messages({chat_id}, {message_id}) не удался: {e}."
            ) from e

    if msg is None or msg.empty:
        raise MtprotoUnavailable(f"Сообщение {chat_id}/{message_id} не найдено или удалено.")

    media = msg.video or msg.document or msg.audio or msg.voice or msg.video_note
    if media is None:
        raise MtprotoUnavailable(f"В сообщении {chat_id}/{message_id} нет media.")

    os.makedirs(os.path.dirname(dest_path) or ".", exist_ok=True)

    log.info(
        "mtproto download start: chat=%s msg=%s size=%s → %s",
        chat_id, message_id, getattr(media, "file_size", "?"), dest_path,
    )
    result_path = await app.download_media(msg, file_name=dest_path)
    if result_path is None:
        raise MtprotoUnavailable("download_media вернул None (отменено или недоступно).")

    size = os.path.getsize(result_path)
    log.info("mtproto download done: %s (%.1f MB)", result_path, size / 1024 / 1024)
    return result_path, size


async def shutdown():
    """Graceful stop — вызвать на выключении бота (опционально)."""
    global _client
    if _client is not None and _client.is_connected:
        try:
            await _client.stop()
        except Exception as e:
            log.warning("pyrogram shutdown warning: %s", e)
        _client = None
