"""
tools/ — registry и реализации tool-обёрток для tg-бота, RAG, cron и внешних интерфейсов.

Импорт пакета триггерит регистрацию всех tool-функций в REGISTRY.
Добавляешь новый tool → создаёшь модуль с @tool декоратором → добавляешь импорт сюда.
"""
from .registry import REGISTRY, Tool, invoke, list_tools, llm_schemas, tool  # noqa: F401

# Регистрация tools (порядок не важен).
from . import chats  # noqa: F401
from . import bom    # noqa: F401
