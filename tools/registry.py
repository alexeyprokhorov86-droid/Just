"""
Tool registry для telegram_logger_bot.

Контракт:
- Tool = функция + pydantic InputModel + JSON-описание.
- Декоратор @tool оборачивает функцию в тонкий validator (bind args → InputModel
  → вызов fn). Валидация срабатывает и на прямом импорте, и через invoke() —
  опечатки в параметрах ловятся на границе. Overhead ~0.1-1мс/вызов.
- invoke(name, params) используется внешними интерфейсами (LLM tool_use,
  /slash, Element, HTTP) — принимает dict, сам вызывает зарегистрированный
  validator.
- llm_schemas() возвращает JSON-schema в формате Anthropic/OpenAI tool_use —
  готово к передаче в поле `tools=` запроса к LLM.

См. tools/__init__.py: импорт модулей триггерит регистрацию.
"""
from __future__ import annotations

import functools
import inspect
import logging
import time
from dataclasses import dataclass
from typing import Any, Callable

from pydantic import BaseModel, ValidationError

log = logging.getLogger("tools")


@dataclass
class Tool:
    name: str
    description: str
    domain: str
    input_model: type[BaseModel]
    fn: Callable[..., Any]  # validated wrapper, не исходная функция

    def llm_schema(self) -> dict:
        """JSON-schema в формате Anthropic/OpenAI tool_use."""
        return {
            "name": self.name,
            "description": self.description,
            "input_schema": self.input_model.model_json_schema(),
        }


REGISTRY: dict[str, Tool] = {}


def tool(
    *,
    name: str,
    description: str,
    domain: str,
    input_model: type[BaseModel],
) -> Callable[[Callable], Callable]:
    def wrap(fn: Callable) -> Callable:
        if name in REGISTRY:
            raise ValueError(f"tool '{name}' already registered")
        sig = inspect.signature(fn)

        @functools.wraps(fn)
        def validated(*args: Any, **kwargs: Any) -> Any:
            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()
            # InputModel ловит опечатки/неверные типы/enum-нарушения одинаково
            # для прямого импорта и для invoke() из dict.
            model = input_model(**bound.arguments)
            return fn(**model.model_dump())

        REGISTRY[name] = Tool(
            name=name,
            description=description,
            domain=domain,
            input_model=input_model,
            fn=validated,
        )
        return validated

    return wrap


def invoke(name: str, params: dict) -> Any:
    """Вызов tool по имени из dict-параметров (LLM/slash/HTTP)."""
    if name not in REGISTRY:
        raise KeyError(f"unknown tool: {name}")
    t = REGISTRY[name]
    t0 = time.time()
    try:
        result = t.fn(**params)
    except ValidationError as e:
        log.warning("tool %s invalid params: %s", name, e)
        raise
    except Exception:
        log.exception("tool %s failed", name)
        raise
    log.info("tool %s ok in %.2fs", name, time.time() - t0)
    return result


def list_tools(domain: str | None = None) -> list[Tool]:
    return [t for t in REGISTRY.values() if domain is None or t.domain == domain]


def llm_schemas(domain: str | None = None) -> list[dict]:
    return [t.llm_schema() for t in list_tools(domain)]
