"""
BOM Graph сервис — reachability-анализ по `bom_expanded`.

Читает последний calculation_id (самый актуальный BOM-снимок), строит
DiGraph product → material, и отвечает на вопросы:
  - «что пострадает если задержит поставщик X» (upward reachability)
  - «из чего состоит продукт Y» (downward reachability, полная развёртка)
  - «где используется сырьё Z» (upward от одного material)

Связка с km_entities (supplier → supplies → sku → bom.material_key) через
source_ref: в km_entities SKU хранятся с префиксом `nomenclature:<uuid>`,
в bom_expanded — просто `<uuid>`. Matching = strip prefix.

Cache: 5 минут, общий на процесс. BOM обновляется редко (раз в сутки по
cron в c1-sync), TTL этого cache не критичен — если нужен свежий — вызвать
`clear_cache()`.
"""
from __future__ import annotations

import logging
import time

import networkx as nx
from pydantic import BaseModel, Field

from ._db import get_conn
from .registry import tool

log = logging.getLogger("tools.bom_graph")

_CACHE_TTL_SEC = 300
_cache: dict = {"graph": None, "calc_id": None, "ts": 0.0}


def build_bom_graph() -> tuple[nx.DiGraph, int]:
    """Строит DiGraph последнего calculation_id. Возвращает (graph, calc_id).

    Узлы: ключ = `product_key` или `material_key` (uuid).
    Node attrs: name, type_level_1/2/3, is_finished (нет входящих рёбер = True).
    Рёбра: product_key → material_key; attrs quantity_per_unit, quantity_kg,
    material_unit.
    """
    G = nx.DiGraph()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT max(calculation_id) FROM bom_expanded")
        (calc_id,) = cur.fetchone()
        if calc_id is None:
            log.warning("bom_expanded is empty, returning empty graph")
            return G, 0

        cur.execute(
            """
            SELECT product_key, product_name, material_key, material_name,
                   material_unit, quantity_per_unit, quantity_kg,
                   type_level_1, type_level_2, type_level_3
            FROM bom_expanded
            WHERE calculation_id = %s
            """,
            (calc_id,),
        )
        for (pk, pname, mk, mname, mu, qpu, qkg, tl1, tl2, tl3) in cur.fetchall():
            if pk not in G:
                G.add_node(pk, name=pname, role="product")
            if mk not in G:
                G.add_node(mk, name=mname, role="material",
                           type_level_1=tl1, type_level_2=tl2, type_level_3=tl3)
            G.add_edge(
                pk, mk,
                quantity_per_unit=float(qpu) if qpu is not None else None,
                quantity_kg=float(qkg) if qkg is not None else None,
                unit=mu,
            )

    # Маркировка finished: узлы без входящих рёбер и с исходящими = готовые продукты;
    # узлы без исходящих = сырьё (leafs); остальные = полуфабрикаты.
    for n in G.nodes():
        in_d = G.in_degree(n)
        out_d = G.out_degree(n)
        if out_d > 0 and in_d == 0:
            G.nodes[n]["kind"] = "finished"
        elif out_d == 0:
            G.nodes[n]["kind"] = "raw"
        else:
            G.nodes[n]["kind"] = "intermediate"

    log.info(
        "BOM graph: calc_id=%s, %d nodes, %d edges (finished=%d, intermediate=%d, raw=%d)",
        calc_id, G.number_of_nodes(), G.number_of_edges(),
        sum(1 for _, d in G.nodes(data=True) if d.get("kind") == "finished"),
        sum(1 for _, d in G.nodes(data=True) if d.get("kind") == "intermediate"),
        sum(1 for _, d in G.nodes(data=True) if d.get("kind") == "raw"),
    )
    return G, calc_id


def _get_cached() -> tuple[nx.DiGraph, int]:
    now = time.time()
    if _cache["graph"] is not None and (now - _cache["ts"]) < _CACHE_TTL_SEC:
        return _cache["graph"], _cache["calc_id"]
    G, calc_id = build_bom_graph()
    _cache["graph"] = G
    _cache["calc_id"] = calc_id
    _cache["ts"] = now
    return G, calc_id


def clear_cache():
    _cache["graph"] = None
    _cache["ts"] = 0.0


def _resolve_bom_item(item_name: str) -> list[str]:
    """Находит ключи в bom_expanded по имени. Case-insensitive, ищет и среди
    product_name, и среди material_name (возможно совпадения в обоих).
    Возвращает список uuid-ключей."""
    if not item_name or not item_name.strip():
        return []
    name = item_name.strip()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT key FROM (
                SELECT product_key AS key FROM bom_expanded
                WHERE calculation_id = (SELECT max(calculation_id) FROM bom_expanded)
                  AND LOWER(product_name) = LOWER(%s)
                UNION
                SELECT material_key FROM bom_expanded
                WHERE calculation_id = (SELECT max(calculation_id) FROM bom_expanded)
                  AND LOWER(material_name) = LOWER(%s)
            ) t
            """,
            (name, name),
        )
        exact = [r[0] for r in cur.fetchall()]
    if exact:
        return exact

    # Fallback: partial match — если содержит (ILIKE %name%), ограничить 10.
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT key, name FROM (
                SELECT product_key AS key, product_name AS name FROM bom_expanded
                WHERE calculation_id = (SELECT max(calculation_id) FROM bom_expanded)
                  AND product_name ILIKE %s
                UNION
                SELECT material_key, material_name FROM bom_expanded
                WHERE calculation_id = (SELECT max(calculation_id) FROM bom_expanded)
                  AND material_name ILIKE %s
            ) t
            LIMIT 10
            """,
            (f"%{name}%", f"%{name}%"),
        )
        return [r[0] for r in cur.fetchall()]


def _node_info(G: nx.DiGraph, node_key: str) -> dict:
    if node_key not in G:
        return {"key": node_key, "name": "[неизвестно]", "kind": "unknown"}
    d = G.nodes[node_key]
    return {
        "key": node_key,
        "name": d.get("name", ""),
        "kind": d.get("kind"),
        "type_level_1": d.get("type_level_1"),
        "type_level_2": d.get("type_level_2"),
        "type_level_3": d.get("type_level_3"),
    }


# ============================================================================
# RAG Tools
# ============================================================================


class BomReachabilityInput(BaseModel):
    item_name: str = Field(description="Имя продукта или материала (product_name или material_name из bom_expanded).")
    direction: str = Field(
        default="upward",
        description=(
            "'upward' — из сырья/полуфабриката найти все готовые продукты, "
            "которые его используют (для вопросов типа 'что пострадает если "
            "не придёт сливки'). 'downward' — из продукта развернуть до сырья."
        ),
    )
    max_depth: int = Field(default=5, ge=1, le=10, description="Потолок глубины обхода.")


@tool(
    name="bom_reachability",
    domain="bom",
    description=(
        "Reachability-анализ в BOM-графе. direction='upward' отвечает «где "
        "используется X» — список готовых продуктов (finished), которые "
        "содержат указанный материал/полуфабрикат в своей рецептуре (с "
        "учётом многоуровневой вложенности). direction='downward' — полная "
        "развёртка продукта до сырья. Использует последний calculation_id "
        "из bom_expanded, кеш 5 мин."
    ),
    input_model=BomReachabilityInput,
)
def bom_reachability(item_name: str, direction: str = "upward", max_depth: int = 5) -> dict:
    keys = _resolve_bom_item(item_name)
    if not keys:
        return {"error": f"«{item_name}» не найден в bom_expanded", "matches": 0}

    G, calc_id = _get_cached()
    direction = (direction or "upward").lower()
    if direction not in ("upward", "downward"):
        return {"error": f"direction должен быть 'upward' или 'downward', получено '{direction}'"}

    results: list[dict] = []
    aggregated: set[str] = set()

    for key in keys:
        if key not in G:
            continue
        if direction == "upward":
            # Рёбра идут product → material. Нам нужны предки (кто использует этот key).
            # nx.ancestors в directed графе = все узлы откуда есть путь к key.
            reachable = nx.ancestors(G, key)
            # Фильтр: показываем только finished (корни), опустим полуфабрикаты для ответа.
            finished = [
                _node_info(G, n)
                for n in reachable
                if G.nodes[n].get("kind") == "finished"
            ]
            intermediate = [
                _node_info(G, n)
                for n in reachable
                if G.nodes[n].get("kind") == "intermediate"
            ]
            aggregated.update(n["key"] for n in finished)
            results.append({
                "query_item": _node_info(G, key),
                "finished_products_using_it": sorted(finished, key=lambda x: x["name"]),
                "intermediate_semi_products": sorted(intermediate, key=lambda x: x["name"]),
            })
        else:  # downward
            reachable = nx.descendants(G, key)
            raw = [_node_info(G, n) for n in reachable if G.nodes[n].get("kind") == "raw"]
            intermediate = [
                _node_info(G, n) for n in reachable
                if G.nodes[n].get("kind") == "intermediate"
            ]
            aggregated.update(n["key"] for n in raw)
            # Детализация по прямым компонентам + их quantity
            direct_components = []
            for _, child, edata in G.out_edges(key, data=True):
                direct_components.append({
                    **_node_info(G, child),
                    "quantity_per_unit": edata.get("quantity_per_unit"),
                    "quantity_kg": edata.get("quantity_kg"),
                    "unit": edata.get("unit"),
                })
            results.append({
                "query_item": _node_info(G, key),
                "direct_components": direct_components,
                "all_raw_materials": sorted(raw, key=lambda x: x["name"]),
                "intermediate_semi_products": sorted(intermediate, key=lambda x: x["name"]),
            })

    return {
        "direction": direction,
        "calculation_id": calc_id,
        "matched_keys": len(keys),
        "results": results,
        "unique_affected_count": len(aggregated),
    }


class BomAffectedBySupplierInput(BaseModel):
    supplier_name: str = Field(description="Имя поставщика (как в km_entities.canonical_name контрагентов).")


@tool(
    name="bom_affected_by_supplier_delay",
    domain="bom",
    description=(
        "Композитный анализ: поставщик → сырьё которое он поставляет (через "
        "km_relations.supplies) → готовые продукты которые пострадают если "
        "поставщик задержит партию (upward reachability по BOM). Ответ на "
        "вопрос «что перестанет производиться если не придут товары от X»."
    ),
    input_model=BomAffectedBySupplierInput,
)
def bom_affected_by_supplier_delay(supplier_name: str) -> dict:
    # 1. Резолвим поставщика
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, entity_type, canonical_name
            FROM km_entities
            WHERE entity_type = 'contractor'
              AND (LOWER(canonical_name) = LOWER(%s)
                   OR LOWER(%s) = ANY(ARRAY(SELECT LOWER(a) FROM unnest(aliases) a)))
              AND (status IS NULL OR status NOT IN ('merged', 'deleted'))
            ORDER BY CASE WHEN LOWER(canonical_name) = LOWER(%s) THEN 0 ELSE 1 END,
                     centrality DESC NULLS LAST
            LIMIT 1
            """,
            (supplier_name, supplier_name, supplier_name),
        )
        row = cur.fetchone()
        if row is None:
            return {"error": f"Поставщик «{supplier_name}» не найден среди km_entities (entity_type='contractor')"}
        supplier = {"id": row[0], "type": row[1], "canonical_name": row[2]}

        # 2. SKU которые поставляет
        cur.execute(
            """
            SELECT e.id, e.canonical_name, e.source_ref
            FROM km_relations r
            JOIN km_entities e ON e.id = r.to_entity_id
            WHERE r.from_entity_id = %s
              AND r.relation_type = 'supplies'
              AND e.entity_type = 'sku'
              AND (r.valid_to IS NULL OR r.valid_to >= CURRENT_DATE)
            """,
            (supplier["id"],),
        )
        skus = cur.fetchall()

    if not skus:
        return {
            "supplier": supplier,
            "supplied_skus": [],
            "note": "У поставщика нет зафиксированных рёбер supplies в km_relations.",
        }

    # 3. Для каждого SKU ищем material_key в BOM и делаем upward reachability
    G, calc_id = _get_cached()
    supplied = []
    all_affected_finished: set[str] = set()
    for sku_id, sku_name, source_ref in skus:
        material_key = None
        if source_ref and source_ref.startswith("nomenclature:"):
            material_key = source_ref[len("nomenclature:"):]
        finished_names: list[str] = []
        if material_key and material_key in G:
            for anc in nx.ancestors(G, material_key):
                if G.nodes[anc].get("kind") == "finished":
                    all_affected_finished.add(anc)
                    finished_names.append(G.nodes[anc].get("name", ""))
        supplied.append({
            "sku_id": sku_id,
            "sku_name": sku_name,
            "matched_in_bom": material_key in G if material_key else False,
            "finished_products_count": len(finished_names),
            "finished_products_sample": sorted(finished_names)[:10],
        })

    affected_products = sorted(
        [_node_info(G, k) for k in all_affected_finished],
        key=lambda x: x["name"],
    )

    return {
        "supplier": supplier,
        "calculation_id": calc_id,
        "supplied_skus": supplied,
        "affected_finished_products_count": len(affected_products),
        "affected_finished_products": affected_products,
    }
