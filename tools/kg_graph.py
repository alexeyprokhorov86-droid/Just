"""
Knowledge Graph сервис: строит NetworkX-граф из km_entities + km_relations,
считает PageRank / degree / community detection, пишет метрики обратно в
km_entities (centrality, degree_in, degree_out, community_id, kg_updated_at).

CLI:
    python3 -m tools.kg_graph rebuild
    python3 -m tools.kg_graph stats

Крон (см. TASK_kg_graph.md Фаза 1.3):
    0 */6 * * * cd /home/admin/telegram_logger_bot && python3 -m tools.kg_graph rebuild

Использование из Python:
    from tools.kg_graph import rebuild_entity_graph, build_graph
    rebuild_entity_graph()  # rebuild + UPDATE
    G = build_graph()       # только построить для read-only запросов

Фаза 2 добавит RAG-tools (graph_neighbors, graph_shortest_path и т.п.)
поверх того же build_graph. Фаза 3 — отдельный BOM-граф.
"""
from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime
from typing import Literal

import networkx as nx
import psycopg2
from psycopg2.extras import execute_batch
from pydantic import BaseModel, Field

from ._db import get_conn
from .registry import tool

log = logging.getLogger("tools.kg_graph")

# Seed для воспроизводимости community detection между прогонами.
# Louvain недетерминирован без seed'а; community_id любой реальной сущности
# не должен прыгать между прогонами cron'а без причины.
COMMUNITY_SEED = 42
PAGERANK_DAMPING = 0.85
PAGERANK_MAX_ITER = 100


# Совместимость: старый _db() продолжает работать для внутренних вызовов,
# но тянет реализацию из общего tools._db.get_conn.
def _db():
    return get_conn()


# ----------------------------------------------------------------------------
# Кеш in-memory графа для RAG-tools (Фаза 2).
# build_graph() читает pg и строит DiGraph на 3-4 секунды. Для RAG-tools это
# недопустимо медленно на каждый вызов — держим кеш с TTL 5 минут.
# ----------------------------------------------------------------------------

_GRAPH_CACHE_TTL_SEC = 300
_graph_cache: dict = {"graph": None, "ts": 0.0}


def _get_cached_graph() -> nx.DiGraph:
    now = time.time()
    if _graph_cache["graph"] is not None and (now - _graph_cache["ts"]) < _GRAPH_CACHE_TTL_SEC:
        return _graph_cache["graph"]
    G = build_graph()
    _graph_cache["graph"] = G
    _graph_cache["ts"] = now
    return G


def _resolve_entity(name: str, entity_type: str | None = None) -> dict | None:
    """Находит сущность по canonical_name или алиасу (case-insensitive).

    Если entity_type задан — только этого типа (employee/sku/contractor/...).
    Возвращает {id, entity_type, canonical_name, aliases} или None.
    """
    if not name or not name.strip():
        return None
    name = name.strip()
    clauses = ["(LOWER(canonical_name) = LOWER(%s) OR LOWER(%s) = ANY(ARRAY(SELECT LOWER(a) FROM unnest(aliases) a)))"]
    params: list = [name, name]
    if entity_type:
        clauses.append("entity_type = %s")
        params.append(entity_type)
    # Приоритет: точное совпадение canonical → алиас. Плюс фильтр по статусу.
    clauses.append("(status IS NULL OR status NOT IN ('merged', 'deleted'))")
    sql = (
        "SELECT id, entity_type, canonical_name, aliases, centrality, community_id "
        "FROM km_entities WHERE " + " AND ".join(clauses) +
        " ORDER BY CASE WHEN LOWER(canonical_name) = LOWER(%s) THEN 0 ELSE 1 END, centrality DESC NULLS LAST "
        "LIMIT 1"
    )
    params.append(name)
    with _db() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "entity_type": row[1],
        "canonical_name": row[2],
        "aliases": list(row[3] or []),
        "centrality": float(row[4]) if row[4] is not None else None,
        "community_id": int(row[5]) if row[5] is not None else None,
    }


def build_graph() -> nx.DiGraph:
    """Читает km_entities + km_relations, возвращает DiGraph.

    Фильтры:
      - Сущности: status != 'merged' (merged-сущности — алиасы, не самостоятельные узлы).
      - Отношения: valid_to IS NULL OR valid_to >= today.

    Node attrs: entity_type, canonical_name, status, confidence.
    Edge attrs: relation_type, weight, source_count, valid_from, valid_to.
    """
    G = nx.DiGraph()
    with _db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id, entity_type, canonical_name, status, confidence
            FROM km_entities
            WHERE status IS NULL OR status NOT IN ('merged', 'deleted')
        """)
        for eid, etype, name, status, conf in cur.fetchall():
            G.add_node(
                eid,
                entity_type=etype,
                canonical_name=name,
                status=status,
                confidence=conf or 0.0,
            )

        cur.execute("""
            SELECT from_entity_id, to_entity_id, relation_type, weight,
                   source_count, valid_from, valid_to
            FROM km_relations
            WHERE (valid_to IS NULL OR valid_to >= CURRENT_DATE)
              AND from_entity_id IS NOT NULL
              AND to_entity_id IS NOT NULL
        """)
        edge_count = 0
        skipped_dangling = 0
        for src, dst, rtype, w, sc, vf, vt in cur.fetchall():
            if src not in G or dst not in G:
                skipped_dangling += 1
                continue
            # Если ребро уже есть (параллельные relations разного типа — берём
            # максимальный вес, суммируем source_count). DiGraph не multigraph.
            if G.has_edge(src, dst):
                data = G[src][dst]
                data["weight"] = max(data.get("weight", 0), w or 1.0)
                data["source_count"] = (data.get("source_count", 0) or 0) + (sc or 0)
                data["relation_types"] = data.get("relation_types", []) + [rtype]
            else:
                G.add_edge(
                    src, dst,
                    weight=float(w or 1.0),
                    relation_type=rtype,
                    relation_types=[rtype],
                    source_count=sc or 0,
                    valid_from=vf,
                    valid_to=vt,
                )
            edge_count += 1

    log.info(
        "graph built: %d nodes, %d edges (dangling skipped: %d)",
        G.number_of_nodes(), G.number_of_edges(), skipped_dangling,
    )
    return G


def _compute_metrics(G: nx.DiGraph) -> dict[int, dict]:
    """Возвращает {entity_id: {centrality, degree_in, degree_out, community_id}}."""
    t0 = time.time()

    # PageRank на directed-графе. Если граф пустой — вернём пустой dict.
    if G.number_of_nodes() == 0:
        return {}

    try:
        pr = nx.pagerank(
            G, alpha=PAGERANK_DAMPING, max_iter=PAGERANK_MAX_ITER, weight="weight"
        )
    except nx.PowerIterationFailedConvergence:
        log.warning("PageRank не сошёлся за %d итераций, использую частичный результат", PAGERANK_MAX_ITER)
        pr = nx.pagerank(G, alpha=PAGERANK_DAMPING, max_iter=1000, weight="weight", tol=1e-4)
    log.info("pagerank: %.2fs", time.time() - t0)

    in_deg = dict(G.in_degree())
    out_deg = dict(G.out_degree())

    # Community detection — на undirected проекции. Louvain быстрее и даёт
    # лучший modularity чем greedy_modularity; seed делает результат
    # воспроизводимым между rebuild'ами.
    t1 = time.time()
    UG = G.to_undirected(as_view=False)
    try:
        communities = nx.community.louvain_communities(UG, seed=COMMUNITY_SEED, weight="weight")
    except AttributeError:
        # NetworkX < 3.0 — fallback
        communities = nx.algorithms.community.greedy_modularity_communities(UG, weight="weight")
    log.info("louvain: %.2fs, %d communities", time.time() - t1, len(communities))

    # Сортируем communities по размеру → стабильный cluster_id: самая большая = 0.
    communities_sorted = sorted(communities, key=lambda c: -len(c))
    node_community: dict[int, int] = {}
    for cid, members in enumerate(communities_sorted):
        for node in members:
            node_community[node] = cid

    return {
        nid: {
            "centrality": float(pr.get(nid, 0.0)),
            "degree_in": int(in_deg.get(nid, 0)),
            "degree_out": int(out_deg.get(nid, 0)),
            "community_id": int(node_community.get(nid, -1)),
        }
        for nid in G.nodes()
    }


def _write_back(metrics: dict[int, dict]) -> int:
    """UPDATE km_entities батчами. Возвращает количество обновлённых строк."""
    if not metrics:
        return 0
    now = datetime.utcnow()
    rows = [
        (m["centrality"], m["degree_in"], m["degree_out"], m["community_id"], now, eid)
        for eid, m in metrics.items()
    ]
    with _db() as conn, conn.cursor() as cur:
        execute_batch(
            cur,
            """
            UPDATE km_entities
            SET centrality = %s, degree_in = %s, degree_out = %s,
                community_id = %s, kg_updated_at = %s
            WHERE id = %s
            """,
            rows,
            page_size=500,
        )
        conn.commit()
    return len(rows)


def rebuild_entity_graph() -> dict:
    """Основная функция: build → compute → write-back. Возвращает статистику.

    Возврат:
        {nodes, edges, communities, updated_rows, elapsed_sec, top_centrality}
    """
    t0 = time.time()
    G = build_graph()
    metrics = _compute_metrics(G)
    updated = _write_back(metrics)

    # Топ-5 по centrality для лога и проверки sanity
    top = []
    if metrics:
        top_ids = sorted(metrics.keys(), key=lambda i: metrics[i]["centrality"], reverse=True)[:5]
        with _db() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT id, entity_type, canonical_name FROM km_entities WHERE id = ANY(%s)",
                (top_ids,),
            )
            name_map = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
        top = [
            {
                "id": eid,
                "entity_type": name_map.get(eid, ("?", "?"))[0],
                "canonical_name": name_map.get(eid, ("?", "?"))[1],
                "centrality": round(metrics[eid]["centrality"], 5),
            }
            for eid in top_ids
        ]

    comm_count = len({m["community_id"] for m in metrics.values()}) if metrics else 0
    stats = {
        "nodes": G.number_of_nodes(),
        "edges": G.number_of_edges(),
        "communities": comm_count,
        "updated_rows": updated,
        "elapsed_sec": round(time.time() - t0, 2),
        "top_centrality": top,
    }
    log.info(
        "rebuild done: %d nodes, %d edges, %d communities, %d rows updated in %.2fs",
        stats["nodes"], stats["edges"], stats["communities"],
        stats["updated_rows"], stats["elapsed_sec"],
    )
    if top:
        log.info("top-5 centrality: %s", top)
    return stats


def stats() -> dict:
    """Быстрый read-only снимок текущего состояния — без rebuild'а."""
    with _db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT
                count(*) FILTER (WHERE kg_updated_at IS NOT NULL) AS with_metrics,
                count(*) AS total,
                max(kg_updated_at) AS last_update,
                count(DISTINCT community_id) FILTER (WHERE community_id IS NOT NULL) AS communities,
                max(centrality) AS max_cent
            FROM km_entities
            WHERE status IS NULL OR status NOT IN ('merged', 'deleted')
        """)
        row = cur.fetchone()
        return {
            "entities_with_metrics": row[0],
            "entities_total": row[1],
            "last_update": row[2].isoformat() if row[2] else None,
            "communities": row[3],
            "max_centrality": float(row[4]) if row[4] else None,
        }


# ============================================================================
# Фаза 2 — RAG tools (graph_neighbors, graph_shortest_path, employee_responsibility)
# ============================================================================


class GraphNeighborsInput(BaseModel):
    entity_name: str = Field(description="Имя сущности (сотрудник/клиент/поставщик/SKU/склад и т.п.). Резолвится через canonical_name + aliases.")
    entity_type: str = Field(default="", description="Опционально: ограничить тип (employee/sku/contractor/warehouse/department/position/process/other/product/client).")
    relation_types: list[str] = Field(default_factory=list, description="Опционально: только эти типы рёбер (supplies/buys/works_in/responsible_for/stored_at/holds_position/reports_to/collaborates_with/approves/complains_about).")
    depth: int = Field(default=1, ge=1, le=2, description="1 или 2. Depth=2 даёт соседей соседей (осторожно, может быть много).")
    limit: int = Field(default=50, ge=1, le=500, description="Потолок на количество возвращаемых соседей.")


@tool(
    name="graph_neighbors",
    domain="kg",
    description=(
        "Возвращает связанные сущности из knowledge graph. Работает на km_entities + "
        "km_relations. entity_name резолвится по canonical_name и aliases. "
        "Применения: «с кем работает Иванов» (entity_type='employee', relation_types="
        "['works_in','collaborates_with']), «что продаёт поставщик X» (supplies), "
        "«где хранится SKU Y» (stored_at). Depth=2 возвращает и соседей соседей."
    ),
    input_model=GraphNeighborsInput,
)
def graph_neighbors(
    entity_name: str,
    entity_type: str = "",
    relation_types: list[str] | None = None,
    depth: int = 1,
    limit: int = 50,
) -> dict:
    root = _resolve_entity(entity_name, entity_type or None)
    if root is None:
        return {"entity": None, "error": f"Сущность «{entity_name}» не найдена в km_entities", "neighbors": []}

    rel_filter = ""
    params: list = [root["id"], root["id"]]
    if relation_types:
        rel_filter = "AND relation_type = ANY(%s) "
        params.append(list(relation_types))

    # Соседи depth=1: рёбра в обе стороны (directed, но спрашивающему важно оба направления).
    sql_d1 = (
        "SELECT r.from_entity_id, r.to_entity_id, r.relation_type, r.weight, "
        "       r.source_count, r.valid_from, r.valid_to, "
        "       e1.entity_type, e1.canonical_name, "
        "       e2.entity_type, e2.canonical_name "
        "FROM km_relations r "
        "JOIN km_entities e1 ON e1.id = r.from_entity_id "
        "JOIN km_entities e2 ON e2.id = r.to_entity_id "
        "WHERE (r.from_entity_id = %s OR r.to_entity_id = %s) "
        f"{rel_filter}"
        "AND (r.valid_to IS NULL OR r.valid_to >= CURRENT_DATE) "
        "ORDER BY r.weight DESC NULLS LAST, r.source_count DESC NULLS LAST "
        f"LIMIT {int(limit)}"
    )
    neighbors: list[dict] = []
    seen_ids: set[int] = {root["id"]}
    with _db() as conn, conn.cursor() as cur:
        cur.execute(sql_d1, params)
        for fid, tid, rtype, w, sc, vf, vt, e1t, e1n, e2t, e2n in cur.fetchall():
            if fid == root["id"]:
                other_id, other_type, other_name, direction = tid, e2t, e2n, "out"
            else:
                other_id, other_type, other_name, direction = fid, e1t, e1n, "in"
            if other_id in seen_ids:
                continue
            seen_ids.add(other_id)
            neighbors.append({
                "id": other_id,
                "entity_type": other_type,
                "canonical_name": other_name,
                "via_relation": rtype,
                "direction": direction,
                "weight": float(w or 0.0),
                "source_count": int(sc or 0),
                "distance": 1,
            })

    if depth >= 2 and neighbors:
        d1_ids = [n["id"] for n in neighbors]
        # Depth=2: соседи соседей, но исключая уже известных.
        sql_d2 = (
            "SELECT r.from_entity_id, r.to_entity_id, r.relation_type, r.weight, "
            "       e.entity_type, e.canonical_name "
            "FROM km_relations r "
            "JOIN km_entities e ON e.id = CASE WHEN r.from_entity_id = ANY(%s) "
            "                                  THEN r.to_entity_id ELSE r.from_entity_id END "
            "WHERE (r.from_entity_id = ANY(%s) OR r.to_entity_id = ANY(%s)) "
            "AND (r.valid_to IS NULL OR r.valid_to >= CURRENT_DATE) "
            f"LIMIT {int(limit)}"
        )
        with _db() as conn, conn.cursor() as cur:
            cur.execute(sql_d2, (d1_ids, d1_ids, d1_ids))
            for fid, tid, rtype, w, e_type, e_name in cur.fetchall():
                other = tid if fid in d1_ids else fid
                if other in seen_ids:
                    continue
                seen_ids.add(other)
                neighbors.append({
                    "id": other, "entity_type": e_type, "canonical_name": e_name,
                    "via_relation": rtype, "direction": "transitive",
                    "weight": float(w or 0.0), "source_count": 0, "distance": 2,
                })

    return {"entity": root, "neighbors": neighbors, "total": len(neighbors)}


class GraphShortestPathInput(BaseModel):
    from_entity: str = Field(description="Начальная сущность (canonical_name или alias).")
    to_entity: str = Field(description="Конечная сущность.")
    max_hops: int = Field(default=4, ge=1, le=6, description="Максимум рёбер в пути.")


@tool(
    name="graph_shortest_path",
    domain="kg",
    description=(
        "Находит кратчайший путь в knowledge graph между двумя сущностями (напр. «как "
        "связаны клиент Магнит и поставщик Внуковское»). Использует in-memory NetworkX "
        "граф с кешем 5 мин. Возвращает список хопов с relation_type и промежуточными "
        "сущностями или None если пути нет."
    ),
    input_model=GraphShortestPathInput,
)
def graph_shortest_path(from_entity: str, to_entity: str, max_hops: int = 4) -> dict:
    src = _resolve_entity(from_entity)
    dst = _resolve_entity(to_entity)
    if src is None:
        return {"error": f"from_entity «{from_entity}» не найдена"}
    if dst is None:
        return {"error": f"to_entity «{to_entity}» не найдена"}
    if src["id"] == dst["id"]:
        return {"path": [src], "hops": 0}

    G = _get_cached_graph()
    UG = G.to_undirected(as_view=True)
    try:
        path_ids = nx.shortest_path(UG, source=src["id"], target=dst["id"])
    except (nx.NetworkXNoPath, nx.NodeNotFound):
        return {"error": "нет пути между сущностями в текущем графе", "src": src, "dst": dst}

    if len(path_ids) - 1 > max_hops:
        return {"error": f"путь существует ({len(path_ids)-1} хопов), но превышает max_hops={max_hops}", "hops": len(path_ids) - 1}

    path: list[dict] = []
    for i, nid in enumerate(path_ids):
        node_attrs = G.nodes[nid]
        step = {
            "id": nid,
            "entity_type": node_attrs.get("entity_type"),
            "canonical_name": node_attrs.get("canonical_name"),
        }
        if i > 0:
            prev = path_ids[i - 1]
            # Направление ребра может быть любым в исходном DiGraph
            if G.has_edge(prev, nid):
                edata = G[prev][nid]
                step["via_from_previous"] = edata.get("relation_type") or (edata.get("relation_types") or [None])[0]
            elif G.has_edge(nid, prev):
                edata = G[nid][prev]
                step["via_from_previous_reverse"] = edata.get("relation_type") or (edata.get("relation_types") or [None])[0]
        path.append(step)

    return {"path": path, "hops": len(path_ids) - 1}


class EmployeeResponsibilityInput(BaseModel):
    person_name: str = Field(description="Имя сотрудника (ФИО или alias из km_entities.aliases).")


@tool(
    name="employee_responsibility",
    domain="kg",
    description=(
        "Профиль сотрудника из knowledge graph: должность, отдел, за что отвечает "
        "(responsible_for), с кем сотрудничает (collaborates_with), кому подчиняется "
        "(reports_to). Для вопросов «за что отвечает Иванов», «кто руководит Петровым», "
        "«кто работает в Производстве»."
    ),
    input_model=EmployeeResponsibilityInput,
)
def employee_responsibility(person_name: str) -> dict:
    emp = _resolve_entity(person_name, entity_type="employee")
    if emp is None:
        return {"error": f"Сотрудник «{person_name}» не найден"}

    out = {
        "employee": emp,
        "position": None,
        "department": None,
        "responsible_for": [],
        "collaborates_with": [],
        "reports_to": None,
        "subordinates": [],
    }
    with _db() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT r.relation_type, r.from_entity_id, r.to_entity_id, "
            "       e.entity_type, e.canonical_name "
            "FROM km_relations r "
            "JOIN km_entities e ON e.id = CASE WHEN r.from_entity_id = %s "
            "                                  THEN r.to_entity_id ELSE r.from_entity_id END "
            "WHERE (r.from_entity_id = %s OR r.to_entity_id = %s) "
            "AND (r.valid_to IS NULL OR r.valid_to >= CURRENT_DATE)",
            (emp["id"], emp["id"], emp["id"]),
        )
        for rtype, fid, tid, e_type, e_name in cur.fetchall():
            other_is_out = (fid == emp["id"])
            item = {"id": tid if other_is_out else fid, "type": e_type, "name": e_name}
            if rtype == "holds_position" and other_is_out and e_type == "position":
                out["position"] = item
            elif rtype == "works_in" and other_is_out and e_type == "department":
                out["department"] = item
            elif rtype == "responsible_for" and other_is_out:
                out["responsible_for"].append(item)
            elif rtype == "collaborates_with":
                out["collaborates_with"].append(item)
            elif rtype == "reports_to":
                if other_is_out:
                    out["reports_to"] = item
                else:
                    out["subordinates"].append(item)
    return out


def _main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    if len(sys.argv) < 2:
        print("usage: python3 -m tools.kg_graph {rebuild|stats}", file=sys.stderr)
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "rebuild":
        result = rebuild_entity_graph()
        print(
            f"✅ Rebuild done: {result['nodes']} nodes, {result['edges']} edges, "
            f"{result['communities']} communities, {result['updated_rows']} rows updated "
            f"in {result['elapsed_sec']}s"
        )
        for t in result["top_centrality"]:
            print(f"  • [{t['entity_type']}] {t['canonical_name']} — centrality {t['centrality']}")
    elif cmd == "stats":
        result = stats()
        for k, v in result.items():
            print(f"  {k}: {v}")
    else:
        print(f"unknown command: {cmd}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    # Загружаем .env как в остальных скриптах
    from dotenv import load_dotenv
    import pathlib
    env_path = pathlib.Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    _main()
