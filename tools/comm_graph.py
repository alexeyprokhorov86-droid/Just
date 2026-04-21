"""
Communication Graph — граф общения сотрудников из Telegram-чатов.

Собирает рёбра из tg_chat_*:
  - reply_to_message_id → A reply'ил B (сильный сигнал, weight=3.0)
  - co-participation в одном чате в одну неделю (слабый, weight=0.2)

Агрегирует в таблицу `comm_edges`, строит undirected-граф, считает Louvain
communities и betweenness. Пишет per-user метрики в `comm_users`.

CLI:
    python3 -m tools.comm_graph rebuild    # пересчёт всего
    python3 -m tools.comm_graph stats      # read-only снимок

RAG tools (домен `comm`):
    comm_neighbors(person_name) — с кем тесно общается
    comm_community(person_name) — в каком кластере, кто ещё в нём
    matrix_migration_wave(size) — следующая волна для Matrix-миграции

Интеграции с km_entities / v_current_staff / matrix_user_mapping:
  - tg_user_id → employee_ref_key через matrix_user_mapping.
  - employee_ref_key → km_entities.id через km_entities.attrs->>'ref_key'.
  - is_external из matrix_user_mapping (если помечено identification'ом).

Ограничения MVP (что НЕ делаем сейчас):
  - Matrix room co-participation (требует опроса Synapse API).
  - Email from→to pairs (81 mailbox, отдельная ingestion).
  - Time decay на weight (сейчас просто last_seen учитывается в отбор окна,
    веса не затухают экспоненциально — можно добавить позже).
"""
from __future__ import annotations

import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Literal

import networkx as nx
import psycopg2
from psycopg2.extras import execute_batch
from pydantic import BaseModel, Field

from ._db import get_conn
from .registry import tool

log = logging.getLogger("tools.comm_graph")

# ----------------------------------------------------------------------------
# Параметры ingestion
# ----------------------------------------------------------------------------
WINDOW_DAYS = 180          # собираем события за последние 180 дней
WEIGHT_REPLY = 3.0         # вес reply (на каждый факт)
WEIGHT_CO_ACTIVITY = 0.2   # co-participation per неделя
COMMUNITY_SEED = 42

_CACHE_TTL_SEC = 300
_cache: dict = {"graph": None, "ts": 0.0, "users": None}


# ----------------------------------------------------------------------------
# Rebuild
# ----------------------------------------------------------------------------

def _list_tg_chat_tables() -> list[str]:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT tablename FROM pg_tables WHERE tablename ~ '^tg_chat_\\d' ORDER BY tablename")
        return [r[0] for r in cur.fetchall()]


def _ingest_reply_edges(tables: list[str], since: datetime) -> dict[tuple[int, int], dict]:
    """A (reply author) → B (original author), weight = WEIGHT_REPLY * count."""
    edges: dict[tuple[int, int], dict] = {}
    for t in tables:
        with get_conn() as conn, conn.cursor() as cur:
            try:
                cur.execute(
                    f"""
                    SELECT m.user_id AS from_uid, orig.user_id AS to_uid, count(*) AS c, max(m.created_at) AS last_ts
                    FROM "{t}" m
                    JOIN "{t}" orig ON orig.message_id = m.reply_to_message_id
                    WHERE m.reply_to_message_id IS NOT NULL
                      AND m.user_id IS NOT NULL
                      AND orig.user_id IS NOT NULL
                      AND m.user_id <> orig.user_id
                      AND m.created_at >= %s
                    GROUP BY m.user_id, orig.user_id
                    """,
                    (since,),
                )
                for from_uid, to_uid, c, last_ts in cur.fetchall():
                    key = (from_uid, to_uid)
                    e = edges.setdefault(
                        key,
                        {"weight": 0.0, "occurrences": 0, "last_seen": None, "signal_type": "reply"},
                    )
                    e["weight"] += WEIGHT_REPLY * c
                    e["occurrences"] += c
                    if last_ts and (e["last_seen"] is None or last_ts > e["last_seen"]):
                        e["last_seen"] = last_ts
            except Exception as ex:
                log.warning("reply scan failed on %s: %s", t, ex)
    log.info("reply edges collected: %d pairs across %d chats", len(edges), len(tables))
    return edges


def _ingest_coactivity_edges(tables: list[str], since: datetime) -> dict[tuple[int, int], dict]:
    """Co-participation: все пары user'ов, активных в чате на ОДНОЙ неделе.

    Вес WEIGHT_CO_ACTIVITY за каждую такую неделю. Симметричный сигнал,
    сохраняем only для (a < b) чтобы не дублировать (слияние с reply будет
    при merge в comm_edges).

    Фильтр: чат не должен быть «публичным broadcast» — если > 40 активных
    участников за окно, скипаем (сильно шумно: каждый vs каждый).
    """
    edges: dict[tuple[int, int], dict] = {}
    for t in tables:
        with get_conn() as conn, conn.cursor() as cur:
            try:
                cur.execute(
                    f"""
                    SELECT date_trunc('week', created_at) AS wk, user_id
                    FROM "{t}"
                    WHERE created_at >= %s AND user_id IS NOT NULL
                    GROUP BY date_trunc('week', created_at), user_id
                    """,
                    (since,),
                )
                week_users: dict = {}
                for wk, uid in cur.fetchall():
                    week_users.setdefault(wk, set()).add(uid)

                # Пропускаем чрезмерно шумные чаты
                max_users = max((len(s) for s in week_users.values()), default=0)
                if max_users > 40:
                    continue

                for wk, uids in week_users.items():
                    uids_sorted = sorted(uids)
                    for i in range(len(uids_sorted)):
                        for j in range(i + 1, len(uids_sorted)):
                            a, b = uids_sorted[i], uids_sorted[j]
                            key = (a, b)
                            e = edges.setdefault(
                                key,
                                {"weight": 0.0, "occurrences": 0, "last_seen": None, "signal_type": "co_activity"},
                            )
                            e["weight"] += WEIGHT_CO_ACTIVITY
                            e["occurrences"] += 1
                            ts = wk + timedelta(days=6)
                            if e["last_seen"] is None or ts > e["last_seen"]:
                                e["last_seen"] = ts
            except Exception as ex:
                log.warning("co-activity scan failed on %s: %s", t, ex)
    log.info("co-activity edges collected: %d pairs", len(edges))
    return edges


def _ingest_user_profiles(tables: list[str]) -> dict[int, dict]:
    """Собирает профили юзеров из tg_chat_*: последний known display_name, username, last_active."""
    profiles: dict[int, dict] = {}
    for t in tables:
        with get_conn() as conn, conn.cursor() as cur:
            try:
                cur.execute(
                    f"""
                    SELECT DISTINCT ON (user_id) user_id, first_name, last_name, username, created_at
                    FROM "{t}"
                    WHERE user_id IS NOT NULL
                    ORDER BY user_id, created_at DESC
                    """,
                )
                for uid, first, last, uname, ts in cur.fetchall():
                    if uid in profiles and profiles[uid]["last_active"] and profiles[uid]["last_active"] > (ts or datetime.min):
                        continue
                    display = " ".join(x for x in [first, last] if x) or uname or str(uid)
                    profiles[uid] = {
                        "display_name": display,
                        "username": uname,
                        "last_active": ts,
                    }
            except Exception as ex:
                log.warning("profile scan failed on %s: %s", t, ex)
    log.info("user profiles collected: %d unique tg users", len(profiles))
    return profiles


def _merge_mapping(profiles: dict[int, dict]) -> None:
    """Дополняет profiles данными из matrix_user_mapping: is_external, matrix_joined,
    employee_ref_key, km_entity_id."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT m.telegram_user_id, m.employee_ref_key::text, m.is_external,
                   (m.joined_at IS NOT NULL) AS matrix_joined,
                   e.id AS km_entity_id
            FROM matrix_user_mapping m
            LEFT JOIN km_entities e
              ON e.entity_type = 'employee'
              AND e.attrs->>'ref_key' = m.employee_ref_key::text
            """,
        )
        for tg_uid, ref_key, is_ext, joined, km_id in cur.fetchall():
            if tg_uid in profiles:
                profiles[tg_uid]["employee_ref_key"] = ref_key
                profiles[tg_uid]["is_external"] = is_ext
                profiles[tg_uid]["matrix_joined"] = joined
                profiles[tg_uid]["km_entity_id"] = km_id
            else:
                # Юзер в matrix mapping, но не в tg-сообщениях (внесён руками)
                profiles[tg_uid] = {
                    "display_name": None,
                    "username": None,
                    "last_active": None,
                    "employee_ref_key": ref_key,
                    "is_external": is_ext,
                    "matrix_joined": joined,
                    "km_entity_id": km_id,
                }


def _write_edges(reply_edges: dict, coact_edges: dict) -> int:
    # Сначала очищаем старое
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE comm_edges RESTART IDENTITY")
        conn.commit()

    rows = []
    for (f, t), e in reply_edges.items():
        rows.append((f, t, "reply", e["weight"], e["occurrences"], e["last_seen"]))
    for (a, b), e in coact_edges.items():
        rows.append((a, b, "co_activity", e["weight"], e["occurrences"], e["last_seen"]))

    if not rows:
        return 0

    with get_conn() as conn, conn.cursor() as cur:
        execute_batch(
            cur,
            """
            INSERT INTO comm_edges
                (from_tg_user_id, to_tg_user_id, signal_type, weight, occurrences, last_seen)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            rows,
            page_size=500,
        )
        conn.commit()
    return len(rows)


def _write_users(profiles: dict[int, dict], metrics: dict[int, dict]) -> int:
    if not profiles:
        return 0
    now = datetime.utcnow()
    rows = []
    for uid, p in profiles.items():
        m = metrics.get(uid, {})
        rows.append((
            uid,
            p.get("display_name"),
            p.get("username"),
            p.get("km_entity_id"),
            p.get("employee_ref_key"),
            p.get("is_external"),
            p.get("matrix_joined"),
            m.get("community_id"),
            m.get("betweenness"),
            m.get("degree_weighted"),
            p.get("last_active"),
            now,
        ))

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE comm_users")
        execute_batch(
            cur,
            """
            INSERT INTO comm_users
                (tg_user_id, display_name, username, km_entity_id, employee_ref_key,
                 is_external, matrix_joined, community_id, betweenness, degree_weighted,
                 last_active, computed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            rows,
            page_size=500,
        )
        conn.commit()
    return len(rows)


def _build_graph_from_edges() -> nx.Graph:
    """Читаем comm_edges, строим undirected Graph с суммированием весов по типам."""
    G = nx.Graph()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT from_tg_user_id, to_tg_user_id, signal_type, weight
            FROM comm_edges
            """,
        )
        for f, t, _sig, w in cur.fetchall():
            a, b = (f, t) if f < t else (t, f)
            if G.has_edge(a, b):
                G[a][b]["weight"] += float(w)
            else:
                G.add_edge(a, b, weight=float(w))
    log.info("comm graph built: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())
    return G


def _compute_metrics(G: nx.Graph) -> dict[int, dict]:
    if G.number_of_nodes() == 0:
        return {}
    t0 = time.time()

    communities = list(nx.community.louvain_communities(G, seed=COMMUNITY_SEED, weight="weight"))
    communities.sort(key=lambda c: -len(c))
    node_community: dict[int, int] = {}
    for cid, members in enumerate(communities):
        for node in members:
            node_community[node] = cid

    # Betweenness на графе до 5k узлов = сравнительно быстро.
    # Для бОльших графов нужен approximated (k=500 sampling).
    if G.number_of_nodes() <= 2000:
        bc = nx.betweenness_centrality(G, weight="weight", normalized=True)
    else:
        bc = nx.betweenness_centrality(G, k=min(500, G.number_of_nodes() // 3), weight="weight", normalized=True)

    degree_w = dict(G.degree(weight="weight"))

    log.info(
        "metrics: %d communities, betweenness+degree_w in %.2fs",
        len(communities), time.time() - t0,
    )
    return {
        n: {
            "community_id": node_community.get(n, -1),
            "betweenness": float(bc.get(n, 0.0)),
            "degree_weighted": float(degree_w.get(n, 0.0)),
        }
        for n in G.nodes()
    }


def rebuild_comm_graph() -> dict:
    t0 = time.time()
    since = datetime.utcnow() - timedelta(days=WINDOW_DAYS)
    tables = _list_tg_chat_tables()

    reply_edges = _ingest_reply_edges(tables, since)
    coact_edges = _ingest_coactivity_edges(tables, since)
    profiles = _ingest_user_profiles(tables)
    _merge_mapping(profiles)

    edges_written = _write_edges(reply_edges, coact_edges)
    G = _build_graph_from_edges()
    metrics = _compute_metrics(G)
    users_written = _write_users(profiles, metrics)

    # Инвалидация кеша tools
    _cache["graph"] = None
    _cache["ts"] = 0.0

    return {
        "tables_scanned": len(tables),
        "reply_pairs": len(reply_edges),
        "coactivity_pairs": len(coact_edges),
        "edges_written": edges_written,
        "nodes": G.number_of_nodes(),
        "communities": len({m["community_id"] for m in metrics.values()}) if metrics else 0,
        "users_written": users_written,
        "elapsed_sec": round(time.time() - t0, 2),
    }


def stats() -> dict:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT count(*), max(computed_at) FROM comm_edges")
        edges, edges_ts = cur.fetchone()
        cur.execute("SELECT count(*), count(DISTINCT community_id), max(computed_at) FROM comm_users")
        users, communities, users_ts = cur.fetchone()
        cur.execute("SELECT count(*) FROM comm_users WHERE km_entity_id IS NOT NULL")
        (mapped,) = cur.fetchone()
    return {
        "edges": edges,
        "edges_computed_at": edges_ts.isoformat() if edges_ts else None,
        "users": users,
        "communities": communities,
        "users_with_km_match": mapped,
        "users_computed_at": users_ts.isoformat() if users_ts else None,
    }


# ----------------------------------------------------------------------------
# RAG tools
# ----------------------------------------------------------------------------

def _resolve_user(name_or_id: str) -> dict | None:
    """Резолвит по: tg_user_id (число), tg username (@X или X), display_name,
    employee_ref_key. Возвращает строку comm_users."""
    if not name_or_id or not str(name_or_id).strip():
        return None
    q = str(name_or_id).strip().lstrip("@")
    with get_conn() as conn, conn.cursor() as cur:
        if q.isdigit():
            cur.execute(
                "SELECT tg_user_id, display_name, username, km_entity_id, employee_ref_key, "
                "       is_external, matrix_joined, community_id, betweenness, degree_weighted, last_active "
                "FROM comm_users WHERE tg_user_id = %s", (int(q),),
            )
        else:
            cur.execute(
                """
                SELECT tg_user_id, display_name, username, km_entity_id, employee_ref_key,
                       is_external, matrix_joined, community_id, betweenness, degree_weighted, last_active
                FROM comm_users
                WHERE LOWER(COALESCE(display_name, '')) = LOWER(%s)
                   OR LOWER(COALESCE(username, '')) = LOWER(%s)
                   OR employee_ref_key = %s
                ORDER BY degree_weighted DESC NULLS LAST
                LIMIT 1
                """,
                (q, q, q),
            )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "tg_user_id": row[0], "display_name": row[1], "username": row[2],
        "km_entity_id": row[3], "employee_ref_key": row[4], "is_external": row[5],
        "matrix_joined": row[6], "community_id": row[7], "betweenness": row[8],
        "degree_weighted": row[9], "last_active": row[10].isoformat() if row[10] else None,
    }


class CommNeighborsInput(BaseModel):
    person_name: str = Field(description="Имя/юзернейм/tg_user_id/employee_ref_key человека.")
    limit: int = Field(default=15, ge=1, le=100)


@tool(
    name="comm_neighbors",
    domain="comm",
    description=(
        "Топ-N собеседников человека в Telegram-чатах компании (по суммарному "
        "весу сигналов reply + co-activity за последние 180 дней). Помогает "
        "понять кто с кем реально взаимодействует, не полагаясь на формальную "
        "иерархию."
    ),
    input_model=CommNeighborsInput,
)
def comm_neighbors(person_name: str, limit: int = 15) -> dict:
    root = _resolve_user(person_name)
    if not root:
        return {"error": f"Человек «{person_name}» не найден в comm_users (может comm_graph не запущен?)"}

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            WITH edges AS (
                SELECT CASE WHEN from_tg_user_id = %s THEN to_tg_user_id ELSE from_tg_user_id END AS other_id,
                       signal_type, weight, occurrences, last_seen
                FROM comm_edges
                WHERE from_tg_user_id = %s OR to_tg_user_id = %s
            ), agg AS (
                SELECT other_id,
                       sum(weight) AS total_weight,
                       sum(occurrences) AS total_occ,
                       max(last_seen) AS last_seen,
                       sum(CASE WHEN signal_type='reply' THEN weight ELSE 0 END) AS reply_weight,
                       sum(CASE WHEN signal_type='co_activity' THEN weight ELSE 0 END) AS coact_weight
                FROM edges GROUP BY other_id
            )
            SELECT a.other_id, u.display_name, u.username, u.community_id, u.is_external,
                   a.total_weight, a.total_occ, a.last_seen, a.reply_weight, a.coact_weight
            FROM agg a
            LEFT JOIN comm_users u ON u.tg_user_id = a.other_id
            ORDER BY a.total_weight DESC
            LIMIT %s
            """,
            (root["tg_user_id"], root["tg_user_id"], root["tg_user_id"], limit),
        )
        neighbors = [
            {
                "tg_user_id": r[0], "display_name": r[1], "username": r[2],
                "community_id": r[3], "is_external": r[4],
                "total_weight": round(float(r[5] or 0), 2),
                "occurrences": int(r[6] or 0),
                "last_seen": r[7].isoformat() if r[7] else None,
                "reply_weight": round(float(r[8] or 0), 2),
                "coact_weight": round(float(r[9] or 0), 2),
            }
            for r in cur.fetchall()
        ]
    return {"person": root, "neighbors": neighbors}


class CommCommunityInput(BaseModel):
    person_name: str = Field(description="Имя/юзернейм/tg_user_id/employee_ref_key.")
    limit: int = Field(default=50, ge=1, le=500)


@tool(
    name="comm_community",
    domain="comm",
    description=(
        "Возвращает community (кластер собеседников) указанного человека: "
        "всех других юзеров того же кластера, отсортированных по "
        "degree_weighted. Для понимания «в каком отделе/проекте живёт этот "
        "человек по реальным коммуникациям»."
    ),
    input_model=CommCommunityInput,
)
def comm_community(person_name: str, limit: int = 50) -> dict:
    root = _resolve_user(person_name)
    if not root:
        return {"error": f"Человек «{person_name}» не найден"}
    if root["community_id"] is None:
        return {"error": "community_id не посчитан (rebuild требуется)", "person": root}
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT tg_user_id, display_name, username, degree_weighted, betweenness,
                   is_external, matrix_joined, employee_ref_key
            FROM comm_users
            WHERE community_id = %s
            ORDER BY degree_weighted DESC NULLS LAST
            LIMIT %s
            """,
            (root["community_id"], limit),
        )
        members = [
            {
                "tg_user_id": r[0], "display_name": r[1], "username": r[2],
                "degree_weighted": round(float(r[3] or 0), 2),
                "betweenness": round(float(r[4] or 0), 4),
                "is_external": r[5], "matrix_joined": r[6],
                "employee_ref_key": r[7],
            }
            for r in cur.fetchall()
        ]
    return {"person": root, "community_id": root["community_id"], "size": len(members), "members": members}


class MatrixMigrationWaveInput(BaseModel):
    wave_size: int = Field(default=15, ge=1, le=100, description="Сколько сотрудников выбрать для текущей волны приглашений.")


@tool(
    name="matrix_migration_wave",
    domain="comm",
    description=(
        "Предлагает список сотрудников для следующей волны приглашений в "
        "Matrix/Element. Логика: ещё не приглашённые + не внешние + с "
        "высоким degree_weighted, сгруппированные по community (миграция по "
        "кластеру общения даёт immediate value — коллегам сразу есть с кем "
        "переписываться в Matrix)."
    ),
    input_model=MatrixMigrationWaveInput,
)
def matrix_migration_wave(wave_size: int = 15) -> dict:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT community_id, count(*) AS total,
                   count(*) FILTER (WHERE matrix_joined IS NOT TRUE AND is_external IS NOT TRUE) AS ready
            FROM comm_users
            WHERE community_id IS NOT NULL
            GROUP BY community_id
            HAVING count(*) FILTER (WHERE matrix_joined IS NOT TRUE AND is_external IS NOT TRUE) > 0
            ORDER BY ready DESC, total DESC
            """,
        )
        communities = cur.fetchall()
    if not communities:
        return {"note": "нет кандидатов — все не-внешние сотрудники уже в Matrix или comm_graph пуст"}

    picks: list[dict] = []
    with get_conn() as conn, conn.cursor() as cur:
        for cid, _total, ready in communities:
            if len(picks) >= wave_size:
                break
            take = min(wave_size - len(picks), int(ready))
            cur.execute(
                """
                SELECT tg_user_id, display_name, username, employee_ref_key,
                       community_id, degree_weighted, betweenness, last_active
                FROM comm_users
                WHERE community_id = %s
                  AND (matrix_joined IS NOT TRUE)
                  AND (is_external IS NOT TRUE)
                ORDER BY degree_weighted DESC NULLS LAST
                LIMIT %s
                """,
                (cid, take),
            )
            for r in cur.fetchall():
                picks.append({
                    "tg_user_id": r[0], "display_name": r[1], "username": r[2],
                    "employee_ref_key": r[3], "community_id": r[4],
                    "degree_weighted": round(float(r[5] or 0), 2),
                    "betweenness": round(float(r[6] or 0), 4),
                    "last_active": r[7].isoformat() if r[7] else None,
                })
    return {"wave_size": len(picks), "picks": picks, "communities_touched": len({p["community_id"] for p in picks})}


def _main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    if len(sys.argv) < 2:
        print("usage: python3 -m tools.comm_graph {rebuild|stats}", file=sys.stderr)
        sys.exit(1)
    cmd = sys.argv[1]
    if cmd == "rebuild":
        result = rebuild_comm_graph()
        print("✅ comm_graph rebuild:")
        for k, v in result.items():
            print(f"  {k}: {v}")
    elif cmd == "stats":
        for k, v in stats().items():
            print(f"  {k}: {v}")
    else:
        print(f"unknown: {cmd}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    from dotenv import load_dotenv
    import pathlib
    env_path = pathlib.Path(__file__).resolve().parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
    _main()
