"""
Distillation Pipeline — извлечение знаний из source_documents.
Использует GPT-4.1-mini для extraction сущностей, фактов, решений.
"""
import psycopg2
import psycopg2.extras
import json
import os
import time
import requests
from dotenv import load_dotenv
from embedding_service import create_embedding

load_dotenv('/home/admin/telegram_logger_bot/.env')

CONFIG_PG = {
    'host': os.getenv('DB_HOST', '172.20.0.2'),
    'port': 5432,
    'dbname': 'knowledge_base',
    'user': 'knowledge',
    'password': os.getenv('DB_PASSWORD'),
}

ROUTER_AI_URL = os.getenv('ROUTERAI_BASE_URL', 'https://routerai.ru/api/v1')
ROUTER_AI_KEY = os.getenv('ROUTERAI_API_KEY', '')

EXTRACTION_SYSTEM_PROMPT = """Ты — система извлечения корпоративных знаний для кондитерской компании Фрумелад.
Из каждого сообщения извлеки ВСЕ что найдёшь из следующих категорий.
Все сообщения в батче — из ОДНОГО чата/канала, расположены хронологически. Анализируй их как связанную переписку, учитывай контекст предыдущих сообщений.

НЕ извлекай:
- Персональные данные (паспорта, ИНН физлиц, номера водительских удостоверений)
- Дубликаты уже извлечённых фактов
- Приветствия, благодарности, общие фразы без конкретного содержания

Отвечай ТОЛЬКО валидным JSON без markdown-обёрток. Структура:
{
  "facts": [
    {
      "fact_type": "тип",
      "subject": "кто/что (имя или название)",
      "object": "кого/чего (если есть)",
      "text": "краткая формулировка факта",
      "confidence": 0.9
    }
  ],
  "decisions": [
    {
      "text": "формулировка решения",
      "scope": "supplier/product/process/department/company",
      "decided_by": "кто принял (если известно)",
      "importance": 0.7
    }
  ],
  "relations": [
    {
      "from_name": "сущность 1",
      "from_type": "employee/contractor/sku/department",
      "relation": "тип связи",
      "to_name": "сущность 2",
      "to_type": "employee/contractor/sku/department"
    }
  ],
  "tasks": [
    {
      "assignee": "кому поручено",
      "task_text": "что нужно сделать",
      "deadline": "срок если указан или null"
    }
  ],
  "policies": [
    {
      "text": "формулировка правила/нормы",
      "scope": "supplier/product/process/department/company"
    }
  ],
  "procedures": [
    {
      "name": "название процедуры",
      "text": "описание процедуры",
      "process_type": "закупка/рекламация/приёмка/отгрузка/согласование/производство/другое",
      "steps": ["шаг 1", "шаг 2"]
    }
  ],
  "cases": [
    {
      "title": "краткое название кейса",
      "problem": "описание проблемы",
      "resolution": "как решили",
      "outcome": "успех/неудача/частично",
      "lessons": "что извлекли из опыта"
    }
  ]
}
Типы фактов (fact_type): supplies, responsible_for, quality_issue, pricing, delivery_terms, procedure_step, policy, complaint, stock_info, production_info, hr_info, financial_info
Типы связей (relation): manages, reports_to, responsible_for, supplies, complains_about, approves, replaces, collaborates_with
Типы правил (policies): правила работы с поставщиками, клиентами, качеством, документооборотом, согласованием
Типы процедур (procedures): пошаговые инструкции, регламенты, алгоритмы действий
Типы кейсов (cases): инциденты, решённые проблемы, прецеденты с поставщиками/клиентами/производством

Извлекай policies когда видишь утверждённые правила, нормы, требования.
Извлекай procedures когда видишь пошаговые инструкции или описания процессов.
Извлекай cases когда видишь описание проблемы и её решения.
Пример извлечения из письма:
Письмо: "Добрый день, по результатам встречи с Белый Город решили зафиксировать цену на сахар-песок 67 руб/кг до конца квартала. Андрей, оформи договор до пятницы. Напоминаю: все закупки свыше 500к согласовываем с Алексеем."

Правильный ответ:
{
  "facts": [
    {"fact_type": "pricing", "subject": "Белый Город", "object": "сахар-песок", "text": "Цена на сахар-песок от Белый Город зафиксирована 67 руб/кг до конца квартала", "confidence": 0.95}
  ],
  "decisions": [
    {"text": "Зафиксировать цену на сахар-песок 67 руб/кг от Белый Город до конца квартала", "scope": "supplier", "decided_by": null, "importance": 0.8}
  ],
  "relations": [
    {"from_name": "Белый Город", "from_type": "contractor", "relation": "supplies", "to_name": "сахар-песок", "to_type": "sku"}
  ],
  "tasks": [
    {"assignee": "Андрей", "task_text": "Оформить договор с Белый Город на фиксацию цены сахар-песок", "deadline": "пятница"}
  ],
  "policies": [
    {"text": "Все закупки свыше 500 000 руб согласовываются с Алексеем", "scope": "company"}
  ],
  "procedures": [],
  "cases": []
}

Пример кейса:
Письмо: "По рекламации от Лента — обнаружен посторонний предмет в партии медовиков. Провели расследование: причина — износ сита на линии 2. Заменили сито, усилили контроль на входе сырья. Лента приняла результаты, штраф отменён."

Правильный ответ:
{
  "facts": [
    {"fact_type": "quality_issue", "subject": "Лента", "object": "медовик", "text": "Рекламация от Лента: посторонний предмет в партии медовиков", "confidence": 0.95}
  ],
  "decisions": [
    {"text": "Заменить сито на линии 2 и усилить контроль на входе сырья", "scope": "process", "decided_by": null, "importance": 0.9}
  ],
  "relations": [],
  "tasks": [],
  "policies": [],
  "procedures": [],
  "cases": [
    {"title": "Рекламация Лента — посторонний предмет в медовиках", "problem": "Обнаружен посторонний предмет в партии медовиков при поставке в Лента", "resolution": "Расследование выявило износ сита на линии 2. Сито заменено, усилен контроль входного сырья", "outcome": "успех", "lessons": "Необходим регулярный контроль износа сит на производственных линиях"}
  ]
}
Если категория пуста — возвращай пустой массив []."""


def call_llm(messages, temperature=0.1):
    """Вызов LLM через RouterAI."""
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {ROUTER_AI_KEY}'
    }
    payload = {
        'model': 'openai/gpt-4.1-mini',
        'messages': messages,
        'temperature': temperature,
        'max_tokens': 4000
    }
    
    resp = requests.post(
        f'{ROUTER_AI_URL}/chat/completions',
        headers=headers,
        json=payload,
        timeout=60
    )
    resp.raise_for_status()
    return resp.json()['choices'][0]['message']['content']


def extract_knowledge(messages_batch):
    """Извлечь знания из пачки сообщений."""
    combined = []
    for msg in messages_batch:
        header = f"[{msg['doc_date']}] {msg['author_name']} в {msg['channel_name']}:"
        combined.append(f"{header}\n{msg['body_text'][:2000]}")
    
    user_content = "\n\n---\n\n".join(combined)
    
    result = call_llm([
        {'role': 'system', 'content': EXTRACTION_SYSTEM_PROMPT},
        {'role': 'user', 'content': user_content}
    ])
    
    # Парсим JSON
    result = result.strip()
    if result.startswith('```'):
        result = result.split('\n', 1)[1].rsplit('```', 1)[0]
    
    return json.loads(result)


def resolve_entity(cur, name, entity_type=None):
    """Найти или создать сущность в km_entities по имени."""
    if not name or len(name.strip()) < 2:
        return None
    
    name = name.strip()
    
    # 1. Точное совпадение
    if entity_type:
        cur.execute(
            "SELECT id FROM km_entities WHERE canonical_name = %s AND entity_type = %s LIMIT 1",
            (name, entity_type)
        )
    else:
        cur.execute(
            "SELECT id FROM km_entities WHERE canonical_name = %s LIMIT 1",
            (name,)
        )
    row = cur.fetchone()
    if row:
        return row[0]
    
    # 2. Поиск по ILIKE (регистронезависимый)
    if entity_type:
        cur.execute(
            "SELECT id FROM km_entities WHERE LOWER(canonical_name) = LOWER(%s) AND entity_type = %s LIMIT 1",
            (name, entity_type)
        )
    else:
        cur.execute(
            "SELECT id FROM km_entities WHERE LOWER(canonical_name) = LOWER(%s) LIMIT 1",
            (name,)
        )
    row = cur.fetchone()
    if row:
        return row[0]
    
    # 3. Fuzzy — поиск по trigram similarity (pg_trgm)
    try:
        if entity_type:
            cur.execute("""
                SELECT id, similarity(canonical_name, %s) as sim
                FROM km_entities
                WHERE entity_type = %s AND similarity(canonical_name, %s) > 0.4
                ORDER BY sim DESC LIMIT 1
            """, (name, entity_type, name))
        else:
            cur.execute("""
                SELECT id, similarity(canonical_name, %s) as sim
                FROM km_entities
                WHERE similarity(canonical_name, %s) > 0.4
                ORDER BY sim DESC LIMIT 1
            """, (name, name))
        row = cur.fetchone()
        if row:
            return row[0]
    except Exception:
        pass
    
    # 4. Автосоздание новой сущности
    if not entity_type:
        # Определяем тип по контексту
        name_lower = name.lower()
        if any(w in name_lower for w in ['ооо', 'оао', 'ип ', 'зао', 'пао', 'ltd', 'inc']):
            entity_type = 'contractor'
        elif '@' in name:
            entity_type = 'contractor'
        else:
            entity_type = 'other'
    
    try:
        cur.execute("""
            INSERT INTO km_entities (entity_type, canonical_name, status, confidence)
            VALUES (%s, %s, 'auto_created', 0.6)
            ON CONFLICT DO NOTHING
            RETURNING id
        """, (entity_type, name[:255]))
        row = cur.fetchone()
        if not row:
            cur.execute(
                "SELECT id FROM km_entities WHERE canonical_name = %s LIMIT 1",
                (name[:255],)
            )
            row = cur.fetchone()
        if row:
            try:
                emb = create_embedding(name)
                cur.execute("UPDATE km_entities SET embedding = %s WHERE id = %s", (str(emb), row[0]))
            except Exception:
                pass
            return row[0]
        if row:
            try:
                emb = create_embedding(name)
                cur.execute("UPDATE km_entities SET embedding = %s WHERE id = %s", (str(emb), row[0]))
            except Exception:
                pass
            return row[0]
    except Exception:
        pass
    
    return None

# Кэш правил фильтрации (обновляется раз в запуск)
_filter_rules_cache = None

def load_filter_rules(conn):
    """Загружает правила фильтрации из km_filter_rules."""
    global _filter_rules_cache
    cur = conn.cursor()
    cur.execute("""
        SELECT rule_type, target, value FROM km_filter_rules 
        WHERE is_active = true AND approval_status = 'active'
    """)
    rules = {'junk_words': {'facts': [], 'decisions': [], 'all': []},
             'safe_words': [],
             'min_length': {'facts': 0, 'decisions': 0}}
    
    for rule_type, target, value in cur.fetchall():
        if rule_type == 'junk_word':
            rules['junk_words'][target].append(value.lower())
        elif rule_type == 'safe_word':
            rules['safe_words'].append(value.lower())
        elif rule_type == 'min_length':
            rules['min_length'][target] = int(value)
    
    cur.close()
    _filter_rules_cache = rules
    return rules


def get_filter_rules(conn):
    """Возвращает кэшированные правила или загружает."""
    global _filter_rules_cache
    if _filter_rules_cache is None:
        return load_filter_rules(conn)
    return _filter_rules_cache


def is_junk(text, target='facts', conn=None):
    """Проверяет текст по правилам из km_filter_rules. True = мусор."""
    if not text or not text.strip():
        return True
    
    rules = get_filter_rules(conn) if conn else _filter_rules_cache
    if not rules:
        return False
    
    # Проверка минимальной длины
    min_len = rules['min_length'].get(target, 0)
    if min_len and len(text.strip()) < min_len:
        return True
    
    t = text.lower()

    # Проверка safe_words — если содержит защищённое слово, НЕ мусор
    for safe in rules.get('safe_words', []):
        if safe in t:
            return False
    
    # Проверка junk_words для этого target + 'all'
    for word in rules['junk_words'].get(target, []) + rules['junk_words'].get('all', []):
        if word in t:
            return True
    
    return False


def update_hit_counts(conn, hits):
    """Обновляет счётчики срабатываний правил."""
    if not hits:
        return
    cur = conn.cursor()
    for value in hits:
        cur.execute("""
            UPDATE km_filter_rules SET hit_count = hit_count + 1, updated_at = NOW()
            WHERE value = %s AND is_active = true
        """, (value,))
    conn.commit()
    cur.close()

def save_extraction(cur, extracted, doc_ids, conn=None):
    """Сохранить извлечённые знания в km_* таблицы."""
    stats = {'facts': 0, 'decisions': 0, 'relations': 0, 'tasks': 0, 'policies': 0, 'procedures': 0, 'cases': 0}
    
    # Факты
    for fact in extracted.get('facts', []):
        if is_junk(fact.get('text', ''), 'facts', conn):
            continue
        subject_id = resolve_entity(cur, fact.get('subject'))
        object_id = resolve_entity(cur, fact.get('object'))
        
        cur.execute("""
            INSERT INTO km_facts (fact_type, subject_entity_id, object_entity_id,
                fact_text, confidence, verification_status)
            VALUES (%s, %s, %s, %s, %s, 'extracted')
            RETURNING id
        """, (
            fact.get('fact_type', 'general'),
            subject_id,
            object_id,
            fact.get('text', ''),
            fact.get('confidence', 0.8)
        ))
        fact_row = cur.fetchone()
        fact_id = fact_row[0] if fact_row else None
        stats['facts'] += 1
        if fact_id:
            fact['_id'] = fact_id
            try:
                emb = create_embedding(fact.get('text', ''))
                cur.execute("UPDATE km_facts SET embedding = %s WHERE id = %s", (str(emb), fact_id))
            except Exception:
                pass
    
    # Решения
    for dec in extracted.get('decisions', []):
        if is_junk(dec.get('text', ''), 'decisions', conn):
            continue
        decided_by_id = resolve_entity(cur, dec.get('decided_by'), 'employee')
        scope_entity_id = None
        
        cur.execute("""
            INSERT INTO km_decisions (decision_text, scope_type, scope_entity_id,
                decided_by_entity_id, importance, confidence)
            VALUES (%s, %s, %s, %s, %s, 0.8)
            RETURNING id
        """, (
            dec.get('text', ''),
            dec.get('scope', 'company'),
            scope_entity_id,
            decided_by_id,
            dec.get('importance', 0.5)
        ))
        dec_row = cur.fetchone()
        dec_id = dec_row[0] if dec_row else None
        stats['decisions'] += 1
        if dec_id:
            dec['_id'] = dec_id
            try:
                emb = create_embedding(dec.get('text', ''))
                cur.execute("UPDATE km_decisions SET embedding = %s WHERE id = %s", (str(emb), dec_id))
            except Exception:
                pass
    
    # Связи
    for rel in extracted.get('relations', []):
        from_id = resolve_entity(cur, rel.get('from_name'), rel.get('from_type'))
        to_id = resolve_entity(cur, rel.get('to_name'), rel.get('to_type'))
        
        if from_id and to_id:
            cur.execute("""
                INSERT INTO km_relations (from_entity_id, relation_type, to_entity_id)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (from_id, rel.get('relation', 'related_to'), to_id))
            stats['relations'] += 1
     # Задачи
    for task in extracted.get('tasks', []):
        assignee_id = resolve_entity(cur, task.get('assignee'), 'employee')
        deadline = None
        if task.get('deadline'):
            try:
                from datetime import datetime
                deadline = datetime.strptime(task['deadline'], '%Y-%m-%d').date()
            except:
                pass
        
        cur.execute("""
            INSERT INTO km_tasks (task_text, assignee_entity_id, source_document_id,
                deadline, confidence)
            VALUES (%s, %s, %s, %s, 0.8)
            RETURNING id
        """, (
            task.get('task_text', '')[:500],
            assignee_id,
            doc_ids[0] if doc_ids else None,
            deadline
        ))
        task_row = cur.fetchone()
        task_id = task_row[0] if task_row else None
        stats['tasks'] += 1
        if task_id:
            try:
                emb = create_embedding(task.get('task_text', ''))
                cur.execute("UPDATE km_tasks SET embedding = %s WHERE id = %s", (str(emb), task_id))
            except Exception:
                pass

    # Политики/правила
    for pol in extracted.get('policies', []):
        cur.execute("""
            INSERT INTO km_policies (policy_text, scope_type, confidence)
            VALUES (%s, %s, 0.8)
            RETURNING id
        """, (pol.get('text', '')[:500], pol.get('scope', 'company')))
        pol_id = cur.fetchone()[0]
        try:
            emb = create_embedding(pol.get('text', ''))
            cur.execute("UPDATE km_policies SET embedding = %s WHERE id = %s", (str(emb), pol_id))
        except Exception:
            pass
        stats['policies'] = stats.get('policies', 0) + 1

    # Процедуры
    for proc in extracted.get('procedures', []):
        steps_json = json.dumps(proc.get('steps', []), ensure_ascii=False) if proc.get('steps') else None
        cur.execute("""
            INSERT INTO km_procedures (procedure_name, procedure_text, process_type, steps, confidence)
            VALUES (%s, %s, %s, %s, 0.8)
            RETURNING id
        """, (
            proc.get('name', '')[:300],
            proc.get('text', '')[:500],
            proc.get('process_type', 'другое'),
            steps_json
        ))
        proc_id = cur.fetchone()[0]
        try:
            emb = create_embedding(proc.get('text', ''))
            cur.execute("UPDATE km_procedures SET embedding = %s WHERE id = %s", (str(emb), proc_id))
        except Exception:
            pass
        stats['procedures'] = stats.get('procedures', 0) + 1

    # Кейсы
    for case in extracted.get('cases', []):
        cur.execute("""
            INSERT INTO km_cases (case_title, problem_text, resolution_text, outcome, lessons_learned, confidence)
            VALUES (%s, %s, %s, %s, %s, 0.8)
            RETURNING id
        """, (
            case.get('title', '')[:500],
            case.get('problem', ''),
            case.get('resolution', ''),
            case.get('outcome', ''),
            case.get('lessons', '')
        ))
        case_id = cur.fetchone()[0]
        try:
            case_text = f"{case.get('problem', '')} {case.get('resolution', '')}"
            emb = create_embedding(case_text)
            cur.execute("UPDATE km_cases SET embedding = %s WHERE id = %s", (str(emb), case_id))
        except Exception:
            pass
        stats['cases'] = stats.get('cases', 0) + 1
    
    # Сохраняем evidence — связь с документами
    for doc_id in doc_ids:
        for fact_type in ['facts', 'decisions']:
            for item in extracted.get(fact_type, []):
                obj_id = item.get('_id')
                if not obj_id:
                    continue
                cur.execute("""
                    INSERT INTO source_evidence (object_type, object_id, document_id, evidence_text)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (fact_type[:-1], obj_id, doc_id, item.get('text', '')[:500]))
    
    return stats


def get_unprocessed_docs(conn, batch_size=10, source_kind='telegram_message', min_length=0):
    """Получить необработанные документы — сгруппированные по каналу, хронологически."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # 1. Находим канал с наибольшим количеством необработанных
    cur.execute("""
        SELECT channel_name
        FROM source_documents
        WHERE source_kind = %s
          AND (meta->>'distilled') IS NULL
          AND (
              source_kind != 'email_message'
              OR meta->>'email_category' IN ('internal', 'external_business')
          )
        GROUP BY channel_name
        ORDER BY COUNT(*) DESC
        LIMIT 1
    """, (source_kind,))
    
    row = cur.fetchone()
    if not row:
        cur.close()
        return []
    
    channel = row['channel_name']
    
    # 2. Берём batch_size последовательных сообщений из этого канала
    cur.execute("""
        SELECT id, body_text, doc_date, author_name, channel_name, channel_ref
        FROM source_documents
        WHERE source_kind = %s
          AND channel_name = %s
          AND (meta->>'distilled') IS NULL
          AND (
              source_kind != 'email_message'
              OR meta->>'email_category' IN ('internal', 'external_business')
          )
        ORDER BY doc_date ASC
        LIMIT %s
    """, (source_kind, channel, batch_size))
    
    docs = cur.fetchall()
    cur.close()
    return docs


def mark_as_processed(cur, doc_ids):
    """Пометить документы как обработанные."""
    for doc_id in doc_ids:
        cur.execute("""
            UPDATE source_documents 
            SET meta = meta || '{"distilled": true}'::jsonb,
                updated_at = NOW()
            WHERE id = %s
        """, (doc_id,))


def run_distillation(source_kind='telegram_message', min_length=0, 
                     batch_size=10, max_batches=50):
    """Основной цикл distillation."""
    conn = psycopg2.connect(**CONFIG_PG)
    load_filter_rules(conn)
    
    total_stats = {'facts': 0, 'decisions': 0, 'relations': 0, 'tasks': 0, 'policies': 0, 'procedures': 0, 'cases': 0, 'errors': 0}
    
    for batch_num in range(max_batches):
        docs = get_unprocessed_docs(conn, batch_size=batch_size, 
                                     source_kind=source_kind, min_length=min_length)
        if not docs:
            print("Нет необработанных документов")
            break
        
        doc_ids = [d['id'] for d in docs]
        channels = set(d['channel_name'] for d in docs)
        
        print(f"\nBatch {batch_num+1}: {len(docs)} docs из {', '.join(channels)}")
        
        try:
            extracted = extract_knowledge(docs)
            
            cur = conn.cursor()
            stats = save_extraction(cur, extracted, doc_ids, conn)
            mark_as_processed(cur, doc_ids)
            conn.commit()
            cur.close()
            
            for k in stats:
                total_stats[k] += stats[k]
            
            print(f"facts={stats['facts']}, decisions={stats['decisions']}, "
                  f"relations={stats['relations']}, tasks={stats['tasks']}, "
                  f"policies={stats.get('policies',0)}, procedures={stats.get('procedures',0)}, "
                  f"cases={stats.get('cases',0)}")
            
            # Rate limiting
            time.sleep(1)
            
        except json.JSONDecodeError as e:
            print(f"  JSON parse error: {e}")
            total_stats['errors'] += 1
            # Всё равно помечаем как обработанные чтобы не зацикливаться
            cur = conn.cursor()
            mark_as_processed(cur, doc_ids)
            conn.commit()
            cur.close()
            
        except Exception as e:
            print(f"  Error: {e}")
            total_stats['errors'] += 1
            conn.rollback()
            time.sleep(3)
    
    conn.close()
    print(f"\n{'='*60}")
    print(f"ИТОГО: {total_stats}")
    return total_stats


if __name__ == '__main__':
    import sys
    import fcntl
    
    # Защита от одновременного запуска
    lock_file = open('/tmp/distillation.lock', 'w')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print("Distillation уже запущен, выходим")
        sys.exit(0)
    
    source = sys.argv[1] if len(sys.argv) > 1 else 'telegram_message'
    max_b = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    
    print(f"Distillation: source={source}, max_batches={max_b}")
    run_distillation(source_kind=source, max_batches=max_b)
