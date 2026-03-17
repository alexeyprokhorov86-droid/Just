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

load_dotenv('/home/admin/telegram_logger_bot/.env')

CONFIG_PG = {
    'host': '172.17.0.2',
    'port': 5432,
    'dbname': 'knowledge_base',
    'user': 'knowledge',
    'password': os.getenv('PG_PASSWORD', 'Prokhorov2025Secure')
}

ROUTER_AI_URL = os.getenv('ROUTERAI_BASE_URL', 'https://routerai.ru/api/v1')
ROUTER_AI_KEY = os.getenv('ROUTERAI_API_KEY', '')

EXTRACTION_SYSTEM_PROMPT = """Ты — система извлечения корпоративных знаний для кондитерской компании Фрумелад.
Из каждого сообщения извлеки ВСЕ что найдёшь из следующих категорий.

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
  ]
}

Типы фактов (fact_type): supplies, responsible_for, quality_issue, pricing, delivery_terms, procedure_step, policy, complaint, stock_info, production_info, hr_info, financial_info

Типы связей (relation): manages, reports_to, responsible_for, supplies, complains_about, approves, replaces, collaborates_with

Правила:
- Извлекай только конкретные факты, не общие фразы
- Если сообщение — просто приветствие или бессодержательное, верни пустые массивы
- Имена сотрудников пиши полностью как в тексте
- Названия компаний пиши как в тексте
- confidence от 0.5 до 1.0
- importance от 0.3 до 1.0
"""


def call_llm(messages, temperature=0.1):
    """Вызов LLM через RouterAI."""
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {ROUTER_AI_KEY}'
    }
    payload = {
        'model': 'gpt-4.1-mini',
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
    """Найти сущность в km_entities по имени."""
    if not name or len(name.strip()) < 2:
        return None
    
    name = name.strip()
    
    # Точное совпадение
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
    
    # Поиск по ILIKE
    if entity_type:
        cur.execute(
            "SELECT id FROM km_entities WHERE canonical_name ILIKE %s AND entity_type = %s LIMIT 1",
            (f'%{name}%', entity_type)
        )
    else:
        cur.execute(
            "SELECT id FROM km_entities WHERE canonical_name ILIKE %s LIMIT 1",
            (f'%{name}%',)
        )
    row = cur.fetchone()
    if row:
        return row[0]
    
    return None


def save_extraction(cur, extracted, doc_ids):
    """Сохранить извлечённые знания в km_* таблицы."""
    stats = {'facts': 0, 'decisions': 0, 'relations': 0, 'tasks': 0}
    
    # Факты
    for fact in extracted.get('facts', []):
        subject_id = resolve_entity(cur, fact.get('subject'))
        object_id = resolve_entity(cur, fact.get('object'))
        
        cur.execute("""
            INSERT INTO km_facts (fact_type, subject_entity_id, object_entity_id,
                fact_text, confidence, verification_status)
            VALUES (%s, %s, %s, %s, %s, 'extracted')
        """, (
            fact.get('fact_type', 'general'),
            subject_id,
            object_id,
            fact.get('text', ''),
            fact.get('confidence', 0.8)
        ))
        stats['facts'] += 1
    
    # Решения
    for dec in extracted.get('decisions', []):
        decided_by_id = resolve_entity(cur, dec.get('decided_by'), 'employee')
        scope_entity_id = None
        
        cur.execute("""
            INSERT INTO km_decisions (decision_text, scope_type, scope_entity_id,
                decided_by_entity_id, importance, confidence)
            VALUES (%s, %s, %s, %s, %s, 0.8)
        """, (
            dec.get('text', ''),
            dec.get('scope', 'company'),
            scope_entity_id,
            decided_by_id,
            dec.get('importance', 0.5)
        ))
        stats['decisions'] += 1
    
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
    
    # Сохраняем evidence — связь с документами
    for doc_id in doc_ids:
        for fact_type in ['facts', 'decisions']:
            for item in extracted.get(fact_type, []):
                cur.execute("""
                    INSERT INTO source_evidence (object_type, object_id, document_id, evidence_text)
                    VALUES (%s, currval('km_facts_id_seq'), %s, %s)
                    ON CONFLICT DO NOTHING
                """, (fact_type[:-1], doc_id, item.get('text', '')[:500]))
    
    return stats


def get_unprocessed_docs(conn, batch_size=10, source_kind='telegram_message', min_length=100):
    """Получить необработанные документы."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # Документы которые ещё не обработаны (нет в meta.distilled)
    cur.execute("""
        SELECT id, body_text, doc_date, author_name, channel_name, channel_ref
        FROM source_documents
        WHERE source_kind = %s
        AND LENGTH(body_text) >= %s
        AND (meta->>'distilled') IS NULL
        ORDER BY doc_date DESC
        LIMIT %s
    """, (source_kind, min_length, batch_size))
    
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


def run_distillation(source_kind='telegram_message', min_length=100, 
                     batch_size=5, max_batches=50):
    """Основной цикл distillation."""
    conn = psycopg2.connect(**CONFIG_PG)
    
    total_stats = {'facts': 0, 'decisions': 0, 'relations': 0, 'tasks': 0, 'errors': 0}
    
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
            stats = save_extraction(cur, extracted, doc_ids)
            mark_as_processed(cur, doc_ids)
            conn.commit()
            cur.close()
            
            for k in stats:
                total_stats[k] += stats[k]
            
            print(f"  facts={stats['facts']}, decisions={stats['decisions']}, "
                  f"relations={stats['relations']}, tasks={stats['tasks']}")
            
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
    
    source = sys.argv[1] if len(sys.argv) > 1 else 'telegram_message'
    max_b = int(sys.argv[2]) if len(sys.argv) > 2 else 20
    
    print(f"Distillation: source={source}, max_batches={max_b}")
    run_distillation(source_kind=source, max_batches=max_b)
