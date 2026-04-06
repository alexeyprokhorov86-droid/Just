"""
Автозаполнение БЖУ и аллергенов для номенклатуры Сырья.

Двухэтапный процесс:
1. ПОИСК: gpt-4o-mini-search-preview ищет КБЖУ в интернете
2. ВЕРИФИКАЦИЯ: gpt-4.1-mini проверяет данные на адекватность
3. Запись в 1С через OData PATCH + обновление PostgreSQL
"""
import json
import os
import time
import requests
import psycopg2
from urllib.parse import quote
from dotenv import load_dotenv
from datetime import datetime

load_dotenv('/home/admin/telegram_logger_bot/.env')

# === Конфигурация из .env ===
ODATA_BASE_URL = os.environ["ODATA_BASE_URL"].rstrip("/")
ODATA_USER = os.environ["ODATA_USERNAME"]
ODATA_PASS = os.environ["ODATA_PASSWORD"]

ROUTERAI_BASE_URL = os.environ["ROUTERAI_BASE_URL"]
ROUTERAI_API_KEY = os.environ["ROUTERAI_API_KEY"]

DB_HOST = os.environ.get("DB_HOST", "172.20.0.2")
DB_NAME = os.environ.get("DB_NAME", "knowledge_base")
DB_USER = os.environ.get("DB_USER", "knowledge")
DB_PASSWORD = os.environ["DB_PASSWORD"]

# Модели
SEARCH_MODEL = "openai/gpt-4o-mini-search-preview"
VERIFY_MODEL = "openai/gpt-4.1-mini"

# Маппинг Свойство_Key -> поле
NUTRITION_PROPS = {
    '89f344f6-8e2b-11f0-8e2c-000c299cc968': {'field': 'protein', 'name': 'Содержание белков, %'},
    '6415fc48-8e55-11f0-8e2c-000c299cc968': {'field': 'fat', 'name': 'Содержание жиров, %'},
    '72c01a93-8e2c-11f0-8e2c-000c299cc968': {'field': 'carbs', 'name': 'Содержание углеводов, %'},
    '9f465a41-8e2c-11f0-8e2c-000c299cc968': {'field': 'sugar', 'name': 'В том числе сахара, %'},
    'c4c3da14-8e2d-11f0-8e2c-000c299cc968': {'field': 'calories', 'name': 'Калорийность на 100г, кКал'},
    'ec65aa99-8e2c-11f0-8e2c-000c299cc968': {'field': 'moisture', 'name': 'Влажность, %'},
    '11fe0378-8e2d-11f0-8e2c-000c299cc968': {'field': 'fiber', 'name': 'Содержание клетчатки, %'},
    '3cdacac8-8e2d-11f0-8e2c-000c299cc968': {'field': 'lactose', 'name': 'Содержание лактозы, %'},
    '87d8874e-8e2d-11f0-8e2c-000c299cc968': {'field': 'sweetness', 'name': 'Относительная сладость, %'},
}

ALLERGEN_PROPS = {
    '87e46ae6-8e56-11f0-8e2c-000c299cc968': {'field': 'has_allergens', 'name': 'Наличие аллергенов'},
    'd68a8efc-8e56-11f0-8e2c-000c299cc968': {'field': 'глютен', 'name': 'Глютен'},
    'e15c2ff6-8e56-11f0-8e2c-000c299cc968': {'field': 'ракообразные', 'name': 'Ракообразные'},
    'ef1fea8c-8e56-11f0-8e2c-000c299cc968': {'field': 'яйца', 'name': 'Яйца'},
    'fe76964e-8e56-11f0-8e2c-000c299cc968': {'field': 'рыба', 'name': 'Рыба'},
    '0e9dc99e-8e57-11f0-8e2c-000c299cc968': {'field': 'арахис', 'name': 'Арахис'},
    '1aa12403-8e57-11f0-8e2c-000c299cc968': {'field': 'соя', 'name': 'Соя'},
    '286d6d77-8e57-11f0-8e2c-000c299cc968': {'field': 'молоко', 'name': 'Молоко'},
    '35ad29ff-8e57-11f0-8e2c-000c299cc968': {'field': 'орехи', 'name': 'Орехи'},
    '42c06b6c-8e57-11f0-8e2c-000c299cc968': {'field': 'сельдерей', 'name': 'Сельдерей'},
    '4aef2c22-8e57-11f0-8e2c-000c299cc968': {'field': 'горчица', 'name': 'Горчица'},
    '596e8fcb-8e57-11f0-8e2c-000c299cc968': {'field': 'кунжут', 'name': 'Кунжут'},
    '70229b66-8e57-11f0-8e2c-000c299cc968': {'field': 'диоксид_серы', 'name': 'Диоксид серы и сульфиты'},
    '7b4560cb-8e57-11f0-8e2c-000c299cc968': {'field': 'люпин', 'name': 'Люпин'},
    '86c00960-8e57-11f0-8e2c-000c299cc968': {'field': 'моллюски', 'name': 'Моллюски'},
}

# Виды номенклатуры Сырья
SYRYE_TYPE_IDS = [
    '59718fc5-64a8-11eb-8106-005056a759ff',
    'e6fc1a75-64a0-11eb-8106-005056a759ff',
    '773e4cfa-179f-11ec-bf1d-000c29247c35',
    '7389a6bb-17af-11ec-bf1d-000c29247c35',
    'd503d3f5-ce32-11ed-8e18-000c299cc968',
    '97e5cba0-17af-11ec-bf1d-000c29247c35',
    'b63360b4-17af-11ec-bf1d-000c29247c35',
]

JSON_FORMAT = """{
    "protein": число или null,
    "fat": число или null,
    "carbs": число или null,
    "sugar": число или null,
    "calories": число или null,
    "moisture": число или null,
    "fiber": число или null,
    "lactose": число или null,
    "sweetness": число или null,
    "has_allergens": true/false,
    "allergens": {
        "глютен": true/false,
        "ракообразные": true/false,
        "яйца": true/false,
        "рыба": true/false,
        "арахис": true/false,
        "соя": true/false,
        "молоко": true/false,
        "орехи": true/false,
        "сельдерей": true/false,
        "горчица": true/false,
        "кунжут": true/false,
        "диоксид_серы": true/false,
        "люпин": true/false,
        "моллюски": true/false
    },
    "sources": ["url1", "url2"]
}"""


def get_db():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)


def llm_call(model, messages, temperature=0.1):
    """Универсальный вызов LLM через RouterAI."""
    try:
        response = requests.post(
            f"{ROUTERAI_BASE_URL}/chat/completions",
            headers={
                "Authorization": f"Bearer {ROUTERAI_API_KEY}",
                "Content-Type": "application/json"
            },
            json={
                "model": model,
                "messages": messages,
                "max_tokens": 1500,
                "temperature": temperature
            },
            timeout=90
        )
        if response.status_code != 200:
            print(f"  LLM error ({model}): {response.status_code} {response.text[:200]}")
            return None
        return response.json()["choices"][0]["message"]["content"]
    except Exception as e:
        print(f"  LLM request error ({model}): {e}")
        return None


def parse_json_response(text):
    """Извлечь JSON из ответа LLM."""
    if not text:
        return None
    text = text.strip()
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        start = text.find('{')
        end = text.rfind('}')
        if start >= 0 and end > start:
            try:
                return json.loads(text[start:end+1])
            except json.JSONDecodeError:
                pass
        print(f"  JSON parse error. Raw: {text[:200]}")
        return None


def get_unfilled_nomenclature(conn):
    """Получить номенклатуру сырья без заполненных БЖУ."""
    cur = conn.cursor()
    placeholders = ','.join(['%s'] * len(SYRYE_TYPE_IDS))
    cur.execute(f"""
        SELECT id, name 
        FROM nomenclature 
        WHERE type_id IN ({placeholders})
          AND is_folder = false
          AND (protein IS NULL AND fat IS NULL AND carbs IS NULL AND calories IS NULL)
        ORDER BY name
    """, SYRYE_TYPE_IDS)
    result = cur.fetchall()
    cur.close()
    return result


def get_current_dop_rekvizity(nom_id):
    """Получить текущие доп. реквизиты из 1С."""
    encoded = quote("Catalog_Номенклатура", safe='_')
    dp = quote("ДополнительныеРеквизиты", safe='')
    url = f"{ODATA_BASE_URL}/{encoded}(guid'{nom_id}')/{dp}?$format=json"
    r = requests.get(url, auth=(ODATA_USER, ODATA_PASS), timeout=30)
    if r.status_code == 200:
        return r.json().get('value', [])
    return []


def search_nutrition(product_name):
    """ЭТАП 1: Поиск КБЖУ через gpt-4o-mini-search-preview."""
    clean_name = product_name
    for word in ['серии', 'серия', 'Серии', 'Серия', 'сер.', 'сер']:
        clean_name = clean_name.replace(word, '').strip()
    
    prompt = f"""Найди пищевую ценность (КБЖУ) и информацию об аллергенах для пищевого сырья: "{clean_name}"

Контекст: это сырьё для кондитерского производства (торты, пирожные, печенье).

Все числа — на 100 грамм продукта:
- protein, fat, carbs, sugar, fiber, lactose — в граммах на 100г
- calories — в кКал на 100г  
- moisture — влажность в %
- sweetness — относительная сладость (сахар=100, фруктоза=170, глюкоза=75), null если не применимо
- has_allergens — true если содержит любой из 14 аллергенов ЕС
- allergens — для каждого из 14 аллергенов ЕС: true/false

Верни ТОЛЬКО JSON, без пояснений:
{JSON_FORMAT}"""

    text = llm_call(SEARCH_MODEL, [
        {"role": "user", "content": prompt}
    ])
    return parse_json_response(text)


def verify_nutrition(product_name, search_data):
    """ЭТАП 2: Верификация найденных данных через gpt-4.1-mini."""
    prompt = f"""Проверь корректность пищевой ценности для кондитерского сырья "{product_name}".

Найденные данные:
{json.dumps(search_data, ensure_ascii=False, indent=2)}

Правила проверки:
1. Белки + жиры + углеводы + влажность + клетчатка + зола(~1-3%) ≈ 100% (допуск ±10%)
2. Сахар ≤ углеводы
3. Калорийность ≈ белки*4 + жиры*9 + углеводы*4 (допуск ±15%)
4. Диапазоны: белки/жиры/углеводы 0-100, калории 0-900, влажность 0-100
5. Аллергены логичны: мука→глютен, масло сливочное→молоко, яичный порошок→яйца
6. Влажность типична: мука~14%, масло~16%, сахар~0.1%, шоколад~1-2%, сухое молоко~4%
7. Если продукт — непонятная смесь или внутренний полуфабрикат — confidence="low"

Верни ТОЛЬКО JSON:
{{
    "verified": true/false,
    "confidence": "high"/"medium"/"low",
    "corrections": {{}},
    "issues": [],
    "final_data": {{полный набор данных с исправлениями в формате как на входе}}
}}"""

    text = llm_call(VERIFY_MODEL, [
        {"role": "system", "content": "Ты — эксперт по пищевой химии и технологии кондитерского производства. Проверяй строго. Отвечай только JSON."},
        {"role": "user", "content": prompt}
    ])
    return parse_json_response(text)


def write_to_1c(nom_id, nutrition_data, existing_props):
    """Записать БЖУ и аллергены в 1С через OData PATCH."""
    new_props = []
    line_number = 1
    
    all_known_keys = set(NUTRITION_PROPS.keys()) | set(ALLERGEN_PROPS.keys())
    for prop in existing_props:
        if prop.get('Свойство_Key') not in all_known_keys:
            new_props.append({
                'LineNumber': str(line_number),
                'Свойство_Key': prop['Свойство_Key'],
                'Значение': prop.get('Значение', ''),
                'Значение_Type': prop.get('Значение_Type', ''),
                'ТекстоваяСтрока': prop.get('ТекстоваяСтрока', '')
            })
            line_number += 1
    
    for prop_key, info in NUTRITION_PROPS.items():
        field = info['field']
        value = nutrition_data.get(field)
        if value is not None:
            new_props.append({
                'LineNumber': str(line_number),
                'Свойство_Key': prop_key,
                'Значение': float(value),
                'Значение_Type': 'Edm.Double',
                'ТекстоваяСтрока': ''
            })
            line_number += 1
    
    allergens = nutrition_data.get('allergens', {})
    has_allergens = nutrition_data.get('has_allergens', False)
    
    allergen_main_key = '87e46ae6-8e56-11f0-8e2c-000c299cc968'
    new_props.append({
        'LineNumber': str(line_number),
        'Свойство_Key': allergen_main_key,
        'Значение': 'true' if has_allergens else 'false',
        'Значение_Type': 'Edm.Boolean',
        'ТекстоваяСтрока': ''
    })
    line_number += 1
    
    for prop_key, info in ALLERGEN_PROPS.items():
        if prop_key == allergen_main_key:
            continue
        field = info['field']
        value = allergens.get(field, False)
        new_props.append({
            'LineNumber': str(line_number),
            'Свойство_Key': prop_key,
            'Значение': 'true' if value else 'false',
            'Значение_Type': 'Edm.Boolean',
            'ТекстоваяСтрока': ''
        })
        line_number += 1
    
    encoded = quote("Catalog_Номенклатура", safe='_')
    url = f"{ODATA_BASE_URL}/{encoded}(guid'{nom_id}')"
    payload = {'ДополнительныеРеквизиты': new_props}
    
    r = requests.patch(
        url, json=payload,
        headers={'Content-Type': 'application/json', 'Accept': 'application/json'},
        auth=(ODATA_USER, ODATA_PASS), timeout=30
    )
    return r.status_code == 200, r.status_code


def update_local_db(conn, nom_id, nutrition_data):
    """Обновить локальную БД PostgreSQL."""
    cur = conn.cursor()
    allergens_json = json.dumps(nutrition_data.get('allergens', {}), ensure_ascii=False)
    
    cur.execute("""
        UPDATE nomenclature SET
            protein = %s, fat = %s, carbs = %s, sugar = %s,
            calories = %s, moisture = %s, fiber = %s, lactose = %s,
            sweetness = %s, has_allergens = %s, allergens = %s::jsonb
        WHERE id = %s::uuid
    """, (
        nutrition_data.get('protein'), nutrition_data.get('fat'),
        nutrition_data.get('carbs'), nutrition_data.get('sugar'),
        nutrition_data.get('calories'), nutrition_data.get('moisture'),
        nutrition_data.get('fiber'), nutrition_data.get('lactose'),
        nutrition_data.get('sweetness'), nutrition_data.get('has_allergens', False),
        allergens_json, str(nom_id)
    ))
    conn.commit()
    cur.close()


def main(dry_run=False, limit=None):
    """Основной запуск."""
    print(f"\n{'='*60}")
    print(f"АВТОЗАПОЛНЕНИЕ БЖУ СЫРЬЯ — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"Поиск: {SEARCH_MODEL}")
    print(f"Верификация: {VERIFY_MODEL}")
    if dry_run:
        print("РЕЖИМ: DRY RUN (без записи в 1С)")
    print(f"{'='*60}\n")
    
    conn = get_db()
    unfilled = get_unfilled_nomenclature(conn)
    print(f"Незаполненных позиций: {len(unfilled)}")
    
    if limit:
        unfilled = unfilled[:limit]
        print(f"Ограничение: обработаем {limit}")
    
    stats = {'success': 0, 'skipped': 0, 'errors': 0, 'low_confidence': 0}
    
    for i, (nom_id, name) in enumerate(unfilled):
        print(f"\n[{i+1}/{len(unfilled)}] {name}")
        
        # --- ЭТАП 1: ПОИСК ---
        search_result = search_nutrition(name)
        if not search_result:
            print("  ✗ Поиск не вернул данных")
            stats['errors'] += 1
            time.sleep(1)
            continue
        
        print(f"  Поиск: Б={search_result.get('protein')} Ж={search_result.get('fat')} "
              f"У={search_result.get('carbs')} Кал={search_result.get('calories')}")
        
        # --- ЭТАП 2: ВЕРИФИКАЦИЯ ---
        verify_result = verify_nutrition(name, search_result)
        if not verify_result:
            print("  ✗ Верификация не удалась")
            stats['errors'] += 1
            time.sleep(1)
            continue
        
        confidence = verify_result.get('confidence', 'low')
        verified = verify_result.get('verified', False)
        issues = verify_result.get('issues', [])
        
        if issues:
            print(f"  Замечания: {'; '.join(issues[:3])}")
        
        final_data = verify_result.get('final_data', search_result)
        
        print(f"  Верификация: {'✓' if verified else '✗'} | confidence={confidence}")
        print(f"  Итог: Б={final_data.get('protein')} Ж={final_data.get('fat')} "
              f"У={final_data.get('carbs')} Кал={final_data.get('calories')} "
              f"Аллергены={'Да' if final_data.get('has_allergens') else 'Нет'}")
        
        if confidence == 'low' or not verified:
            print(f"  ⚠ Пропуск: confidence={confidence}, verified={verified}")
            stats['low_confidence'] += 1
            # Записываем в очередь для технолога с найденными данными
        try:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO nutrition_requests (nom_id, nom_name, status, search_data, assigned_to, assigned_name)
                VALUES (%s, %s, 'pending', %s, 
                    (SELECT user_id FROM tg_user_roles WHERE role ILIKE '%%главн%%технолог%%' LIMIT 1),
                    (SELECT first_name FROM tg_user_roles WHERE role ILIKE '%%главн%%технолог%%' LIMIT 1)
                )
                ON CONFLICT (nom_id) DO UPDATE SET search_data = EXCLUDED.search_data, updated_at = NOW()
            """, (str(nom_id), name, json.dumps(final_data, ensure_ascii=False)))
            conn.commit()
            cur.close()
        except Exception as e:
            print(f"  DB error: {e}")
            time.sleep(0.5)
            continue
        
        if dry_run:
            print("  🔸 DRY RUN — запись пропущена")
            stats['success'] += 1
            time.sleep(0.5)
            continue
        
        # --- ЭТАП 3: ЗАПИСЬ ---
        existing = get_current_dop_rekvizity(nom_id)
        ok, status = write_to_1c(nom_id, final_data, existing)
        
        if ok:
            update_local_db(conn, nom_id, final_data)
            print(f"  ✓ Записано в 1С и БД")
            stats['success'] += 1
        else:
            print(f"  ✗ Ошибка записи в 1С: status={status}")
            stats['errors'] += 1
        
        time.sleep(1)
    
    conn.close()
    
    print(f"\n{'='*60}")
    print(f"ИТОГО:")
    print(f"  Успешно: {stats['success']}")
    print(f"  Пропущено (low confidence): {stats['low_confidence']}")
    print(f"  Ошибки: {stats['errors']}")
    print(f"{'='*60}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='Автозаполнение БЖУ сырья')
    parser.add_argument('--dry-run', action='store_true', help='Без записи в 1С')
    parser.add_argument('--limit', type=int, default=None, help='Ограничить количество позиций')
    args = parser.parse_args()
    
    main(dry_run=args.dry_run, limit=args.limit)
