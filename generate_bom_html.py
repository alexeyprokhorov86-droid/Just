#!/usr/bin/env python3
"""
Генератор HTML-отчёта по составу продукции (BOM) v2.
Фичи: поиск по артикулу, состав по ТР ТС, аллергены, КБЖУ, потери.
Запуск: python3 generate_bom_html.py
Результат: /var/www/bom/index.html
"""
import psycopg2
import psycopg2.extras
import json
import os
from decimal import Decimal
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'dbname': os.getenv('DB_NAME', 'knowledge_base'),
    'user': os.getenv('DB_USER', 'knowledge'),
    'password': os.getenv('DB_PASSWORD'),
}

OUTPUT_DIR = '/var/www/bom'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'index.html')

# 14 аллергенов по ТР ТС 029/2012
ALLERGEN_NAMES = {
    'глютен': 'Глютен (злаки)',
    'ракообразные': 'Ракообразные',
    'яйца': 'Яйца',
    'рыба': 'Рыба',
    'арахис': 'Арахис',
    'соя': 'Соя',
    'молоко': 'Молоко (лактоза)',
    'орехи': 'Орехи',
    'сельдерей': 'Сельдерей',
    'горчица': 'Горчица',
    'кунжут': 'Кунжут',
    'диоксид_серы': 'Диоксид серы и сульфиты',
    'люпин': 'Люпин',
    'моллюски': 'Моллюски',
}


def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def fetch_data():
    """Загрузка данных BOM, цен, КБЖУ, аллергенов, спецификаций из БД"""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Последний расчёт
    cur.execute("""
        SELECT id, started_at, products_processed, materials_total, errors_total
        FROM bom_calculations WHERE status = 'completed'
        ORDER BY id DESC LIMIT 1
    """)
    calc = cur.fetchone()
    if not calc:
        print("Нет завершённых расчётов BOM")
        return None
    calc_id = calc['id']

    # BOM данные
    cur.execute("""
        SELECT
            be.product_key,
            be.product_name,
            be.material_key,
            be.material_name,
            be.material_unit,
            be.quantity_per_unit,
            be.type_level_1,
            be.type_name
        FROM bom_expanded be
        WHERE be.calculation_id = %s
        ORDER BY be.product_name, be.type_level_1 NULLS LAST, be.type_name, be.material_name
    """, (calc_id,))
    rows = cur.fetchall()

    # Последние цены закупок
    cur.execute("""
        SELECT DISTINCT ON (pi.nomenclature_key)
            pi.nomenclature_key,
            pi.price,
            pu.doc_date
        FROM c1_purchase_items pi
        JOIN c1_purchases pu ON pu.ref_key = pi.doc_key
        WHERE pu.posted = true AND pi.price > 0
        ORDER BY pi.nomenclature_key, pu.doc_date DESC
    """)
    prices = {}
    for p in cur.fetchall():
        prices[p['nomenclature_key']] = float(p['price'])

    # КБЖУ + аллергены + moisture для всех материалов в BOM
    cur.execute("""
        SELECT
            n.id::text as nom_key,
            n.protein, n.fat, n.carbs, n.sugar,
            n.calories, n.moisture,
            n.has_allergens,
            n.allergens
        FROM nomenclature n
        WHERE n.id IN (
            SELECT DISTINCT material_key::uuid
            FROM bom_expanded WHERE calculation_id = %s
        )
    """, (calc_id,))
    nutrition = {}
    for row in cur.fetchall():
        nutrition[row['nom_key']] = {
            'protein': float(row['protein']) if row['protein'] is not None else None,
            'fat': float(row['fat']) if row['fat'] is not None else None,
            'carbs': float(row['carbs']) if row['carbs'] is not None else None,
            'sugar': float(row['sugar']) if row['sugar'] is not None else None,
            'calories': float(row['calories']) if row['calories'] is not None else None,
            'moisture': float(row['moisture']) if row['moisture'] is not None else None,
            'has_allergens': row['has_allergens'] or False,
            'allergens': row['allergens'] if row['allergens'] else {},
        }

    # Артикулы и коды продуктов (ГП)
    cur.execute("""
        SELECT DISTINCT ON (n.id)
            n.id::text as nom_key,
            n.article,
            n.code
        FROM nomenclature n
        WHERE n.id IN (
            SELECT DISTINCT product_key::uuid
            FROM bom_expanded WHERE calculation_id = %s
        )
    """, (calc_id,))
    product_articles = {}
    for row in cur.fetchall():
        product_articles[row['nom_key']] = {
            'article': row['article'] or '',
            'code': row['code'] or '',
        }

    # Спецификации (для потерь): product_quantity и сумма материалов
    cur.execute("""
        SELECT
            cs.product_key,
            cs.product_quantity,
            (SELECT SUM(sm.quantity) FROM c1_spec_materials sm WHERE sm.spec_key = cs.ref_key) as materials_sum
        FROM c1_specifications cs
        WHERE cs.status = 'Действует'
          AND cs.product_key IN (
              SELECT DISTINCT product_key FROM bom_expanded WHERE calculation_id = %s
          )
    """, (calc_id,))
    specs = {}
    for row in cur.fetchall():
        specs[row['product_key']] = {
            'product_quantity': float(row['product_quantity']) if row['product_quantity'] else 0,
            'materials_sum': float(row['materials_sum']) if row['materials_sum'] else 0,
        }

    # Ошибки
    cur.execute("""
        SELECT product_name, semifinished_name, error_type, details
        FROM bom_errors WHERE calculation_id = %s
    """, (calc_id,))
    errors = cur.fetchall()

    cur.close()
    conn.close()

    return {
        'rows': rows,
        'prices': prices,
        'nutrition': nutrition,
        'product_articles': product_articles,
        'specs': specs,
        'calc': calc,
        'errors': errors,
    }


def qty_to_kg(qty, unit):
    """Пересчёт количества в кг"""
    u = (unit or '').lower().strip()
    if u in ('кг', 'kg'):
        return qty
    elif u in ('г', 'гр', 'g', 'gr'):
        return qty / 1000
    elif u in ('л', 'l', 'литр'):
        return qty  # 1л = 1кг
    elif u in ('мл', 'ml'):
        return qty / 1000
    return 0  # шт и прочее


def build_products_json(data):
    """Построение JSON-структуры для фронтенда"""
    rows = data['rows']
    prices = data['prices']
    nutrition = data['nutrition']
    product_articles = data['product_articles']
    specs = data['specs']

    products = {}

    for row in rows:
        pk = row['product_key']
        if pk not in products:
            art = product_articles.get(pk, {})
            products[pk] = {
                'name': row['product_name'],
                'key': pk,
                'article': art.get('article', ''),
                'code': art.get('code', ''),
                'groups': {},
                'materials_list': [],  # для состава ТР ТС и КБЖУ
            }

        type_name = row['type_name'] or 'Прочее'
        type_level_1 = row['type_level_1'] or 'Прочее'

        if type_name not in products[pk]['groups']:
            products[pk]['groups'][type_name] = {
                'name': type_name,
                'level1': type_level_1,
                'materials': [],
                'total_kg': 0,
                'total_cost': 0,
            }

        price = prices.get(row['material_key'], 0)
        qty = float(row['quantity_per_unit'])
        cost = qty * price
        unit = row['material_unit'] or 'шт'
        qty_kg = qty_to_kg(qty, unit)
        mat_key = row['material_key']

        mat_nutr = nutrition.get(mat_key, {})

        mat_entry = {
            'name': row['material_name'],
            'key': mat_key,
            'unit': unit,
            'qty': round(qty, 4),
            'price': round(price, 2),
            'cost': round(cost, 2),
            'qty_kg': round(qty_kg, 6),
            'level1': type_level_1,
        }

        products[pk]['groups'][type_name]['materials'].append(mat_entry)
        products[pk]['groups'][type_name]['total_kg'] += qty_kg
        products[pk]['groups'][type_name]['total_cost'] += cost

        # Для ТР ТС / КБЖУ / аллергенов — собираем все материалы с КБЖУ
        products[pk]['materials_list'].append({
            'name': row['material_name'],
            'key': mat_key,
            'qty_kg': qty_kg,
            'unit': unit,
            'level1': type_level_1,
            'protein': mat_nutr.get('protein'),
            'fat': mat_nutr.get('fat'),
            'carbs': mat_nutr.get('carbs'),
            'sugar': mat_nutr.get('sugar'),
            'calories': mat_nutr.get('calories'),
            'moisture': mat_nutr.get('moisture'),
            'has_allergens': mat_nutr.get('has_allergens', False),
            'allergens': mat_nutr.get('allergens', {}),
        })

    # Формируем итоговый список
    product_list = []
    for pk, prod in sorted(products.items(), key=lambda x: x[1]['name']):
        groups = []
        total_kg = 0
        total_cost = 0
        raw_kg = 0  # только сырьё (Себестоимость)
        raw_cost = 0

        for gname, g in sorted(prod['groups'].items()):
            g['total_kg'] = round(g['total_kg'], 4)
            g['total_cost'] = round(g['total_cost'], 2)
            total_kg += g['total_kg']
            total_cost += g['total_cost']
            if g['level1'] == 'Себестоимость':
                raw_kg += g['total_kg']
                raw_cost += g['total_cost']
            groups.append(g)

        # Состав по ТР ТС: ингредиенты из Себестоимость, по убыванию массовой доли
        raw_materials = [m for m in prod['materials_list'] if m['level1'] == 'Себестоимость' and m['qty_kg'] > 0]
        total_raw_kg = sum(m['qty_kg'] for m in raw_materials)

        # Ингредиенты по убыванию доли
        composition = []
        if total_raw_kg > 0:
            for m in sorted(raw_materials, key=lambda x: x['qty_kg'], reverse=True):
                pct = (m['qty_kg'] / total_raw_kg) * 100
                composition.append({
                    'name': m['name'],
                    'pct': round(pct, 1),
                })

        # Аллергены: union всех аллергенов от материалов
        allergens_set = set()
        for m in prod['materials_list']:
            if m.get('has_allergens') and m.get('allergens'):
                for aname, aval in m['allergens'].items():
                    if aval:
                        allergens_set.add(aname)

        # КБЖУ: сумма по всем сырьевым материалам
        # Формула: на 100г готового = sum(nutrient_per_100g * qty_kg) / total_raw_kg * 100
        # Потом пересчёт с учётом влажности
        kbzhu_raw = {'protein': 0, 'fat': 0, 'carbs': 0, 'sugar': 0, 'calories': 0}
        kbzhu_coverage = 0
        for m in raw_materials:
            if m['protein'] is not None:
                # nutrient на 100г сырья * кол-во_кг = nutrient в граммах
                kbzhu_raw['protein'] += (m['protein'] / 100) * m['qty_kg'] * 1000
                kbzhu_raw['fat'] += (m['fat'] or 0) / 100 * m['qty_kg'] * 1000
                kbzhu_raw['carbs'] += (m['carbs'] or 0) / 100 * m['qty_kg'] * 1000
                kbzhu_raw['sugar'] += (m['sugar'] or 0) / 100 * m['qty_kg'] * 1000
                kbzhu_raw['calories'] += (m['calories'] or 0) / 100 * m['qty_kg'] * 1000
                kbzhu_coverage += 1

        # Потери: из спецификации
        spec = specs.get(pk, {})
        spec_output = spec.get('product_quantity', 0)
        spec_input = spec.get('materials_sum', 0)

        # Потери по сухим веществам: нужна влажность ингредиентов
        # Сухие вещества на входе = sum(qty_kg * (1 - moisture/100))
        dry_input = 0
        total_moisture_input = 0
        for m in raw_materials:
            moist = m.get('moisture')
            if moist is not None and m['qty_kg'] > 0:
                dry_input += m['qty_kg'] * (1 - moist / 100)
                total_moisture_input += m['qty_kg'] * (moist / 100)
            else:
                # Без данных о влажности считаем сухим (moisture=0)
                dry_input += m['qty_kg']

        product_list.append({
            'name': prod['name'],
            'key': prod['key'],
            'article': prod.get('article', ''),
            'code': prod.get('code', ''),
            'groups': groups,
            'total_kg': round(total_kg, 4),
            'total_cost': round(total_cost, 2),
            'raw_kg': round(raw_kg, 4),
            'raw_cost': round(raw_cost, 2),
            'composition': composition,
            'allergens': sorted(list(allergens_set)),
            'kbzhu_raw': {k: round(v, 2) for k, v in kbzhu_raw.items()},
            'kbzhu_coverage': kbzhu_coverage,
            'kbzhu_total_materials': len(raw_materials),
            'total_raw_kg': round(total_raw_kg, 4),
            'spec_output': round(spec_output, 4),
            'spec_input': round(spec_input, 4),
            'dry_input_kg': round(dry_input, 6),
            'moisture_input_kg': round(total_moisture_input, 6),
        })

    return product_list


def generate_html(product_list, meta):
    """Генерация HTML файла"""
    calc = meta['calc']
    calc_date = calc['started_at'].strftime('%d.%m.%Y %H:%M') if calc['started_at'] else ''
    products_json = json.dumps(product_list, ensure_ascii=False, default=decimal_default)

    # Маппинг названий аллергенов
    allergen_names_json = json.dumps(ALLERGEN_NAMES, ensure_ascii=False)

    html = f'''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Состав продукции — Фрумелад</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f0f2f5; color: #1a1a2e; line-height: 1.5; }}

.header {{
    background: #1a1a2e;
    color: #e0e0e0;
    padding: 24px 32px;
    border-bottom: 3px solid #e94560;
}}
.header h1 {{ font-size: 20px; font-weight: 700; letter-spacing: 0.5px; color: #fff; }}
.header .meta {{ font-size: 12px; opacity: 0.6; margin-top: 4px; font-family: monospace; }}

.container {{ max-width: 1140px; margin: 0 auto; padding: 20px; }}

/* --- Search --- */
.search-box {{
    position: sticky; top: 0; z-index: 100;
    background: #fff; border-radius: 8px;
    box-shadow: 0 1px 8px rgba(0,0,0,0.08);
    padding: 16px 20px; margin-bottom: 20px;
    border-left: 4px solid #e94560;
}}
.search-box label {{ font-size: 13px; font-weight: 600; color: #666; display: block; margin-bottom: 6px; }}
.search-input {{
    width: 100%; padding: 10px 14px; font-size: 15px;
    border: 1px solid #d0d0d0; border-radius: 6px;
    background: #fafafa;
}}
.search-input:focus {{ border-color: #e94560; outline: none; background: #fff; }}
.dropdown {{
    max-height: 320px; overflow-y: auto;
    border: 1px solid #d0d0d0; border-radius: 6px;
    background: white; display: none; margin-top: 4px;
}}
.dropdown.open {{ display: block; }}
.dropdown-item {{
    padding: 9px 14px; cursor: pointer; font-size: 13px;
    border-bottom: 1px solid #f5f5f5;
    display: flex; justify-content: space-between; align-items: center;
}}
.dropdown-item:hover {{ background: #fff0f3; }}
.dropdown-item.selected {{ background: #ffe0e6; font-weight: 600; }}
.dropdown-item .art {{ color: #999; font-size: 11px; font-family: monospace; }}

/* --- Product header --- */
.product-header {{
    background: #1a1a2e; color: #fff;
    padding: 16px 20px; border-radius: 8px 8px 0 0;
    margin-top: 12px; display: flex; justify-content: space-between; align-items: center;
}}
.product-header h2 {{ font-size: 16px; font-weight: 700; }}
.product-header .art-code {{ font-size: 12px; color: #aaa; font-family: monospace; }}

/* --- Tabs --- */
.tabs {{
    display: flex; background: #2a2a4a; border-radius: 0;
    overflow-x: auto;
}}
.tab {{
    padding: 10px 18px; font-size: 13px; font-weight: 500;
    color: #aaa; cursor: pointer; white-space: nowrap;
    border-bottom: 2px solid transparent;
    transition: all 0.2s;
}}
.tab:hover {{ color: #fff; }}
.tab.active {{ color: #e94560; border-bottom-color: #e94560; }}

.tab-content {{ display: none; background: #fff; padding: 0; border-radius: 0 0 8px 8px; box-shadow: 0 1px 6px rgba(0,0,0,0.06); }}
.tab-content.active {{ display: block; }}

/* --- BOM Table --- */
.bom-table {{
    width: 100%; border-collapse: collapse;
}}
.bom-table th {{
    background: #f7f7fa; padding: 10px 14px;
    text-align: left; font-size: 12px;
    font-weight: 600; color: #888;
    border-bottom: 2px solid #eee;
    text-transform: uppercase; letter-spacing: 0.5px;
}}
.bom-table th.num {{ text-align: right; }}
.bom-table td {{ padding: 7px 14px; font-size: 13px; border-bottom: 1px solid #f0f0f0; }}
.bom-table td.num {{ text-align: right; font-variant-numeric: tabular-nums; font-family: monospace; font-size: 12px; }}

.group-header td {{
    background: #f0f2f5; font-weight: 700;
    font-size: 12px; color: #1a1a2e;
    padding: 10px 14px; border-bottom: 2px solid #ddd;
    text-transform: uppercase; letter-spacing: 0.3px;
}}
.group-subtotal td {{
    background: #fafafa; font-weight: 600;
    font-size: 12px; color: #555;
    border-top: 1px solid #e0e0e0;
}}
.grand-total td {{
    background: #1a1a2e; color: #fff;
    font-weight: 700; font-size: 13px;
    padding: 12px 14px;
}}
.raw-total td {{
    background: #e94560; color: #fff;
    font-weight: 700; font-size: 13px;
    padding: 12px 14px;
}}

.no-price {{ color: #e94560; }}

/* --- Info sections --- */
.info-section {{
    padding: 20px;
}}
.info-section h3 {{
    font-size: 14px; font-weight: 700; color: #1a1a2e;
    margin-bottom: 12px; padding-bottom: 6px;
    border-bottom: 2px solid #e94560;
}}
.composition-text {{
    font-size: 13px; line-height: 1.7; color: #333;
}}
.composition-text .minor {{ color: #888; }}

.allergen-list {{
    display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px;
}}
.allergen-tag {{
    background: #fff0f3; color: #e94560; border: 1px solid #e94560;
    padding: 4px 12px; border-radius: 20px; font-size: 12px; font-weight: 600;
}}
.no-allergens {{ color: #27ae60; font-weight: 600; font-size: 13px; }}

/* --- KBZHU --- */
.kbzhu-grid {{
    display: grid; grid-template-columns: repeat(auto-fit, minmax(130px, 1fr)); gap: 12px; margin-top: 12px;
}}
.kbzhu-card {{
    background: #f7f7fa; border-radius: 8px; padding: 14px; text-align: center;
    border: 1px solid #eee;
}}
.kbzhu-card .value {{ font-size: 22px; font-weight: 700; color: #1a1a2e; }}
.kbzhu-card .label {{ font-size: 11px; color: #888; margin-top: 2px; text-transform: uppercase; letter-spacing: 0.5px; }}
.kbzhu-note {{ font-size: 11px; color: #999; margin-top: 10px; }}

/* --- Moisture input --- */
.moisture-row {{
    display: flex; align-items: center; gap: 12px; margin: 12px 0;
    padding: 12px; background: #f7f7fa; border-radius: 6px;
}}
.moisture-row label {{ font-size: 13px; font-weight: 600; white-space: nowrap; }}
.moisture-input {{
    width: 80px; padding: 6px 10px; font-size: 14px;
    border: 1px solid #d0d0d0; border-radius: 4px; text-align: center;
}}
.moisture-input:focus {{ border-color: #e94560; outline: none; }}

/* --- Losses --- */
.losses-grid {{
    display: grid; grid-template-columns: 1fr 1fr; gap: 16px; margin-top: 12px;
}}
.loss-card {{
    background: #f7f7fa; border-radius: 8px; padding: 16px;
    border: 1px solid #eee;
}}
.loss-card h4 {{ font-size: 13px; font-weight: 700; margin-bottom: 10px; color: #1a1a2e; }}
.loss-row {{
    display: flex; justify-content: space-between; font-size: 13px;
    padding: 4px 0; border-bottom: 1px solid #f0f0f0;
}}
.loss-row .lbl {{ color: #666; }}
.loss-row .val {{ font-weight: 600; font-family: monospace; }}
.loss-row .val.negative {{ color: #e94560; }}

.empty {{ text-align: center; padding: 60px; color: #999; font-size: 15px; }}

@media (max-width: 768px) {{
    .losses-grid {{ grid-template-columns: 1fr; }}
    .kbzhu-grid {{ grid-template-columns: repeat(2, 1fr); }}
    .product-header {{ flex-direction: column; gap: 4px; }}
}}
</style>
</head>
<body>

<div class="header">
    <h1>СОСТАВ ПРОДУКЦИИ НА 1 ЕДИНИЦУ</h1>
    <div class="meta">Расчёт {calc_date} &middot; {calc['products_processed']} продуктов &middot; {calc['materials_total']} материалов</div>
</div>

<div class="container">
    <div class="search-box">
        <label>Поиск по названию или артикулу:</label>
        <input type="text" class="search-input" id="searchInput" placeholder="Введите название, артикул или код..." autocomplete="off">
        <div class="dropdown" id="dropdown"></div>
    </div>
    <div id="result" class="empty">Выберите продукт из списка</div>
</div>

<script>
const PRODUCTS = {products_json};
const ALLERGEN_NAMES = {allergen_names_json};

const searchInput = document.getElementById('searchInput');
const dropdown = document.getElementById('dropdown');
const result = document.getElementById('result');

let selectedIndex = -1;
let filteredProducts = [];
let currentProduct = null;
let currentMoisture = 17; // дефолт

function renderDropdown(filter) {{
    if (!filter || filter.length < 1) {{ dropdown.classList.remove('open'); return; }}
    const lower = filter.toLowerCase();
    filteredProducts = PRODUCTS.filter(p =>
        p.name.toLowerCase().includes(lower) ||
        (p.article && p.article.toLowerCase().includes(lower)) ||
        (p.code && p.code.toLowerCase().includes(lower))
    );
    if (filteredProducts.length === 0) {{
        dropdown.innerHTML = '<div class="dropdown-item" style="color:#999">Ничего не найдено</div>';
        dropdown.classList.add('open');
        return;
    }}
    dropdown.innerHTML = filteredProducts.map((p, i) =>
        `<div class="dropdown-item" data-idx="${{i}}" onclick="selectProduct(${{i}})">
            <span>${{p.name}}</span>
            <span class="art">${{p.article ? 'арт. ' + p.article : ''}}</span>
        </div>`
    ).join('');
    dropdown.classList.add('open');
    selectedIndex = -1;
}}

searchInput.addEventListener('input', (e) => renderDropdown(e.target.value));
searchInput.addEventListener('focus', (e) => {{ if (e.target.value) renderDropdown(e.target.value); }});
document.addEventListener('click', (e) => {{ if (!e.target.closest('.search-box')) dropdown.classList.remove('open'); }});

searchInput.addEventListener('keydown', (e) => {{
    const items = dropdown.querySelectorAll('.dropdown-item[data-idx]');
    if (e.key === 'ArrowDown') {{
        e.preventDefault();
        selectedIndex = Math.min(selectedIndex + 1, items.length - 1);
        items.forEach((el, i) => el.classList.toggle('selected', i === selectedIndex));
        items[selectedIndex]?.scrollIntoView({{ block: 'nearest' }});
    }} else if (e.key === 'ArrowUp') {{
        e.preventDefault();
        selectedIndex = Math.max(selectedIndex - 1, 0);
        items.forEach((el, i) => el.classList.toggle('selected', i === selectedIndex));
        items[selectedIndex]?.scrollIntoView({{ block: 'nearest' }});
    }} else if (e.key === 'Enter' && selectedIndex >= 0) {{
        e.preventDefault();
        selectProduct(selectedIndex);
    }}
}});

function selectProduct(idx) {{
    currentProduct = filteredProducts[idx];
    searchInput.value = currentProduct.name;
    dropdown.classList.remove('open');
    currentMoisture = 17;
    renderProduct(currentProduct);
}}

function fmt(n, d) {{
    if (n === null || n === undefined || isNaN(n)) return '—';
    return n.toFixed(d).replace(/\\.?0+$/, '') || '0';
}}

function switchTab(tabId) {{
    document.querySelectorAll('.tab').forEach(t => t.classList.toggle('active', t.dataset.tab === tabId));
    document.querySelectorAll('.tab-content').forEach(c => c.classList.toggle('active', c.id === tabId));
}}

function onMoistureChange(val) {{
    const v = parseFloat(val);
    if (!isNaN(v) && v >= 0 && v <= 100) {{
        currentMoisture = v;
        updateKBZHU(currentProduct);
        updateLosses(currentProduct);
    }}
}}

function updateKBZHU(prod) {{
    const el = document.getElementById('kbzhu-cards');
    if (!el || !prod) return;

    const rawKg = prod.total_raw_kg || 0;
    if (rawKg === 0) {{ el.innerHTML = '<div class="kbzhu-note">Нет данных по сырью</div>'; return; }}

    // КБЖУ сырья (в граммах на всю загрузку)
    const raw = prod.kbzhu_raw;
    // Масса выхода с учётом влажности продукта
    // Формула: КБЖУ на 100г готового = (КБЖУ_сырья_граммы / масса_выхода_граммы) * 100
    // Масса выхода: если есть spec_output, используем его, иначе rawKg * (1 - потери)
    // Потери: считаем что влажность финального продукта = currentMoisture%
    // Сухие вещества на входе = dry_input_kg
    // Масса готового продукта = dry_input / (1 - moisture_product/100)
    const dryIn = prod.dry_input_kg || rawKg;
    const outputKg = dryIn / (1 - currentMoisture / 100);
    const outputG = outputKg * 1000;

    const p100 = outputG > 0 ? (raw.protein / outputG) * 100 : 0;
    const f100 = outputG > 0 ? (raw.fat / outputG) * 100 : 0;
    const c100 = outputG > 0 ? (raw.carbs / outputG) * 100 : 0;
    const s100 = outputG > 0 ? (raw.sugar / outputG) * 100 : 0;
    const cal100 = outputG > 0 ? (raw.calories / outputG) * 100 : 0;

    el.innerHTML = `
        <div class="kbzhu-card"><div class="value">${{fmt(p100, 1)}}</div><div class="label">Белки, г</div></div>
        <div class="kbzhu-card"><div class="value">${{fmt(f100, 1)}}</div><div class="label">Жиры, г</div></div>
        <div class="kbzhu-card"><div class="value">${{fmt(c100, 1)}}</div><div class="label">Углеводы, г</div></div>
        <div class="kbzhu-card"><div class="value">${{fmt(s100, 1)}}</div><div class="label">Сахар, г</div></div>
        <div class="kbzhu-card"><div class="value">${{fmt(cal100, 0)}}</div><div class="label">кКал</div></div>
    `;

    const noteEl = document.getElementById('kbzhu-note');
    if (noteEl) {{
        noteEl.textContent = `Расчёт на 100 г готового продукта при влажности ${{currentMoisture}}%. Покрытие КБЖУ: ${{prod.kbzhu_coverage}} из ${{prod.kbzhu_total_materials}} ингредиентов.`;
    }}
}}

function updateLosses(prod) {{
    const el = document.getElementById('losses-content');
    if (!el || !prod) return;

    const rawKg = prod.total_raw_kg || 0;
    if (rawKg === 0) {{ el.innerHTML = '<div class="kbzhu-note">Нет данных</div>'; return; }}

    // 1. Чистые потери (общие)
    // Сухие вещества на входе
    const dryIn = prod.dry_input_kg || rawKg;
    // Масса готового продукта при заданной влажности
    const outputKg = dryIn / (1 - currentMoisture / 100);
    const totalLoss = rawKg - outputKg;
    const totalLossPct = rawKg > 0 ? (totalLoss / rawKg) * 100 : 0;

    // 2. Потери влаги
    const moistureIn = prod.moisture_input_kg || 0;
    const moistureOut = outputKg * (currentMoisture / 100);
    const moistureLoss = moistureIn - moistureOut;

    // 3. Потери сухих веществ = чистые потери - потери влаги
    const dryOut = outputKg * (1 - currentMoisture / 100);
    const dryLoss = dryIn - dryOut;
    const dryLossPct = dryIn > 0 ? (dryLoss / dryIn) * 100 : 0;

    el.innerHTML = `
    <div class="losses-grid">
        <div class="loss-card">
            <h4>Общие потери</h4>
            <div class="loss-row"><span class="lbl">Масса сырья на входе</span><span class="val">${{fmt(rawKg, 4)}} кг</span></div>
            <div class="loss-row"><span class="lbl">Масса продукта на выходе</span><span class="val">${{fmt(outputKg, 4)}} кг</span></div>
            <div class="loss-row"><span class="lbl">Потери (абс.)</span><span class="val${{totalLoss > 0 ? ' negative' : ''}}">${{fmt(totalLoss, 4)}} кг</span></div>
            <div class="loss-row"><span class="lbl">Потери (%)</span><span class="val${{totalLossPct > 0 ? ' negative' : ''}}">${{fmt(totalLossPct, 1)}}%</span></div>
        </div>
        <div class="loss-card">
            <h4>Потери сухих веществ</h4>
            <div class="loss-row"><span class="lbl">Сухие вещества на входе</span><span class="val">${{fmt(dryIn, 4)}} кг</span></div>
            <div class="loss-row"><span class="lbl">Сухие вещества на выходе</span><span class="val">${{fmt(dryOut, 4)}} кг</span></div>
            <div class="loss-row"><span class="lbl">Потери сухих в-в (абс.)</span><span class="val${{dryLoss > 0 ? ' negative' : ''}}">${{fmt(dryLoss, 6)}} кг</span></div>
            <div class="loss-row"><span class="lbl">Потери сухих в-в (%)</span><span class="val${{dryLossPct > 0 ? ' negative' : ''}}">${{fmt(dryLossPct, 2)}}%</span></div>
            <div class="loss-row"><span class="lbl">Потери влаги</span><span class="val">${{fmt(moistureLoss, 4)}} кг</span></div>
        </div>
    </div>
    `;
}}

function renderProduct(prod) {{
    let artCode = '';
    if (prod.article) artCode += 'арт. ' + prod.article;
    if (prod.code) artCode += (artCode ? ' / ' : '') + 'код ' + prod.code;

    let html = `<div class="product-header">
        <h2>${{prod.name}}</h2>
        <span class="art-code">${{artCode}}</span>
    </div>`;

    // Tabs
    html += `<div class="tabs">
        <div class="tab active" data-tab="tab-bom" onclick="switchTab('tab-bom')">Спецификация</div>
        <div class="tab" data-tab="tab-composition" onclick="switchTab('tab-composition')">Состав (ТР ТС)</div>
        <div class="tab" data-tab="tab-allergens" onclick="switchTab('tab-allergens')">Аллергены</div>
        <div class="tab" data-tab="tab-kbzhu" onclick="switchTab('tab-kbzhu')">КБЖУ</div>
        <div class="tab" data-tab="tab-losses" onclick="switchTab('tab-losses')">Потери</div>
    </div>`;

    // === TAB: BOM ===
    html += '<div class="tab-content active" id="tab-bom">';
    html += '<table class="bom-table">';
    html += '<thead><tr><th>Материал</th><th class="num">Ед.</th><th class="num">Кол-во</th><th class="num">Цена, ₽</th><th class="num">Стоимость, ₽</th></tr></thead>';
    html += '<tbody>';

    for (const group of prod.groups) {{
        html += `<tr class="group-header"><td colspan="5">${{group.name}}</td></tr>`;
        for (const mat of group.materials) {{
            const pc = mat.price === 0 ? ' no-price' : '';
            html += `<tr>
                <td style="padding-left:28px">${{mat.name}}</td>
                <td class="num">${{mat.unit}}</td>
                <td class="num">${{fmt(mat.qty, 4)}}</td>
                <td class="num${{pc}}">${{fmt(mat.price, 2)}}</td>
                <td class="num">${{fmt(mat.cost, 2)}}</td>
            </tr>`;
        }}
        html += `<tr class="group-subtotal">
            <td style="padding-left:28px">Итого ${{group.name}}</td>
            <td class="num">кг</td>
            <td class="num">${{fmt(group.total_kg, 4)}}</td>
            <td class="num"></td>
            <td class="num">${{fmt(group.total_cost, 2)}}</td>
        </tr>`;
    }}

    // Итого по сырью (Себестоимость)
    html += `<tr class="raw-total">
        <td>СЫРЬЁ (Себестоимость)</td>
        <td class="num">кг</td>
        <td class="num">${{fmt(prod.raw_kg, 4)}}</td>
        <td class="num"></td>
        <td class="num">${{fmt(prod.raw_cost, 2)}}</td>
    </tr>`;

    html += `<tr class="grand-total">
        <td>ИТОГО на 1 единицу</td>
        <td class="num">кг</td>
        <td class="num">${{fmt(prod.total_kg, 4)}}</td>
        <td class="num"></td>
        <td class="num">${{fmt(prod.total_cost, 2)}}</td>
    </tr>`;

    html += '</tbody></table></div>';

    // === TAB: Состав (ТР ТС) ===
    html += '<div class="tab-content" id="tab-composition"><div class="info-section">';
    html += '<h3>Состав продукта (ТР ТС 022/2011)</h3>';

    if (prod.composition && prod.composition.length > 0) {{
        // Основные (>= 2%) и мелкие (< 2%)
        const main = prod.composition.filter(c => c.pct >= 2);
        const minor = prod.composition.filter(c => c.pct < 2);

        let text = '<p class="composition-text">';
        text += main.map(c => `<strong>${{c.name}}</strong> (${{fmt(c.pct, 1)}}%)`).join(', ');

        if (minor.length > 0) {{
            text += ', <span class="minor">' + minor.map(c => c.name + ' (' + fmt(c.pct, 1) + '%)').join(', ') + '</span>';
        }}
        text += '.</p>';
        html += text;
    }} else {{
        html += '<p class="composition-text" style="color:#999">Нет данных по составу</p>';
    }}
    html += '</div></div>';

    // === TAB: Аллергены ===
    html += '<div class="tab-content" id="tab-allergens"><div class="info-section">';
    html += '<h3>Аллергены (ТР ТС 029/2012)</h3>';

    if (prod.allergens && prod.allergens.length > 0) {{
        html += '<div class="allergen-list">';
        for (const a of prod.allergens) {{
            const displayName = ALLERGEN_NAMES[a] || a;
            html += `<span class="allergen-tag">${{displayName}}</span>`;
        }}
        html += '</div>';
    }} else {{
        html += '<p class="no-allergens">Аллергены не обнаружены</p>';
    }}
    html += '</div></div>';

    // === TAB: КБЖУ ===
    html += '<div class="tab-content" id="tab-kbzhu"><div class="info-section">';
    html += '<h3>Пищевая ценность на 100 г готового продукта</h3>';
    html += `<div class="moisture-row">
        <label>Влажность готового продукта, %:</label>
        <input type="number" class="moisture-input" id="moistureInput" value="${{currentMoisture}}" min="0" max="99" step="0.5"
               onchange="onMoistureChange(this.value)" oninput="onMoistureChange(this.value)">
        <span style="font-size:12px;color:#999">(по умолчанию 17%)</span>
    </div>`;
    html += '<div class="kbzhu-grid" id="kbzhu-cards"></div>';
    html += '<div class="kbzhu-note" id="kbzhu-note"></div>';
    html += '</div></div>';

    // === TAB: Потери ===
    html += '<div class="tab-content" id="tab-losses"><div class="info-section">';
    html += '<h3>Потери при производстве</h3>';
    html += `<div class="moisture-row">
        <label>Влажность готового продукта, %:</label>
        <input type="number" class="moisture-input" id="moistureInputLosses" value="${{currentMoisture}}" min="0" max="99" step="0.5"
               onchange="syncMoisture(this.value)" oninput="syncMoisture(this.value)">
        <span style="font-size:12px;color:#999">(по умолчанию 17%)</span>
    </div>`;
    html += '<div id="losses-content"></div>';
    html += '</div></div>';

    result.innerHTML = html;

    // Инициализация КБЖУ и потерь
    updateKBZHU(prod);
    updateLosses(prod);
}}

function syncMoisture(val) {{
    const v = parseFloat(val);
    if (!isNaN(v) && v >= 0 && v <= 100) {{
        currentMoisture = v;
        // Синхронизируем оба поля
        const el1 = document.getElementById('moistureInput');
        const el2 = document.getElementById('moistureInputLosses');
        if (el1) el1.value = v;
        if (el2) el2.value = v;
        updateKBZHU(currentProduct);
        updateLosses(currentProduct);
    }}
}}
</script>
</body>
</html>'''

    return html


def main():
    print("Загрузка данных BOM v2...")
    data = fetch_data()

    if not data:
        print("Нет данных")
        return

    rows = data['rows']
    print(f"  Строк BOM: {len(rows)}, цен: {len(data['prices'])}")
    print(f"  КБЖУ: {len(data['nutrition'])}, артикулов: {len(data['product_articles'])}")
    print(f"  Спецификаций: {len(data['specs'])}")

    product_list = build_products_json(data)
    print(f"  Продуктов: {len(product_list)}")

    html = generate_html(product_list, {'calc': data['calc'], 'errors': data['errors']})

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"  HTML: {OUTPUT_FILE} ({len(html):,} bytes)")
    print("Готово!")


if __name__ == "__main__":
    main()
