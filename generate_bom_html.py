#!/usr/bin/env python3
"""
Генератор HTML-отчёта по составу продукции (BOM).
Создаёт статический HTML с встроенными данными и интерактивным выбором.
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


def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def fetch_data():
    """Загрузка данных BOM и цен из БД"""
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
        return None, None, None
    
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
    
    # Ошибки
    cur.execute("""
        SELECT product_name, semifinished_name, error_type, details
        FROM bom_errors WHERE calculation_id = %s
    """, (calc_id,))
    errors = cur.fetchall()
    
    cur.close()
    conn.close()
    
    return rows, prices, {
        'calc': calc,
        'errors': errors
    }


def build_products_json(rows, prices):
    """Построение JSON структуры для фронтенда"""
    products = {}
    
    for row in rows:
        pk = row['product_key']
        if pk not in products:
            products[pk] = {
                'name': row['product_name'],
                'key': pk,
                'groups': {}
            }
        
        type_name = row['type_name'] or 'Прочее'
        if type_name not in products[pk]['groups']:
            products[pk]['groups'][type_name] = {
                'name': type_name,
                'level1': row['type_level_1'] or 'Прочее',
                'materials': [],
                'total_kg': 0,
                'total_cost': 0
            }
        
        price = prices.get(row['material_key'], 0)
        qty = float(row['quantity_per_unit'])
        cost = qty * price
        unit = row['material_unit'] or 'шт'
        
        # Пересчёт в кг
        qty_kg = 0
        if unit in ('кг', 'kg'):
            qty_kg = qty
        elif unit in ('г', 'гр', 'g'):
            qty_kg = qty / 1000
        elif unit in ('л', 'l'):
            qty_kg = qty
        elif unit in ('мл', 'ml'):
            qty_kg = qty / 1000
        
        products[pk]['groups'][type_name]['materials'].append({
            'name': row['material_name'],
            'unit': unit,
            'qty': round(qty, 4),
            'price': round(price, 2),
            'cost': round(cost, 2),
            'qty_kg': round(qty_kg, 6)
        })
        products[pk]['groups'][type_name]['total_kg'] += qty_kg
        products[pk]['groups'][type_name]['total_cost'] += cost
    
    # Округляем итоги и сортируем
    product_list = []
    for pk, prod in sorted(products.items(), key=lambda x: x[1]['name']):
        groups = []
        total_kg = 0
        total_cost = 0
        for gname, g in sorted(prod['groups'].items()):
            g['total_kg'] = round(g['total_kg'], 4)
            g['total_cost'] = round(g['total_cost'], 2)
            total_kg += g['total_kg']
            total_cost += g['total_cost']
            groups.append(g)
        product_list.append({
            'name': prod['name'],
            'key': prod['key'],
            'groups': groups,
            'total_kg': round(total_kg, 4),
            'total_cost': round(total_cost, 2)
        })
    
    return product_list


def generate_html(product_list, meta):
    """Генерация HTML файла"""
    
    calc = meta['calc']
    calc_date = calc['started_at'].strftime('%d.%m.%Y %H:%M') if calc['started_at'] else ''
    
    products_json = json.dumps(product_list, ensure_ascii=False, default=decimal_default)
    
    html = f'''<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Состав продукции — Фрумелад</title>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f5f7fa; color: #333; }}

.header {{ background: linear-gradient(135deg, #1a5276, #2e86c1); color: white; padding: 20px 30px; }}
.header h1 {{ font-size: 22px; font-weight: 600; }}
.header .meta {{ font-size: 13px; opacity: 0.8; margin-top: 4px; }}

.container {{ max-width: 1100px; margin: 0 auto; padding: 20px; }}

.search-box {{ 
    position: sticky; top: 0; z-index: 100; 
    background: white; border-radius: 12px; 
    box-shadow: 0 2px 12px rgba(0,0,0,0.1); 
    padding: 16px 20px; margin-bottom: 20px; 
}}
.search-box label {{ font-size: 14px; font-weight: 600; color: #555; display: block; margin-bottom: 8px; }}
.search-box select {{ 
    width: 100%; padding: 10px 14px; font-size: 15px; 
    border: 2px solid #ddd; border-radius: 8px; 
    background: white; cursor: pointer; 
    appearance: none;
    background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath fill='%23666' d='M6 8L1 3h10z'/%3E%3C/svg%3E");
    background-repeat: no-repeat;
    background-position: right 12px center;
}}
.search-box select:focus {{ border-color: #2e86c1; outline: none; }}
.search-input {{ 
    width: 100%; padding: 10px 14px; font-size: 15px; 
    border: 2px solid #ddd; border-radius: 8px; margin-bottom: 8px;
}}
.search-input:focus {{ border-color: #2e86c1; outline: none; }}
.dropdown {{ 
    max-height: 300px; overflow-y: auto; 
    border: 2px solid #ddd; border-radius: 8px; 
    background: white; display: none;
}}
.dropdown.open {{ display: block; }}
.dropdown-item {{ 
    padding: 10px 14px; cursor: pointer; font-size: 14px;
    border-bottom: 1px solid #f0f0f0;
}}
.dropdown-item:hover {{ background: #e8f4fc; }}
.dropdown-item.selected {{ background: #d4edfa; font-weight: 600; }}

.product-title {{ 
    background: #2e86c1; color: white; 
    padding: 14px 20px; border-radius: 10px 10px 0 0;
    font-size: 17px; font-weight: 600; margin-top: 10px;
}}

.bom-table {{ 
    width: 100%; border-collapse: collapse; 
    background: white; border-radius: 0 0 10px 10px;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
    overflow: hidden;
}}
.bom-table th {{ 
    background: #f8f9fa; padding: 10px 14px; 
    text-align: left; font-size: 13px; 
    font-weight: 600; color: #666; 
    border-bottom: 2px solid #e9ecef; 
}}
.bom-table th.num {{ text-align: right; }}
.bom-table td {{ padding: 8px 14px; font-size: 13px; border-bottom: 1px solid #f0f0f0; }}
.bom-table td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}

.group-header td {{ 
    background: #edf2f7; font-weight: 600; 
    font-size: 13px; color: #2c5282; 
    padding: 10px 14px; border-bottom: 2px solid #cbd5e0;
}}
.group-subtotal td {{ 
    background: #f7fafc; font-weight: 600; 
    font-size: 13px; color: #4a5568;
    border-top: 1px solid #e2e8f0;
}}
.grand-total td {{ 
    background: #2e86c1; color: white; 
    font-weight: 700; font-size: 14px; 
    padding: 12px 14px; 
}}

.no-price {{ color: #e53e3e; }}
.empty {{ text-align: center; padding: 60px; color: #999; font-size: 16px; }}

.errors {{ 
    margin-top: 16px; background: #fff5f5; 
    border: 1px solid #feb2b2; border-radius: 8px; 
    padding: 14px; display: none;
}}
.errors h3 {{ color: #c53030; font-size: 14px; margin-bottom: 8px; }}
.errors li {{ font-size: 13px; color: #742a2a; margin-left: 20px; }}
</style>
</head>
<body>

<div class="header">
    <h1>Состав продукции на 1 единицу</h1>
    <div class="meta">Расчёт от {calc_date} &bull; {calc['products_processed']} продуктов &bull; {calc['materials_total']} материалов</div>
</div>

<div class="container">
    <div class="search-box">
        <label>Выберите продукт:</label>
        <input type="text" class="search-input" id="searchInput" placeholder="Начните вводить название..." autocomplete="off">
        <div class="dropdown" id="dropdown"></div>
    </div>
    
    <div id="result" class="empty">Выберите продукт из списка выше</div>
</div>

<script>
const PRODUCTS = {products_json};

const searchInput = document.getElementById('searchInput');
const dropdown = document.getElementById('dropdown');
const result = document.getElementById('result');

let selectedIndex = -1;
let filteredProducts = [];

function renderDropdown(filter) {{
    if (!filter || filter.length < 1) {{
        dropdown.classList.remove('open');
        return;
    }}
    const lower = filter.toLowerCase();
    filteredProducts = PRODUCTS.filter(p => p.name.toLowerCase().includes(lower));
    
    if (filteredProducts.length === 0) {{
        dropdown.innerHTML = '<div class="dropdown-item" style="color:#999">Ничего не найдено</div>';
        dropdown.classList.add('open');
        return;
    }}
    
    dropdown.innerHTML = filteredProducts.map((p, i) => 
        `<div class="dropdown-item" data-idx="${{i}}" onclick="selectProduct(${{i}})">${{p.name}}</div>`
    ).join('');
    dropdown.classList.add('open');
    selectedIndex = -1;
}}

searchInput.addEventListener('input', (e) => renderDropdown(e.target.value));
searchInput.addEventListener('focus', (e) => {{ if (e.target.value) renderDropdown(e.target.value); }});
document.addEventListener('click', (e) => {{ 
    if (!e.target.closest('.search-box')) dropdown.classList.remove('open'); 
}});

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
    const prod = filteredProducts[idx];
    searchInput.value = prod.name;
    dropdown.classList.remove('open');
    renderProduct(prod);
}}

function fmt(n, decimals) {{ 
    if (n === null || n === undefined) return '';
    return n.toFixed(decimals).replace(/\\.?0+$/, '') || '0'; 
}}

function renderProduct(prod) {{
    let html = `<div class="product-title">▶ ${{prod.name}}</div>`;
    html += '<table class="bom-table">';
    html += '<thead><tr><th>Материал</th><th class="num">Ед.</th><th class="num">Кол-во</th><th class="num">Цена, ₽</th><th class="num">Стоимость, ₽</th></tr></thead>';
    html += '<tbody>';
    
    for (const group of prod.groups) {{
        html += `<tr class="group-header"><td colspan="5">◆ ${{group.name}}</td></tr>`;
        
        for (const mat of group.materials) {{
            const priceClass = mat.price === 0 ? ' no-price' : '';
            html += `<tr>
                <td>&nbsp;&nbsp;&nbsp;&nbsp;${{mat.name}}</td>
                <td class="num">${{mat.unit}}</td>
                <td class="num">${{fmt(mat.qty, 4)}}</td>
                <td class="num${{priceClass}}">${{fmt(mat.price, 2)}}</td>
                <td class="num">${{fmt(mat.cost, 2)}}</td>
            </tr>`;
        }}
        
        html += `<tr class="group-subtotal">
            <td>&nbsp;&nbsp;&nbsp;&nbsp;Итого ${{group.name}}</td>
            <td class="num">кг</td>
            <td class="num">${{fmt(group.total_kg, 4)}}</td>
            <td class="num"></td>
            <td class="num">${{fmt(group.total_cost, 2)}}</td>
        </tr>`;
    }}
    
    html += `<tr class="grand-total">
        <td>СЕБЕСТОИМОСТЬ на 1 ед.</td>
        <td class="num">кг</td>
        <td class="num">${{fmt(prod.total_kg, 4)}}</td>
        <td class="num"></td>
        <td class="num">${{fmt(prod.total_cost, 2)}}</td>
    </tr>`;
    
    html += '</tbody></table>';
    result.innerHTML = html;
}}
</script>
</body>
</html>'''
    
    return html


def main():
    print("Загрузка данных BOM...")
    rows, prices, meta = fetch_data()
    
    if not rows:
        print("Нет данных")
        return
    
    print(f"  Строк: {len(rows)}, цен: {len(prices)}")
    
    product_list = build_products_json(rows, prices)
    print(f"  Продуктов: {len(product_list)}")
    
    html = generate_html(product_list, meta)
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(html)
    
    print(f"  HTML: {OUTPUT_FILE} ({len(html):,} bytes)")
    print("Готово!")


if __name__ == "__main__":
    main()
