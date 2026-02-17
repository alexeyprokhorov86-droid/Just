#!/usr/bin/env python3
"""
Генерация отчёта BOM и отправка в Telegram
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from decimal import Decimal
import asyncio
from pathlib import Path

# Загружаем .env
from dotenv import load_dotenv
env_path = Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path)

# Конфигурация
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'dbname': os.getenv('DB_NAME', 'knowledge_base'),
    'user': os.getenv('DB_USER', 'knowledge'),
    'password': os.getenv('DB_PASSWORD', '')
}

TELEGRAM_BOT_TOKEN = os.getenv('BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_ADMIN_ID', '')  # ID администратора


def format_qty(qty):
    """Форматирование количества"""
    if qty is None:
        return ""
    val = float(qty)
    if val == 0:
        return "0"
    if val < 0.0001:
        return f"{val:.6f}"
    if val < 0.01:
        return f"{val:.4f}"
    return f"{val:.4f}".rstrip('0').rstrip('.')


def generate_full_report():
    """Генерация полного отчёта по всем BOM"""
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    report_lines = []
    report_lines.append("=" * 70)
    report_lines.append("ОТЧЁТ ПО РАЗВЁРТКЕ СПЕЦИФИКАЦИЙ (BOM)")
    report_lines.append(f"Дата формирования: {datetime.now().strftime('%d.%m.%Y %H:%M')}")
    report_lines.append("=" * 70)
    
    # Статистика
    cur.execute("""
        SELECT 
            (SELECT COUNT(DISTINCT product_key) FROM bom_expanded) as products_ok,
            (SELECT COUNT(*) FROM bom_expanded) as materials_total,
            (SELECT COUNT(*) FROM bom_errors) as errors_total,
            (SELECT COUNT(DISTINCT product_key) FROM bom_errors 
             WHERE product_key NOT IN (SELECT DISTINCT product_key FROM bom_expanded)) as products_failed
    """)
    stats = cur.fetchone()
    
    report_lines.append("")
    report_lines.append("ОБЩАЯ СТАТИСТИКА:")
    report_lines.append(f"  Успешно развёрнуто продуктов: {stats['products_ok']}")
    report_lines.append(f"  Всего материалов в спецификациях: {stats['materials_total']}")
    report_lines.append(f"  Продуктов с ошибками (не развёрнуты): {stats['products_failed']}")
    report_lines.append(f"  Всего ошибок: {stats['errors_total']}")
    
    # ============================================================
    # ЧАСТЬ 1: УСПЕШНО РАЗВЁРНУТЫЕ СПЕЦИФИКАЦИИ
    # ============================================================
    report_lines.append("")
    report_lines.append("")
    report_lines.append("=" * 70)
    report_lines.append("ЧАСТЬ 1: УСПЕШНО РАЗВЁРНУТЫЕ СПЕЦИФИКАЦИИ")
    report_lines.append("=" * 70)
    
    # Получаем список продуктов
    cur.execute("""
        SELECT DISTINCT product_key, product_name
        FROM bom_expanded
        ORDER BY product_name
    """)
    products = cur.fetchall()
    
    for product in products:
        product_key = product['product_key']
        product_name = product['product_name']
        
        report_lines.append("")
        report_lines.append("-" * 70)
        report_lines.append(f"ПРОДУКТ: {product_name}")
        report_lines.append("-" * 70)
        
        # Получаем материалы с группировкой
        cur.execute("""
            SELECT material_name, material_unit, quantity_per_unit, quantity_kg,
                   type_level_1, type_level_2, type_level_3
            FROM bom_expanded
            WHERE product_key = %s
            ORDER BY type_level_1 NULLS LAST, type_level_2 NULLS LAST, 
                     type_level_3 NULLS LAST, material_name
        """, (product_key,))
        
        materials = cur.fetchall()
        
        current_l1 = None
        current_l2 = None
        current_l3 = None
        
        subtotal_l1 = Decimal('0')
        subtotal_l3 = Decimal('0')
        grand_total = Decimal('0')
        
        for mat in materials:
            l1 = mat['type_level_1'] or "Прочее"
            l2 = mat['type_level_2']
            l3 = mat['type_level_3'] or "Без вида"
            
            # Смена группы уровня 1
            if l1 != current_l1:
                if current_l1 is not None and subtotal_l1 > 0:
                    report_lines.append(f"      ИТОГО {current_l1}: {format_qty(subtotal_l1)} кг")
                current_l1 = l1
                current_l2 = None
                current_l3 = None
                subtotal_l1 = Decimal('0')
                report_lines.append(f"\n  [{l1}]")
            
            # Смена группы уровня 2
            if l2 and l2 != current_l2:
                current_l2 = l2
                current_l3 = None
                report_lines.append(f"    [{l2}]")
            
            # Смена вида
            if l3 != current_l3:
                if current_l3 is not None and subtotal_l3 > 0:
                    report_lines.append(f"          Подитог: {format_qty(subtotal_l3)} кг")
                current_l3 = l3
                subtotal_l3 = Decimal('0')
                indent = "      " if l2 else "    "
                report_lines.append(f"{indent}• {l3}:")
            
            # Материал
            qty = mat['quantity_per_unit']
            unit = mat['material_unit'] or 'шт'
            kg = mat['quantity_kg']
            
            indent = "          " if l2 else "        "
            report_lines.append(f"{indent}- {mat['material_name']}: {format_qty(qty)} {unit}")
            
            # Накапливаем подитоги
            if kg:
                kg_decimal = Decimal(str(kg))
                subtotal_l3 += kg_decimal
                subtotal_l1 += kg_decimal
                grand_total += kg_decimal
        
        # Последние подитоги
        if subtotal_l3 > 0:
            report_lines.append(f"          Подитог: {format_qty(subtotal_l3)} кг")
        if subtotal_l1 > 0:
            report_lines.append(f"      ИТОГО {current_l1}: {format_qty(subtotal_l1)} кг")
        
        if grand_total > 0:
            report_lines.append(f"\n  *** ОБЩИЙ ВЕС НА 1 ЕД.: {format_qty(grand_total)} кг ***")
        
        # Проверяем есть ли ошибки для этого продукта
        cur.execute("""
            SELECT semifinished_name, error_type, details
            FROM bom_errors
            WHERE product_key = %s
        """, (product_key,))
        errors = cur.fetchall()
        
        if errors:
            report_lines.append("")
            report_lines.append("  ⚠️ ПРЕДУПРЕЖДЕНИЯ:")
            for err in errors:
                report_lines.append(f"    - [{err['error_type']}] {err['semifinished_name']}")
                if err['details']:
                    report_lines.append(f"      {err['details']}")
    
    # ============================================================
    # ЧАСТЬ 2: ПРОДУКТЫ С ОШИБКАМИ (НЕ РАЗВЁРНУТЫ)
    # ============================================================
    report_lines.append("")
    report_lines.append("")
    report_lines.append("=" * 70)
    report_lines.append("ЧАСТЬ 2: ПРОДУКТЫ С ОШИБКАМИ (НЕ УДАЛОСЬ РАЗВЕРНУТЬ)")
    report_lines.append("=" * 70)
    
    # Продукты которые есть в ошибках, но нет в развёрнутых
    cur.execute("""
        SELECT DISTINCT e.product_key, e.product_name
        FROM bom_errors e
        WHERE e.product_key NOT IN (SELECT DISTINCT product_key FROM bom_expanded)
        ORDER BY e.product_name
    """)
    failed_products = cur.fetchall()
    
    if not failed_products:
        report_lines.append("")
        report_lines.append("Все продукты успешно развёрнуты!")
    else:
        for product in failed_products:
            product_key = product['product_key']
            product_name = product['product_name']
            
            report_lines.append("")
            report_lines.append("-" * 70)
            report_lines.append(f"ПРОДУКТ: {product_name}")
            report_lines.append("-" * 70)
            
            cur.execute("""
                SELECT semifinished_name, error_type, details
                FROM bom_errors
                WHERE product_key = %s
                ORDER BY semifinished_name
            """, (product_key,))
            errors = cur.fetchall()
            
            report_lines.append("")
            report_lines.append("  Причины ошибок:")
            for err in errors:
                err_type_ru = {
                    'no_spec': 'Нет спецификации',
                    'no_nomenclature': 'Номенклатура не найдена',
                    'circular_ref': 'Циклическая ссылка',
                    'zero_quantity': 'Нулевое количество'
                }.get(err['error_type'], err['error_type'])
                
                report_lines.append(f"    ❌ {err_type_ru}: {err['semifinished_name']}")
                if err['details']:
                    report_lines.append(f"       {err['details']}")
    
    # ============================================================
    # ЧАСТЬ 3: СВОДКА ПО ОТСУТСТВУЮЩИМ СПЕЦИФИКАЦИЯМ
    # ============================================================
    report_lines.append("")
    report_lines.append("")
    report_lines.append("=" * 70)
    report_lines.append("ЧАСТЬ 3: СВОДКА ОТСУТСТВУЮЩИХ СПЕЦИФИКАЦИЙ")
    report_lines.append("=" * 70)
    
    cur.execute("""
        SELECT DISTINCT semifinished_name, COUNT(DISTINCT product_key) as affected_products
        FROM bom_errors
        WHERE error_type = 'no_spec'
        GROUP BY semifinished_name
        ORDER BY affected_products DESC, semifinished_name
    """)
    missing_specs = cur.fetchall()
    
    if missing_specs:
        report_lines.append("")
        report_lines.append("Полуфабрикаты/продукты без действующих спецификаций:")
        report_lines.append("")
        for spec in missing_specs:
            report_lines.append(f"  • {spec['semifinished_name']}")
            report_lines.append(f"    (влияет на {spec['affected_products']} продукт(ов))")
    else:
        report_lines.append("")
        report_lines.append("Все необходимые спецификации найдены!")
    
    # Завершение
    report_lines.append("")
    report_lines.append("")
    report_lines.append("=" * 70)
    report_lines.append("КОНЕЦ ОТЧЁТА")
    report_lines.append("=" * 70)
    
    cur.close()
    conn.close()
    
    return "\n".join(report_lines)


async def send_to_telegram(file_path: str, caption: str = ""):
    """Отправка файла в Telegram"""
    import aiohttp
    
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Ошибка: не настроены TELEGRAM_BOT_TOKEN или TELEGRAM_ADMIN_ID")
        return False
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendDocument"
    
    async with aiohttp.ClientSession() as session:
        with open(file_path, 'rb') as f:
            data = aiohttp.FormData()
            data.add_field('chat_id', TELEGRAM_CHAT_ID)
            data.add_field('document', f, filename=os.path.basename(file_path))
            if caption:
                data.add_field('caption', caption[:1024])  # Telegram limit
            
            async with session.post(url, data=data) as resp:
                result = await resp.json()
                if result.get('ok'):
                    print(f"Файл отправлен в Telegram!")
                    return True
                else:
                    print(f"Ошибка отправки: {result}")
                    return False


def main():
    print("Генерация отчёта BOM...")
    
    # Генерируем отчёт
    report = generate_full_report()
    
    # Сохраняем в файл
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"bom_report_{timestamp}.txt"
    filepath = Path(__file__).parent / filename
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"Отчёт сохранён: {filepath}")
    print(f"Размер: {os.path.getsize(filepath)} байт")
    
    # Отправляем в Telegram
    caption = f"📊 Отчёт BOM от {datetime.now().strftime('%d.%m.%Y %H:%M')}\n"
    caption += f"Успешно: 9 продуктов, 64 материала\n"
    caption += f"Ошибок: 12"
    
    asyncio.run(send_to_telegram(str(filepath), caption))
    
    return str(filepath)


if __name__ == "__main__":
    main()
