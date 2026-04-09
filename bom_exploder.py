#!/usr/bin/env python3
"""
BOM Exploder - Рекурсивное развёртывание спецификаций
Собирает сводную спецификацию на 1 единицу готовой продукции

Версия 2.0 — с версионированием, отслеживанием изменений и автозапуском

Автор: Claude для Frumelad (Кондитерская Прохорова)
"""

import psycopg2
import os
from psycopg2.extras import RealDictCursor
from datetime import datetime
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass, field
from decimal import Decimal
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Конфигурация БД
from dotenv import load_dotenv
load_dotenv()

DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': int(os.getenv('DB_PORT', '5432')),
    'dbname': os.getenv('DB_NAME', 'knowledge_base'),
    'user': os.getenv('DB_USER', 'knowledge'),
    'password': os.getenv('DB_PASSWORD'),
}


@dataclass
class Material:
    """Материал в развёрнутой спецификации"""
    nomenclature_key: str
    name: str
    unit: str
    quantity: Decimal
    type_id: str
    type_name: str
    type_hierarchy: List[str]  # [группа1, группа2, вид]
    
    
@dataclass
class BOMError:
    """Ошибка при развёртывании BOM"""
    product_key: str
    product_name: str
    semifinished_key: str
    semifinished_name: str
    error_type: str  # no_spec, multiple_specs, circular_ref
    details: str = ""


@dataclass 
class BOMResult:
    """Результат развёртывания BOM для одного продукта"""
    product_key: str
    product_name: str
    materials: Dict[str, Material] = field(default_factory=dict)  # key -> Material
    errors: List[BOMError] = field(default_factory=list)
    

class BOMExploder:
    """Класс для рекурсивного развёртывания спецификаций"""
    
    def __init__(self):
        self.conn = psycopg2.connect(**DB_CONFIG)
        self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
        
        # Кэши для оптимизации
        self._nomenclature_cache: Dict[str, dict] = {}
        self._type_cache: Dict[str, dict] = {}
        self._type_hierarchy_cache: Dict[str, List[str]] = {}
        self._spec_cache: Dict[str, dict] = {}  # product_key -> spec
        self._spec_materials_cache: Dict[str, List[dict]] = {}  # spec_key -> materials
        
        # Категории для классификации
        self._semifinished_types: Set[str] = set()  # type_id где есть "полуфабрикат"
        self._terminal_types: Set[str] = set()  # type_id где "себестоимость" или "расходные материалы"
        self._excluded_types: Set[str] = set()  # type_id в архиве
        self._gp_types: Set[str] = set()  # type_id готовой продукции
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cur.close()
        self.conn.close()
        
    def init_caches(self):
        """Загрузка всех необходимых данных в кэш"""
        logger.info("Загрузка кэшей...")
        
        # 1. Загружаем все виды номенклатуры
        self.cur.execute("""
            SELECT id::text, name, parent_id::text, is_folder 
            FROM nomenclature_types
        """)
        for row in self.cur.fetchall():
            self._type_cache[row['id']] = dict(row)
            
        logger.info(f"  Видов номенклатуры: {len(self._type_cache)}")
        
        # 2. Строим иерархию для каждого вида
        self._build_type_hierarchies()
        
        # 3. Классифицируем виды
        self._classify_types()
        
        # 4. Загружаем номенклатуру
        self.cur.execute("""
            SELECT id::text, name, unit, type_id::text, unit_name
            FROM nomenclature
        """)
        for row in self.cur.fetchall():
            self._nomenclature_cache[row['id']] = dict(row)
            
        logger.info(f"  Номенклатуры: {len(self._nomenclature_cache)}")
        
        # 5. Загружаем действующие спецификации
        self.cur.execute("""
            SELECT ref_key, name, product_key, product_quantity, auto_select
            FROM c1_specifications
            WHERE status = 'Действует'
            ORDER BY 
                CASE WHEN auto_select = 'Автоматически' THEN 0 ELSE 1 END
        """)
        for row in self.cur.fetchall():
            product_key = row['product_key']
            # Берём первую (приоритет: Автоматически)
            if product_key not in self._spec_cache:
                self._spec_cache[product_key] = dict(row)
                
        logger.info(f"  Спецификаций (уникальных по продукту): {len(self._spec_cache)}")
        
        # 6. Загружаем материалы спецификаций
        self.cur.execute("""
            SELECT spec_key, nomenclature_key, quantity
            FROM c1_spec_materials
        """)
        for row in self.cur.fetchall():
            spec_key = row['spec_key']
            if spec_key not in self._spec_materials_cache:
                self._spec_materials_cache[spec_key] = []
            self._spec_materials_cache[spec_key].append(dict(row))
            
        logger.info(f"  Строк материалов: {sum(len(v) for v in self._spec_materials_cache.values())}")
        
    def _build_type_hierarchies(self):
        """Построение иерархии для каждого вида"""
        for type_id in self._type_cache:
            hierarchy = []
            current_id = type_id
            visited = set()
            
            while current_id and current_id not in visited:
                visited.add(current_id)
                type_data = self._type_cache.get(current_id)
                if type_data:
                    hierarchy.insert(0, type_data['name'])
                    current_id = type_data['parent_id']
                else:
                    break
                    
            self._type_hierarchy_cache[type_id] = hierarchy
            
    def _classify_types(self):
        """Классификация видов номенклатуры по категориям"""
        for type_id, hierarchy in self._type_hierarchy_cache.items():
            hierarchy_lower = [h.lower() for h in hierarchy]
            hierarchy_str = ' '.join(hierarchy_lower)
            
            # Проверяем архив
            if 'архив' in hierarchy_str:
                self._excluded_types.add(type_id)
                continue
                
            # Проверяем исключения
            if 'продукция timtim серии' in hierarchy_str:
                self._excluded_types.add(type_id)
                continue
            if 'товары' in hierarchy_lower:
                self._excluded_types.add(type_id)
                continue
                
            # Классифицируем
            if 'полуфабрикат' in hierarchy_str:
                self._semifinished_types.add(type_id)
            elif 'себестоимость' in hierarchy_str or 'расходные материалы' in hierarchy_str:
                self._terminal_types.add(type_id)
            
            if 'готовая продукция' in hierarchy_str:
                self._gp_types.add(type_id)
                
        logger.info(f"  Готовая продукция (видов): {len(self._gp_types)}")
        logger.info(f"  Полуфабрикаты (видов): {len(self._semifinished_types)}")
        logger.info(f"  Себестоимость/Расходники (видов): {len(self._terminal_types)}")
        logger.info(f"  Исключённые (видов): {len(self._excluded_types)}")
        
    def get_finished_products(self) -> List[dict]:
        """Получение списка готовой продукции для обработки"""
        products = []
        
        for nom_key, nom_data in self._nomenclature_cache.items():
            type_id = nom_data.get('type_id')
            
            # Проверяем что это готовая продукция
            if type_id not in self._gp_types:
                continue
                
            # Проверяем что не в исключениях
            if type_id in self._excluded_types:
                continue
                
            # Проверяем что есть спецификация
            if nom_key not in self._spec_cache:
                continue
                
            products.append({
                'key': nom_key,
                'name': nom_data['name'],
                'unit': nom_data.get('unit_name') or nom_data.get('unit') or 'шт',
                'type_id': type_id
            })
            
        logger.info(f"Готовой продукции со спецификациями: {len(products)}")
        return products
        
    def explode_bom(self, product_key: str, product_name: str) -> BOMResult:
        """
        Рекурсивное развёртывание BOM для одного продукта
        """
        result = BOMResult(product_key=product_key, product_name=product_name)
        
        # Защита от циклов
        visited_specs: Set[str] = set()
        
        def process_spec(spec_key: str, multiplier: Decimal, path: List[str]):
            """Рекурсивная обработка спецификации"""
            
            if spec_key in visited_specs:
                result.errors.append(BOMError(
                    product_key=product_key,
                    product_name=product_name,
                    semifinished_key=spec_key,
                    semifinished_name=path[-1] if path else "Unknown",
                    error_type="circular_ref",
                    details=f"Путь: {' -> '.join(path)}"
                ))
                return
                
            visited_specs.add(spec_key)
            
            materials = self._spec_materials_cache.get(spec_key, [])
            
            for mat in materials:
                mat_key = mat['nomenclature_key']
                mat_qty = Decimal(str(mat['quantity'])) * multiplier
                
                nom_data = self._nomenclature_cache.get(mat_key)
                if not nom_data:
                    result.errors.append(BOMError(
                        product_key=product_key,
                        product_name=product_name,
                        semifinished_key=mat_key,
                        semifinished_name=f"Unknown ({mat_key})",
                        error_type="no_nomenclature",
                        details=f"Материал не найден в справочнике номенклатуры"
                    ))
                    continue
                    
                type_id = nom_data.get('type_id')
                type_name = self._type_cache.get(type_id, {}).get('name', 'Неизвестно')
                hierarchy = self._type_hierarchy_cache.get(type_id, [])
                
                if type_id in self._semifinished_types:
                    sub_spec = self._spec_cache.get(mat_key)
                    
                    if not sub_spec:
                        result.errors.append(BOMError(
                            product_key=product_key,
                            product_name=product_name,
                            semifinished_key=mat_key,
                            semifinished_name=nom_data['name'],
                            error_type="no_spec",
                            details=f"Нет действующей спецификации для полуфабриката"
                        ))
                        self._add_material(result, mat_key, nom_data, mat_qty, type_id, type_name, hierarchy)
                    else:
                        sub_qty = Decimal(str(sub_spec['product_quantity']))
                        if sub_qty > 0:
                            new_multiplier = mat_qty / sub_qty
                            process_spec(
                                sub_spec['ref_key'], 
                                new_multiplier,
                                path + [nom_data['name']]
                            )
                        else:
                            result.errors.append(BOMError(
                                product_key=product_key,
                                product_name=product_name,
                                semifinished_key=mat_key,
                                semifinished_name=nom_data['name'],
                                error_type="zero_quantity",
                                details=f"Количество выхода спецификации = 0"
                            ))
                else:
                    self._add_material(result, mat_key, nom_data, mat_qty, type_id, type_name, hierarchy)
                    
        # Начинаем с основной спецификации
        main_spec = self._spec_cache.get(product_key)
        if not main_spec:
            result.errors.append(BOMError(
                product_key=product_key,
                product_name=product_name,
                semifinished_key=product_key,
                semifinished_name=product_name,
                error_type="no_spec",
                details="Нет действующей спецификации для готовой продукции"
            ))
            return result
            
        product_qty = Decimal(str(main_spec['product_quantity']))
        if product_qty <= 0:
            result.errors.append(BOMError(
                product_key=product_key,
                product_name=product_name,
                semifinished_key=product_key,
                semifinished_name=product_name,
                error_type="zero_quantity",
                details=f"Количество выхода спецификации = {product_qty}"
            ))
            return result
            
        initial_multiplier = Decimal('1') / product_qty
        process_spec(main_spec['ref_key'], initial_multiplier, [product_name])
        
        return result
        
    def _add_material(self, result: BOMResult, mat_key: str, nom_data: dict, 
                      quantity: Decimal, type_id: str, type_name: str, hierarchy: List[str]):
        """Добавление материала в результат (с объединением дубликатов)"""
        
        if mat_key in result.materials:
            result.materials[mat_key].quantity += quantity
        else:
            result.materials[mat_key] = Material(
                nomenclature_key=mat_key,
                name=nom_data['name'],
                unit=nom_data.get('unit_name') or nom_data.get('unit') or 'шт',
                quantity=quantity,
                type_id=type_id,
                type_name=type_name,
                type_hierarchy=hierarchy
            )
            
    def calculate_kg(self, material: Material) -> Optional[Decimal]:
        """Расчёт веса в кг для агрегации"""
        unit = (material.unit or '').lower().strip()
        qty = material.quantity
        
        if unit in ('кг', 'kg'):
            return qty
        elif unit in ('г', 'гр', 'g', 'gr'):
            return qty / Decimal('1000')
        elif unit in ('л', 'литр', 'l', 'liter'):
            return qty
        elif unit in ('мл', 'ml'):
            return qty / Decimal('1000')
        else:
            return None


class BOMStorage:
    """Сохранение результатов BOM в базу данных с версионированием"""
    
    def __init__(self, conn):
        self.conn = conn
        self.cur = conn.cursor(cursor_factory=RealDictCursor)
        self._calculation_id = None
        self._prev_bom = {}  # (product_key, material_key) -> quantity
        
    def create_tables(self):
        """Создание таблиц для хранения BOM"""
        
        self.cur.execute("""
            -- Основная таблица развёрнутых спецификаций
            CREATE TABLE IF NOT EXISTS bom_expanded (
                id SERIAL PRIMARY KEY,
                calculation_id INTEGER,
                product_key VARCHAR(50) NOT NULL,
                product_name TEXT,
                material_key VARCHAR(50) NOT NULL,
                material_name TEXT,
                material_unit VARCHAR(20),
                quantity_per_unit NUMERIC(18,6),
                quantity_kg NUMERIC(18,6),
                type_id VARCHAR(50),
                type_name TEXT,
                type_level_1 TEXT,
                type_level_2 TEXT,
                type_level_3 TEXT,
                calculated_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_bom_product ON bom_expanded(product_key);
            CREATE INDEX IF NOT EXISTS idx_bom_material ON bom_expanded(material_key);
            CREATE INDEX IF NOT EXISTS idx_bom_calculated ON bom_expanded(calculated_at);
            CREATE INDEX IF NOT EXISTS idx_bom_type ON bom_expanded(type_id);
            CREATE INDEX IF NOT EXISTS idx_bom_calc_id ON bom_expanded(calculation_id);
            
            -- Таблица ошибок
            CREATE TABLE IF NOT EXISTS bom_errors (
                id SERIAL PRIMARY KEY,
                calculation_id INTEGER,
                product_key VARCHAR(50) NOT NULL,
                product_name TEXT,
                semifinished_key VARCHAR(50),
                semifinished_name TEXT,
                error_type VARCHAR(50),
                details TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_bom_errors_product ON bom_errors(product_key);
            CREATE INDEX IF NOT EXISTS idx_bom_errors_created ON bom_errors(created_at);
            CREATE INDEX IF NOT EXISTS idx_bom_errors_calc_id ON bom_errors(calculation_id);
            
            -- Таблица истории расчётов
            CREATE TABLE IF NOT EXISTS bom_calculations (
                id SERIAL PRIMARY KEY,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                products_processed INTEGER,
                materials_total INTEGER,
                errors_total INTEGER,
                status VARCHAR(20),
                trigger VARCHAR(50) DEFAULT 'manual',
                changes_summary TEXT
            );
            
            -- Таблица изменений BOM между расчётами
            CREATE TABLE IF NOT EXISTS bom_changes (
                id SERIAL PRIMARY KEY,
                calculation_id INTEGER NOT NULL,
                change_type VARCHAR(20) NOT NULL,
                product_key VARCHAR(50),
                product_name TEXT,
                material_key VARCHAR(50),
                material_name TEXT,
                old_quantity NUMERIC(18,6),
                new_quantity NUMERIC(18,6),
                created_at TIMESTAMP DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_bom_changes_calc ON bom_changes(calculation_id);
            CREATE INDEX IF NOT EXISTS idx_bom_changes_type ON bom_changes(change_type);
        """)
        
        # Добавляем колонки если их нет (миграция со старой версии)
        for col, col_type in [
            ('calculation_id', 'INTEGER'),
            ('trigger', "VARCHAR(50) DEFAULT 'manual'"),
            ('changes_summary', 'TEXT'),
        ]:
            try:
                table = 'bom_expanded' if col == 'calculation_id' else 'bom_calculations'
                if col == 'calculation_id':
                    # Добавляем в bom_expanded и bom_errors
                    self.cur.execute(f"""
                        ALTER TABLE bom_expanded ADD COLUMN IF NOT EXISTS calculation_id INTEGER
                    """)
                    self.cur.execute(f"""
                        ALTER TABLE bom_errors ADD COLUMN IF NOT EXISTS calculation_id INTEGER
                    """)
                else:
                    self.cur.execute(f"""
                        ALTER TABLE bom_calculations ADD COLUMN IF NOT EXISTS {col} {col_type}
                    """)
            except Exception:
                pass
        
        self.conn.commit()
        logger.info("Таблицы BOM созданы/обновлены")
        
    def start_calculation(self, trigger: str = 'manual'):
        """Начало нового расчёта: сохраняем предыдущие данные для diff"""
        
        # Загружаем предыдущий расчёт для сравнения
        self._prev_bom = {}
        try:
            self.cur.execute("""
                SELECT product_key, material_key, quantity_per_unit
                FROM bom_expanded
                WHERE calculation_id = (SELECT MAX(id) FROM bom_calculations WHERE status = 'completed')
            """)
            for row in self.cur.fetchall():
                key = (row['product_key'], row['material_key'])
                self._prev_bom[key] = Decimal(str(row['quantity_per_unit']))
        except Exception as e:
            logger.warning(f"Не удалось загрузить предыдущий BOM для сравнения: {e}")
        
        logger.info(f"  Предыдущий BOM: {len(self._prev_bom)} записей для сравнения")
        
        # Создаём запись расчёта
        self.cur.execute("""
            INSERT INTO bom_calculations (started_at, status, trigger)
            VALUES (NOW(), 'running', %s) RETURNING id
        """, (trigger,))
        self._calculation_id = self.cur.fetchone()['id']
        self.conn.commit()
        
        logger.info(f"  Расчёт #{self._calculation_id} (trigger: {trigger})")
        
    def save_result(self, result: BOMResult, exploder: BOMExploder, calc_time: datetime):
        """Сохранение результата развёртки одного продукта"""
        
        for mat in result.materials.values():
            hierarchy = mat.type_hierarchy
            level_1 = hierarchy[0] if len(hierarchy) > 0 else None
            level_2 = hierarchy[1] if len(hierarchy) > 1 else None
            level_3 = hierarchy[2] if len(hierarchy) > 2 else None
            
            if len(hierarchy) == 2:
                level_1 = hierarchy[0]
                level_2 = None
                level_3 = hierarchy[1]
            elif len(hierarchy) == 1:
                level_1 = None
                level_2 = None
                level_3 = hierarchy[0]
                
            qty_kg = exploder.calculate_kg(mat)
            
            self.cur.execute("""
                INSERT INTO bom_expanded 
                (calculation_id, product_key, product_name, material_key, material_name, material_unit,
                 quantity_per_unit, quantity_kg, type_id, type_name,
                 type_level_1, type_level_2, type_level_3, calculated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                self._calculation_id,
                result.product_key, result.product_name,
                mat.nomenclature_key, mat.name, mat.unit,
                float(mat.quantity), float(qty_kg) if qty_kg else None,
                mat.type_id, mat.type_name,
                level_1, level_2, level_3,
                calc_time
            ))
            
        for err in result.errors:
            self.cur.execute("""
                INSERT INTO bom_errors
                (calculation_id, product_key, product_name, semifinished_key, semifinished_name, 
                 error_type, details, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                self._calculation_id,
                err.product_key, err.product_name,
                err.semifinished_key, err.semifinished_name,
                err.error_type, err.details, calc_time
            ))
            
        self.conn.commit()

    def finalize(self, results: List[BOMResult], products_count: int, 
                 materials_count: int, errors_count: int):
        """Завершение расчёта: обновление статуса, вычисление diff"""
        
        # Обновляем запись расчёта
        self.cur.execute("""
            UPDATE bom_calculations 
            SET finished_at = NOW(), 
                products_processed = %s, 
                materials_total = %s, 
                errors_total = %s, 
                status = 'completed'
            WHERE id = %s
        """, (products_count, materials_count, errors_count, self._calculation_id))
        
        # Вычисляем изменения относительно предыдущего расчёта
        current_bom = {}
        product_names = {}
        material_names = {}
        
        for result in results:
            product_names[result.product_key] = result.product_name
            for mat_key, mat in result.materials.items():
                key = (result.product_key, mat_key)
                current_bom[key] = mat.quantity
                material_names[mat_key] = mat.name
        
        changes = []
        
        # Новые и изменённые
        for key, new_qty in current_bom.items():
            product_key, mat_key = key
            if key not in self._prev_bom:
                changes.append(('added', product_key, product_names.get(product_key, ''),
                               mat_key, material_names.get(mat_key, ''), None, new_qty))
            else:
                old_qty = self._prev_bom[key]
                if abs(float(old_qty) - float(new_qty)) > 0.000001:
                    changes.append(('quantity_changed', product_key, product_names.get(product_key, ''),
                                   mat_key, material_names.get(mat_key, ''), old_qty, new_qty))
        
        # Удалённые
        for key, old_qty in self._prev_bom.items():
            if key not in current_bom:
                product_key, mat_key = key
                changes.append(('removed', product_key, product_names.get(product_key, ''),
                               mat_key, material_names.get(mat_key, ''), old_qty, None))
        
        # Сохраняем изменения
        for change in changes:
            change_type, prod_key, prod_name, mat_key, mat_name, old_q, new_q = change
            self.cur.execute("""
                INSERT INTO bom_changes (calculation_id, change_type, product_key, product_name,
                    material_key, material_name, old_quantity, new_quantity)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (self._calculation_id, change_type, prod_key, prod_name,
                  mat_key, mat_name,
                  float(old_q) if old_q is not None else None,
                  float(new_q) if new_q is not None else None))
        
        # Обновляем summary
        added = sum(1 for c in changes if c[0] == 'added')
        removed = sum(1 for c in changes if c[0] == 'removed')
        changed = sum(1 for c in changes if c[0] == 'quantity_changed')
        summary = f"Изменений: {len(changes)} (добавлено: {added}, удалено: {removed}, изменено кол-во: {changed})"
        
        self.cur.execute("UPDATE bom_calculations SET changes_summary = %s WHERE id = %s",
                        (summary, self._calculation_id))
        
        self.conn.commit()
        
        logger.info(f"  {summary}")
        
        return len(changes)


def run_bom_explosion(trigger='manual'):
    """Основная функция запуска расчёта BOM"""
    
    start_time = datetime.now()
    logger.info(f"=== Запуск BOM Explosion: {start_time} ===")
    
    with BOMExploder() as exploder:
        # Инициализация
        exploder.init_caches()
        
        # Создаём/обновляем таблицы
        storage = BOMStorage(exploder.conn)
        storage.create_tables()
        storage.start_calculation(trigger=trigger)
        
        # Получаем список ГП
        products = exploder.get_finished_products()
        
        # Статистика
        total_materials = 0
        total_errors = 0
        all_results = []
        
        # Обрабатываем каждый продукт
        for i, product in enumerate(products, 1):
            logger.info(f"[{i}/{len(products)}] {product['name'][:50]}...")
            
            result = exploder.explode_bom(product['key'], product['name'])
            storage.save_result(result, exploder, start_time)
            all_results.append(result)
            
            total_materials += len(result.materials)
            total_errors += len(result.errors)
            
            if result.errors:
                for err in result.errors:
                    logger.warning(f"  ОШИБКА [{err.error_type}]: {err.semifinished_name}")
                    
        # Финализация: обновляем статус, вычисляем diff
        num_changes = storage.finalize(all_results, len(products), total_materials, total_errors)
        
    # Итоги
    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"=== BOM Explosion завершён ===")
    logger.info(f"  Время: {duration}")
    logger.info(f"  Продуктов: {len(products)}")
    logger.info(f"  Материалов: {total_materials}")
    logger.info(f"  Ошибок: {total_errors}")
    logger.info(f"  Изменений vs предыдущий: {num_changes}")
    

def get_bom_report(product_key: str) -> str:
    """
    Формирование отчёта BOM для одного продукта (из последнего расчёта)
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    # Получаем данные из последнего расчёта
    cur.execute("""
        SELECT product_name, material_name, material_unit, quantity_per_unit, quantity_kg,
               type_level_1, type_level_2, type_level_3
        FROM bom_expanded
        WHERE product_key = %s
          AND calculation_id = (SELECT MAX(id) FROM bom_calculations WHERE status = 'completed')
        ORDER BY type_level_1 NULLS LAST, type_level_2 NULLS LAST, type_level_3, material_name
    """, (product_key,))
    
    rows = cur.fetchall()
    
    if not rows:
        return f"Нет данных BOM для продукта {product_key}"
        
    product_name = rows[0]['product_name']
    
    # Группировка и форматирование
    lines = [f"БОМ на 1 единицу: {product_name}", "=" * 60]
    
    current_l1 = None
    current_l2 = None
    current_l3 = None
    
    subtotal_l1 = Decimal('0')
    subtotal_l2 = Decimal('0')
    subtotal_l3 = Decimal('0')
    grand_total = Decimal('0')
    
    def format_qty(qty):
        if qty is None:
            return ""
        return f"{float(qty):.4f}".rstrip('0').rstrip('.')
    
    for row in rows:
        l1 = row['type_level_1'] or "Прочее"
        l2 = row['type_level_2']
        l3 = row['type_level_3'] or "Без вида"
        
        if l1 != current_l1:
            if current_l1 is not None and subtotal_l1 > 0:
                lines.append(f"    Итого {current_l1}: {format_qty(subtotal_l1)} кг")
                lines.append("")
            current_l1 = l1
            current_l2 = None
            current_l3 = None
            subtotal_l1 = Decimal('0')
            lines.append(f"\n{l1}")
            
        if l2 != current_l2:
            if current_l2 is not None and subtotal_l2 > 0:
                lines.append(f"        Итого {current_l2}: {format_qty(subtotal_l2)} кг")
            current_l2 = l2
            current_l3 = None
            subtotal_l2 = Decimal('0')
            if l2:
                lines.append(f"    {l2}")
                
        if l3 != current_l3:
            if current_l3 is not None and subtotal_l3 > 0:
                lines.append(f"            Подитог {current_l3}: {format_qty(subtotal_l3)} кг")
            current_l3 = l3
            subtotal_l3 = Decimal('0')
            indent = "        " if l2 else "    "
            lines.append(f"{indent}{l3}")
            
        qty = row['quantity_per_unit']
        unit = row['material_unit'] or 'шт'
        kg = row['quantity_kg']
        
        indent = "            " if l2 else "        "
        lines.append(f"{indent}- {row['material_name']}: {format_qty(qty)} {unit}")
        
        if kg:
            kg_decimal = Decimal(str(kg))
            subtotal_l3 += kg_decimal
            subtotal_l2 += kg_decimal
            subtotal_l1 += kg_decimal
            grand_total += kg_decimal
            
    if subtotal_l3 > 0:
        lines.append(f"            Подитог {current_l3}: {format_qty(subtotal_l3)} кг")
    if subtotal_l2 > 0 and current_l2:
        lines.append(f"        Итого {current_l2}: {format_qty(subtotal_l2)} кг")
    if subtotal_l1 > 0:
        lines.append(f"    Итого {current_l1}: {format_qty(subtotal_l1)} кг")
        
    lines.append("")
    lines.append("=" * 60)
    lines.append(f"ОБЩИЙ ВЕС: {format_qty(grand_total)} кг")
    
    # Ошибки
    cur.execute("""
        SELECT semifinished_name, error_type, details
        FROM bom_errors
        WHERE product_key = %s
          AND calculation_id = (SELECT MAX(id) FROM bom_calculations WHERE status = 'completed')
    """, (product_key,))
    
    errors = cur.fetchall()
    if errors:
        lines.append("")
        lines.append("⚠️ ОШИБКИ:")
        for err in errors:
            lines.append(f"  - [{err['error_type']}] {err['semifinished_name']}: {err['details']}")
    
    cur.close()
    conn.close()
    
    return "\n".join(lines)


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "report":
        if len(sys.argv) < 3:
            print("Использование: python bom_exploder.py report <product_key>")
            sys.exit(1)
        print(get_bom_report(sys.argv[2]))
    else:
        # Режим расчёта
        trigger = sys.argv[1] if len(sys.argv) > 1 else 'manual'
        run_bom_explosion(trigger=trigger)
