#!/usr/bin/env python3
"""
Синхронизация данных из 1С:КА 2.5 в PostgreSQL
- Закупки
- Продажи (реализация + корректировки)
- Справочник номенклатуры
- Справочник видов номенклатуры

Режимы запуска:
  python sync_1c_full.py              # полная синхронизация (по умолчанию)
  python sync_1c_full.py --full       # полная синхронизация
  python sync_1c_full.py --incremental # только новые документы
"""
import os
import sys
import argparse
import pathlib
from dotenv import load_dotenv
import requests
from requests.auth import HTTPBasicAuth
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta, date
import time

# Загружаем переменные окружения
env_path = pathlib.Path(__file__).parent / '.env'
load_dotenv(dotenv_path=env_path if env_path.exists() else None)

# ============================================================
# НАСТРОЙКИ
# ============================================================
CONFIG_1C = {
    "base_url": os.getenv("ODATA_BASE_URL", "http://185.126.95.33:81/NB_KA/odata/standard.odata"),
    "username": os.getenv("ODATA_USERNAME", "odata.user"),
    "password": os.getenv("ODATA_PASSWORD", ""),
}

CONFIG_PG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "knowledge_base"),
    "user": os.getenv("DB_USER", "knowledge"),
    "password": os.getenv("DB_PASSWORD", ""),
}

EMPTY_UUID = "00000000-0000-0000-0000-000000000000"


# ============================================================
# ИНКРЕМЕНТАЛЬНАЯ СИНХРОНИЗАЦИЯ
# ============================================================

def ensure_sync_status_table(conn):
    """Создаёт таблицу sync_status если её нет."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS sync_status (
                    id SERIAL PRIMARY KEY,
                    entity_type VARCHAR(100) UNIQUE NOT NULL,
                    last_sync_at TIMESTAMP,
                    records_synced INTEGER DEFAULT 0,
                    last_error TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            conn.commit()
    except Exception as e:
        print(f"Ошибка создания таблицы sync_status: {e}")

def ensure_catalog_tables(conn):
    """Создаёт таблицы для справочников если их нет."""
    try:
        with conn.cursor() as cur:
            # Подразделения
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_departments (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    code VARCHAR(50),
                    name VARCHAR(500),
                    parent_key VARCHAR(50),
                    owner_key VARCHAR(50),
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Должности
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_positions (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    code VARCHAR(50),
                    name VARCHAR(500),
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Сотрудники
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_employees (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    code VARCHAR(50),
                    name VARCHAR(500),
                    organization_key VARCHAR(50),
                    is_archived BOOLEAN DEFAULT FALSE,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Статьи ДДС
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_cash_flow_items (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    code VARCHAR(50),
                    name VARCHAR(500),
                    parent_key VARCHAR(50),
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Ресурсные спецификации
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_specifications (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    code VARCHAR(50),
                    name VARCHAR(500),
                    owner_key VARCHAR(50),
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            conn.commit()
            print("✅ Таблицы справочников готовы")
    except Exception as e:
        print(f"Ошибка создания таблиц справочников: {e}")

def get_last_sync_date(conn, entity_type: str) -> datetime:
    """Получает дату последней успешной синхронизации."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT last_sync_at FROM sync_status 
                WHERE entity_type = %s
            """, (entity_type,))
            row = cur.fetchone()
            if row and row[0]:
                return row[0]
    except Exception as e:
        print(f"Ошибка получения даты синхронизации: {e}")
    
    # По умолчанию — 7 дней назад
    return datetime.now() - timedelta(days=7)


def update_last_sync_date(conn, entity_type: str, records_count: int = 0):
    """Обновляет дату последней синхронизации."""
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO sync_status (entity_type, last_sync_at, records_synced, updated_at)
                VALUES (%s, NOW(), %s, NOW())
                ON CONFLICT (entity_type) 
                DO UPDATE SET last_sync_at = NOW(), records_synced = %s, updated_at = NOW()
            """, (entity_type, records_count, records_count))
            conn.commit()
    except Exception as e:
        print(f"Ошибка обновления даты синхронизации: {e}")

# ============================================================
# КЛАСС СИНХРОНИЗАЦИИ
# ============================================================

class Sync1C:
    def __init__(self):
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(
            CONFIG_1C["username"],
            CONFIG_1C["password"]
        )
        self.session.headers.update({
    'Accept': 'application/json'
})
        self.base_url = CONFIG_1C["base_url"]
        
        # Кэши
        self.contractors_cache = {}
        self.nomenclature_cache = {}
        self.nomenclature_types_cache = {}
        self.consignees_cache = {}
    
    def test_connection(self):
        """Проверка подключения к 1С"""
        try:
            r = self.session.get(
                f"{self.base_url}/Catalog_Контрагенты?$top=1&$format=json",
                timeout=30
            )
            return r.status_code == 200
        except Exception as e:
            print(f"Ошибка подключения: {e}")
            return False
    
    def get_all_documents(self, entity_name, filter_posted=True, batch_size=500):
        """Загрузка документов порциями с поиском проблемных документов"""
        from urllib.parse import quote
        
        encoded_entity = quote(entity_name, safe='_')
        url = f"{self.base_url}/{encoded_entity}"
        all_docs = []
        skip = 0
        problem_docs = []
        skip_positions = set()  # Позиции которые нужно пропустить
        
        def try_load(skip_val, top_val):
            """Попытка загрузить порцию"""
            params = {
                "$format": "json",
                "$top": str(top_val),
                "$skip": str(skip_val)
            }
            if filter_posted:
                params["$filter"] = "Posted eq true"
            
            try:
                r = self.session.get(url, params=params, timeout=120)
                if r.status_code == 200:
                    return r.json().get('value', [])
                return None
            except:
                return None
        
        def find_problem_doc(start_skip, batch):
            """Бинарный поиск проблемного документа"""
            print(f"  Поиск проблемного документа в диапазоне {start_skip}-{start_skip + batch}...")
            
            # Пробуем по одному когда диапазон маленький
            if batch <= 10:
                for i in range(batch):
                    pos = start_skip + i
                    if pos in skip_positions:
                        continue
                        
                    docs = try_load(pos, 1)
                    if docs is None:
                        # Получаем инфо о соседних документах
                        before = try_load(pos - 1, 1)
                        after = try_load(pos + 1, 1)
                        
                        info = f"Позиция: {pos}"
                        if before:
                            info += f"\n        Документ ДО: №{before[0].get('Number', '?').strip()} от {before[0].get('Date', '?')[:10]} (Ref_Key: {before[0].get('Ref_Key', '?')})"
                        if after:
                            info += f"\n        Документ ПОСЛЕ: №{after[0].get('Number', '?').strip()} от {after[0].get('Date', '?')[:10]} (Ref_Key: {after[0].get('Ref_Key', '?')})"
                        
                        print(f"  >>> ПРОБЛЕМНЫЙ ДОКУМЕНТ на позиции {pos}")
                        if before:
                            print(f"      Документ ДО: №{before[0].get('Number', '?').strip()} от {before[0].get('Date', '?')[:10]}")
                        if after:
                            print(f"      Документ ПОСЛЕ: №{after[0].get('Number', '?').strip()} от {after[0].get('Date', '?')[:10]}")
                        
                        problem_docs.append(info)
                        skip_positions.add(pos)
                    elif docs:
                        all_docs.extend(docs)
                return start_skip + batch
            
            # Бинарный поиск - делим пополам
            mid = batch // 2
            
            docs = try_load(start_skip, mid)
            if docs is None:
                find_problem_doc(start_skip, mid)
            elif docs:
                all_docs.extend(docs)
                print(f"  Загружено {len(all_docs)} записей...")
            
            docs = try_load(start_skip + mid, batch - mid)
            if docs is None:
                find_problem_doc(start_skip + mid, batch - mid)
            elif docs:
                all_docs.extend(docs)
                print(f"  Загружено {len(all_docs)} записей...")
            
            return start_skip + batch
        
        while True:
            docs = try_load(skip, batch_size)
            
            if docs is None:
                # Ошибка — ищем проблемный документ и пропускаем
                find_problem_doc(skip, batch_size)
                skip += batch_size  # Переходим к следующей порции
                time.sleep(0.5)
                continue
            
            if not docs:
                break
            
            all_docs.extend(docs)
            print(f"  Загружено {len(all_docs)} записей...")
            
            if len(docs) < batch_size:
                break
            
            skip += batch_size
            time.sleep(0.3)
        
        if problem_docs:
            print(f"\n  !!! НАЙДЕНЫ ПРОБЛЕМНЫЕ ДОКУМЕНТЫ ({len(problem_docs)}):")
            for pd in problem_docs:
                print(f"      {pd}")
            print()
        
        return all_docs

    def get_documents_since(self, entity_name: str, since_date: datetime, batch_size: int = 500):
        """Загрузка документов начиная с указанной даты (инкрементально)."""
        from urllib.parse import quote
        
        # Кодируем кириллицу в URL
        encoded_entity = quote(entity_name, safe='_')
        all_docs = []
        skip = 0
        
        # Формат даты для OData
        date_str = since_date.strftime("%Y-%m-%dT%H:%M:%S")
        
        print(f"    Инкрементально с {since_date.strftime('%d.%m.%Y %H:%M')}")
        
        while True:
            # Формируем URL с параметрами напрямую (без кодирования $)
            url = (
                f"{self.base_url}/{encoded_entity}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20gt%20datetime'{date_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20asc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    print(f"    Ошибка HTTP {r.status_code}")
                    break
                
                data = r.json()
                if "odata.error" in data:
                    error_msg = data['odata.error'].get('message', {}).get('value', 'Unknown')
                    print(f"    Ошибка OData: {error_msg}")
                    break
                
                batch = data.get('value', [])
                if not batch:
                    break
                
                all_docs.extend(batch)
                
                if len(batch) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.3)
                
            except Exception as e:
                print(f"    Ошибка загрузки: {e}")
                break
        
        print(f"    Найдено: {len(all_docs)} новых документов")
        return all_docs

    def get_catalog_items(self, catalog_name: str):
        """Загружает все элементы справочника."""
        from urllib.parse import quote
        
        encoded_catalog = quote(catalog_name, safe='_')
        all_items = []
        skip = 0
        batch_size = 500
        
        print(f"  Загрузка {catalog_name}...")
        
        while True:
            url = (
                f"{self.base_url}/{encoded_catalog}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    print(f"    Ошибка HTTP {r.status_code}")
                    break
                
                data = r.json()
                if "odata.error" in data:
                    print(f"    Ошибка OData: {data['odata.error'].get('message', {}).get('value', '')}")
                    break
                
                batch = data.get('value', [])
                if not batch:
                    break
                
                all_items.extend(batch)
                
                if len(batch) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
                
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Загружено: {len(all_items)} записей")
        return all_items

    def sync_departments(self, conn):
        """Синхронизация подразделений."""
        print("\n[Подразделения]")
        items = self.get_catalog_items("Catalog_ПодразделенияОрганизаций")
        
        if not items:
            return 0
        
        count = 0
        with conn.cursor() as cur:
            for item in items:
                try:
                    cur.execute("""
                        INSERT INTO c1_departments (ref_key, code, name, parent_key, owner_key, is_deleted, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (ref_key) DO UPDATE SET
                            code = EXCLUDED.code,
                            name = EXCLUDED.name,
                            parent_key = EXCLUDED.parent_key,
                            owner_key = EXCLUDED.owner_key,
                            is_deleted = EXCLUDED.is_deleted,
                            updated_at = NOW()
                    """, (
                        item.get('Ref_Key'),
                        item.get('Code', ''),
                        item.get('Description', ''),
                        item.get('Parent_Key') if item.get('Parent_Key') != EMPTY_UUID else None,
                        item.get('Owner_Key') if item.get('Owner_Key') != EMPTY_UUID else None,
                        item.get('DeletionMark', False)
                    ))
                    count += 1
                except Exception as e:
                    print(f"    Ошибка записи: {e}")
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {count} подразделений")
        return count

    def sync_positions(self, conn):
        """Синхронизация должностей."""
        print("\n[Должности]")
        items = self.get_catalog_items("Catalog_Должности")
        
        if not items:
            return 0
        
        count = 0
        with conn.cursor() as cur:
            for item in items:
                try:
                    cur.execute("""
                        INSERT INTO c1_positions (ref_key, code, name, is_deleted, updated_at)
                        VALUES (%s, %s, %s, %s, NOW())
                        ON CONFLICT (ref_key) DO UPDATE SET
                            code = EXCLUDED.code,
                            name = EXCLUDED.name,
                            is_deleted = EXCLUDED.is_deleted,
                            updated_at = NOW()
                    """, (
                        item.get('Ref_Key'),
                        item.get('Code', ''),
                        item.get('Description', ''),
                        item.get('DeletionMark', False)
                    ))
                    count += 1
                except Exception as e:
                    print(f"    Ошибка записи: {e}")
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {count} должностей")
        return count

    def sync_employees(self, conn):
        """Синхронизация сотрудников."""
        print("\n[Сотрудники]")
        items = self.get_catalog_items("Catalog_Сотрудники")
        
        if not items:
            return 0
        
        count = 0
        with conn.cursor() as cur:
            for item in items:
                try:
                    cur.execute("""
                        INSERT INTO c1_employees (ref_key, code, name, organization_key, is_archived, is_deleted, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (ref_key) DO UPDATE SET
                            code = EXCLUDED.code,
                            name = EXCLUDED.name,
                            organization_key = EXCLUDED.organization_key,
                            is_archived = EXCLUDED.is_archived,
                            is_deleted = EXCLUDED.is_deleted,
                            updated_at = NOW()
                    """, (
                        item.get('Ref_Key'),
                        item.get('Code', ''),
                        item.get('Description', ''),
                        item.get('ГоловнаяОрганизация_Key') if item.get('ГоловнаяОрганизация_Key') != EMPTY_UUID else None,
                        item.get('ВАрхиве', False),
                        item.get('DeletionMark', False)
                    ))
                    count += 1
                except Exception as e:
                    print(f"    Ошибка записи: {e}")
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {count} сотрудников")
        return count

    def sync_cash_flow_items(self, conn):
        """Синхронизация статей ДДС."""
        print("\n[Статьи ДДС]")
        items = self.get_catalog_items("Catalog_СтатьиДвиженияДенежныхСредств")
        
        if not items:
            return 0
        
        count = 0
        with conn.cursor() as cur:
            for item in items:
                try:
                    cur.execute("""
                        INSERT INTO c1_cash_flow_items (ref_key, code, name, parent_key, is_deleted, updated_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (ref_key) DO UPDATE SET
                            code = EXCLUDED.code,
                            name = EXCLUDED.name,
                            parent_key = EXCLUDED.parent_key,
                            is_deleted = EXCLUDED.is_deleted,
                            updated_at = NOW()
                    """, (
                        item.get('Ref_Key'),
                        item.get('Code', ''),
                        item.get('Description', ''),
                        item.get('Parent_Key') if item.get('Parent_Key') != EMPTY_UUID else None,
                        item.get('DeletionMark', False)
                    ))
                    count += 1
                except Exception as e:
                    print(f"    Ошибка записи: {e}")
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {count} статей ДДС")
        return count

    def sync_specifications(self, conn):
        """Синхронизация ресурсных спецификаций."""
        print("\n[Ресурсные спецификации]")
        items = self.get_catalog_items("Catalog_РесурсныеСпецификации")
        
        if not items:
            return 0
        
        count = 0
        with conn.cursor() as cur:
            for item in items:
                try:
                    cur.execute("""
                        INSERT INTO c1_specifications (ref_key, code, name, owner_key, is_deleted, updated_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (ref_key) DO UPDATE SET
                            code = EXCLUDED.code,
                            name = EXCLUDED.name,
                            owner_key = EXCLUDED.owner_key,
                            is_deleted = EXCLUDED.is_deleted,
                            updated_at = NOW()
                    """, (
                        item.get('Ref_Key'),
                        item.get('Code', ''),
                        item.get('Description', ''),
                        item.get('Owner_Key') if item.get('Owner_Key') != EMPTY_UUID else None,
                        item.get('DeletionMark', False)
                    ))
                    count += 1
                except Exception as e:
                    print(f"    Ошибка записи: {e}")
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {count} спецификаций")
        return count

    def sync_all_catalogs(self, conn):
        """Синхронизация всех справочников."""
        print("\n" + "=" * 60)
        print("СПРАВОЧНИКИ (новые)")
        print("=" * 60)
        
        ensure_catalog_tables(conn)
        
        self.sync_departments(conn)
        self.sync_positions(conn)
        self.sync_employees(conn)
        self.sync_cash_flow_items(conn)
        self.sync_specifications(conn)
  
    def get_catalog(self, catalog_name, batch_size=1000):
        """Загрузка справочника"""
        from urllib.parse import quote
        
        encoded_catalog = quote(catalog_name, safe='_')
        
        params = {
            "$format": "json",
            "$top": str(batch_size)
        }
        url = f"{self.base_url}/{encoded_catalog}"
        all_items = []
        skip = 0
        
        while True:
            try:
                current_params = params.copy()
                if skip > 0:
                    current_params["$skip"] = str(skip)
                
                r = self.session.get(url, params=current_params, timeout=120)
                if r.status_code != 200:
                    break
                
                data = r.json()
                items = data.get('value', [])
                
                if not items:
                    break
                
                all_items.extend(items)
                
                if len(items) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
                
            except Exception as e:
                print(f"  Ошибка загрузки {catalog_name}: {e}")
                break
        
        return all_items
    
    def get_name_by_key(self, cache, catalog_name, key, name_field='Description'):
        """Получить название по ключу с кэшированием"""
        from urllib.parse import quote
        
        if not key or key == EMPTY_UUID:
            return None
        
        if key in cache:
            return cache[key]
        
        try:
            encoded_catalog = quote(catalog_name, safe='_')
            r = self.session.get(
                f"{self.base_url}/{encoded_catalog}(guid'{key}')?$format=json",
                timeout=30
            )
            if r.status_code == 200:
                name = r.json().get(name_field, '')
                cache[key] = name
                return name
        except:
            pass
        return None
    
    # ========== СПРАВОЧНИКИ ==========
    
    def sync_nomenclature_types(self, conn):
        """Синхронизация видов номенклатуры"""
        print("\n[ВИДЫ НОМЕНКЛАТУРЫ]")
        items = self.get_catalog("Catalog_ВидыНоменклатуры")
        print(f"  Получено {len(items)} видов")
        
        if not items:
            return
        
        cur = conn.cursor()
        cur.execute("DELETE FROM nomenclature_types")
        
        values = []
        for item in items:
            ref_key = item.get('Ref_Key')
            if not ref_key or ref_key == EMPTY_UUID:
                continue
            
            parent_key = item.get('Parent_Key')
            if parent_key == EMPTY_UUID:
                parent_key = None
            
            values.append((
                ref_key,
                parent_key,
                item.get('Description', ''),
                item.get('IsFolder', False),
            ))
        
        if values:
            execute_values(
                cur,
                """INSERT INTO nomenclature_types (id, parent_id, name, is_folder)
                   VALUES %s ON CONFLICT (id) DO UPDATE SET
                   parent_id=EXCLUDED.parent_id, name=EXCLUDED.name, is_folder=EXCLUDED.is_folder""",
                values
            )
        
        conn.commit()
        cur.close()
        print(f"  Сохранено {len(values)} видов номенклатуры")
    
    def sync_nomenclature(self, conn):
        """Синхронизация номенклатуры"""
        print("\n[НОМЕНКЛАТУРА]")
        items = self.get_catalog("Catalog_Номенклатура")
        print(f"  Получено {len(items)} позиций")
        
        if not items:
            return
        
        cur = conn.cursor()
        cur.execute("DELETE FROM nomenclature")
        
        values = []
        for item in items:
            ref_key = item.get('Ref_Key')
            if not ref_key or ref_key == EMPTY_UUID:
                continue
            
            parent_key = item.get('Parent_Key')
            if parent_key == EMPTY_UUID:
                parent_key = None
            
            type_key = item.get('ВидНоменклатуры_Key')
            if type_key == EMPTY_UUID:
                type_key = None
            
            unit_key = item.get('ЕдиницаИзмерения_Key')
            if unit_key == EMPTY_UUID:
                unit_key = None
            
            # Вес
            weight = None
            if item.get('ВесЧислитель') and item.get('ВесЗнаменатель'):
                try:
                    num = float(item.get('ВесЧислитель', 0) or 0)
                    den = float(item.get('ВесЗнаменатель', 1) or 1)
                    if den > 0:
                        weight = num / den
                except:
                    pass
            
            values.append((
                ref_key,
                parent_key,
                item.get('IsFolder', False),
                item.get('Code', ''),
                item.get('Description', ''),
                item.get('НаименованиеПолное', ''),
                item.get('Артикул', ''),
                type_key,
                unit_key,
                '',  # unit_name - заполним позже
                weight,
                '',  # weight_unit
            ))
            
            # Кэшируем
            self.nomenclature_cache[ref_key] = item.get('Description', '')
        
        if values:
            execute_values(
                cur,
                """INSERT INTO nomenclature 
                   (id, parent_id, is_folder, code, name, full_name, article, 
                    type_id, unit_id, unit_name, weight, weight_unit)
                   VALUES %s ON CONFLICT (id) DO UPDATE SET
                   parent_id=EXCLUDED.parent_id, name=EXCLUDED.name, 
                   full_name=EXCLUDED.full_name, article=EXCLUDED.article,
                   type_id=EXCLUDED.type_id, weight=EXCLUDED.weight""",
                values
            )
        
        conn.commit()
        cur.close()
        print(f"  Сохранено {len(values)} позиций номенклатуры")
    
    def sync_clients(self, conn):
        """Синхронизация клиентов (партнёров)"""
        print("\n[КЛИЕНТЫ]")
        items = self.get_catalog("Catalog_Партнеры")
        print(f"  Получено {len(items)} партнёров")
        
        if not items:
            return
        
        cur = conn.cursor()
        cur.execute("DELETE FROM clients")
        
        values = []
        for item in items:
            ref_key = item.get('Ref_Key')
            if not ref_key or ref_key == EMPTY_UUID:
                continue
            if item.get('IsFolder', False):
                continue
            
            values.append((
                ref_key,
                item.get('Description', '') or item.get('НаименованиеПолное', ''),
                item.get('ИНН', ''),
            ))
            
            self.contractors_cache[ref_key] = item.get('Description', '')
        
        if values:
            execute_values(
                cur,
                """INSERT INTO clients (id, name, inn)
                   VALUES %s ON CONFLICT (id) DO UPDATE SET
                   name=EXCLUDED.name, inn=EXCLUDED.inn""",
                values
            )
        
        conn.commit()
        cur.close()
        print(f"  Сохранено {len(values)} клиентов")
    
    # ========== ПРОДАЖИ ==========
    
    def sync_sales(self, conn, date_from, date_to):
        """Синхронизация продаж с локальной фильтрацией по дате"""
        from urllib.parse import quote
        
        print("\n[ПРОДАЖИ]")
        
        # Загружаем реализации порциями
        print("  Загрузка реализаций...")
        
        encoded_entity = quote("Document_РеализацияТоваровУслуг", safe='_')
        url = f"{self.base_url}/{encoded_entity}"
        
        sales_docs = []
        skip = 0
        batch_size = 100
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        while consecutive_errors < max_consecutive_errors:
            params = {
                "$format": "json",
                "$filter": "Posted eq true",
                "$top": str(batch_size),
                "$skip": str(skip)
            }
            
            try:
                r = self.session.get(url, params=params, timeout=120)
                
                if r.status_code == 500:
                    print(f"  Ошибка 500 на skip={skip}, пропускаем порцию...")
                    skip += batch_size
                    consecutive_errors += 1
                    time.sleep(0.5)
                    continue
                
                if r.status_code != 200:
                    print(f"  Ошибка HTTP {r.status_code}")
                    break
                
                docs = r.json().get('value', [])
                
                if not docs:
                    break
                
                # Фильтруем по дате локально
                for doc in docs:
                    doc_date_str = doc.get('Date', '')[:10]
                    try:
                        doc_date = datetime.strptime(doc_date_str, "%Y-%m-%d").date()
                        if date_from <= doc_date <= date_to:
                            sales_docs.append(doc)
                    except:
                        continue
                
                print(f"  Обработано {skip + len(docs)}, подходящих: {len(sales_docs)}...")
                
                consecutive_errors = 0
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
                
            except Exception as e:
                print(f"  Ошибка на skip={skip}: {e}, пропускаем...")
                skip += batch_size
                consecutive_errors += 1
                time.sleep(0.5)
        
        print(f"  Всего реализаций за период: {len(sales_docs)}")
        
        # Загружаем корректировки
        print("  Загрузка корректировок...")
        
        encoded_corr = quote("Document_КорректировкаРеализации", safe='_')
        url_corr = f"{self.base_url}/{encoded_corr}"
        
        corrections = []
        skip = 0
        consecutive_errors = 0
        
        while consecutive_errors < max_consecutive_errors:
            params = {
                "$format": "json",
                "$filter": "Posted eq true",
                "$top": str(batch_size),
                "$skip": str(skip)
            }
            
            try:
                r = self.session.get(url_corr, params=params, timeout=120)
                
                if r.status_code == 500:
                    skip += batch_size
                    consecutive_errors += 1
                    continue
                
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                
                if not docs:
                    break
                
                for doc in docs:
                    doc_date_str = doc.get('Date', '')[:10]
                    try:
                        doc_date = datetime.strptime(doc_date_str, "%Y-%m-%d").date()
                        if date_from <= doc_date <= date_to:
                            corrections.append(doc)
                    except:
                        continue
                
                consecutive_errors = 0
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
                
            except Exception as e:
                skip += batch_size
                consecutive_errors += 1
        
        print(f"  Всего корректировок за период: {len(corrections)}")
        
        records = []
        
        # Обрабатываем реализации
        for doc in sales_docs:
            doc_date = doc.get('Date', '')[:10]
            doc_number = doc.get('Number', '').strip()
            doc_id = doc.get('Ref_Key')
            
            client_key = doc.get('Партнер_Key') or doc.get('Контрагент_Key')
            client_name = self.get_name_by_key(
                self.contractors_cache, "Catalog_Партнеры", client_key
            ) or self.get_name_by_key(
                self.contractors_cache, "Catalog_Контрагенты", client_key
            )
            
            consignee_key = doc.get('Грузополучатель_Key')
            consignee_name = None
            if consignee_key and consignee_key != EMPTY_UUID:
                consignee_name = self.get_name_by_key(
                    self.consignees_cache, "Catalog_Партнеры", consignee_key
                )
            
            pallets = doc.get('АгросервисИТ_КоличествоПаллетов', '0')
            try:
                pallets = float(pallets) if pallets else 0
            except:
                pallets = 0
            
            logistics_fact = doc.get('АгросервисИТ_ФактическаяСтоимостьТраспортныхРасходов', 0) or 0
            logistics_plan = doc.get('АгросервисИТ_ПлановаяСтоимостьТраспортныхРасходов', 0) or 0
            
            for item in doc.get('Товары', []):
                nom_key = item.get('Номенклатура_Key')
                if not nom_key or nom_key == EMPTY_UUID:
                    continue
                
                nom_name = self.get_name_by_key(
                    self.nomenclature_cache, "Catalog_Номенклатура", nom_key
                )
                
                quantity = float(item.get('Количество', 0) or 0)
                price = float(item.get('Цена', 0) or 0)
                sum_without_vat = float(item.get('Сумма', 0) or 0)
                sum_with_vat = float(item.get('СуммаСНДС', 0) or 0)
                
                if quantity == 0:
                    continue
                
                records.append({
                    'doc_type': 'Реализация',
                    'doc_date': doc_date,
                    'doc_number': doc_number,
                    'doc_id': doc_id,
                    'client_id': client_key if client_key != EMPTY_UUID else None,
                    'client_name': client_name,
                    'consignee_id': consignee_key if consignee_key and consignee_key != EMPTY_UUID else None,
                    'consignee_name': consignee_name,
                    'nomenclature_id': nom_key,
                    'nomenclature_name': nom_name,
                    'nomenclature_type': None,
                    'quantity': quantity,
                    'price': price,
                    'sum_without_vat': sum_without_vat,
                    'sum_with_vat': sum_with_vat,
                    'pallets_count': pallets,
                    'logistics_cost_fact': logistics_fact,
                    'logistics_cost_plan': logistics_plan,
                })
        
        # Обрабатываем корректировки
        for doc in corrections:
            doc_date = doc.get('Date', '')[:10]
            doc_number = doc.get('Number', '').strip()
            doc_id = doc.get('Ref_Key')
            
            client_key = doc.get('Партнер_Key') or doc.get('Контрагент_Key')
            client_name = self.get_name_by_key(
                self.contractors_cache, "Catalog_Партнеры", client_key
            )
            
            consignee_key = doc.get('Грузополучатель_Key')
            consignee_name = None
            if consignee_key and consignee_key != EMPTY_UUID:
                consignee_name = self.get_name_by_key(
                    self.consignees_cache, "Catalog_Партнеры", consignee_key
                )
            
            for item in doc.get('Расхождения', []):
                nom_key = item.get('Номенклатура_Key')
                if not nom_key or nom_key == EMPTY_UUID:
                    continue
                
                nom_name = self.get_name_by_key(
                    self.nomenclature_cache, "Catalog_Номенклатура", nom_key
                )
                
                quantity = float(item.get('Количество', 0) or 0)
                sum_without_vat = float(item.get('Сумма', 0) or 0)
                sum_with_vat = float(item.get('СуммаСНДС', 0) or 0)
                price = sum_without_vat / quantity if quantity != 0 else 0
                
                records.append({
                    'doc_type': 'Корректировка',
                    'doc_date': doc_date,
                    'doc_number': doc_number,
                    'doc_id': doc_id,
                    'client_id': client_key if client_key != EMPTY_UUID else None,
                    'client_name': client_name,
                    'consignee_id': consignee_key if consignee_key and consignee_key != EMPTY_UUID else None,
                    'consignee_name': consignee_name,
                    'nomenclature_id': nom_key,
                    'nomenclature_name': nom_name,
                    'nomenclature_type': None,
                    'quantity': quantity,
                    'price': round(price, 2),
                    'sum_without_vat': sum_without_vat,
                    'sum_with_vat': sum_with_vat,
                    'pallets_count': 0,
                    'logistics_cost_fact': 0,
                    'logistics_cost_plan': 0,
                })
        
        print(f"  Всего записей о продажах: {len(records)}")
        
        if records:
            self._save_sales(conn, records, date_from, date_to)
    
    def _save_sales(self, conn, records, date_from, date_to):
        """Сохранение продаж в PostgreSQL"""
        cur = conn.cursor()
        
        # Удаляем старые данные за период
        cur.execute(
            "DELETE FROM sales WHERE doc_date BETWEEN %s AND %s",
            (date_from, date_to)
        )
        
        values = [
            (
                r['doc_type'], r['doc_date'], r['doc_number'], r['doc_id'],
                r['client_id'], r['client_name'],
                r['consignee_id'], r['consignee_name'],
                r['nomenclature_id'], r['nomenclature_name'], r['nomenclature_type'],
                r['quantity'], r['price'], r['sum_without_vat'], r['sum_with_vat'],
                r['pallets_count'], r['logistics_cost_fact'], r['logistics_cost_plan']
            )
            for r in records
        ]
        
        execute_values(
            cur,
            """INSERT INTO sales 
               (doc_type, doc_date, doc_number, doc_id,
                client_id, client_name, consignee_id, consignee_name,
                nomenclature_id, nomenclature_name, nomenclature_type,
                quantity, price, sum_without_vat, sum_with_vat,
                pallets_count, logistics_cost_fact, logistics_cost_plan)
               VALUES %s""",
            values
        )
        
        conn.commit()
        cur.close()
        print(f"  Сохранено {len(records)} записей о продажах")
    
    # ========== ЗАКУПКИ ==========
    
    def sync_purchases(self, conn, date_from, date_to):
        """Синхронизация закупок (из старого скрипта)"""
        print("\n[ЗАКУПКИ]")
        
        docs = self.get_all_documents("Document_ПриобретениеТоваровУслуг")
        
        # Фильтруем по дате
        filtered = []
        for doc in docs:
            doc_date_str = doc.get('Date', '')[:10]
            try:
                doc_date = datetime.strptime(doc_date_str, "%Y-%m-%d").date()
                if date_from <= doc_date <= date_to:
                    filtered.append(doc)
            except:
                continue
        
        print(f"  После фильтрации: {len(filtered)} документов")
        
        records = []
        for doc in filtered:
            doc_date = doc.get('Date', '')[:10]
            doc_number = doc.get('Number', '').strip()
            
            contractor_key = doc.get('Контрагент_Key', '')
            contractor_name = self.get_name_by_key(
                self.contractors_cache, "Catalog_Контрагенты", contractor_key
            )
            
            for item in doc.get('Товары', []):
                nom_key = item.get('Номенклатура_Key', '')
                nom_name = self.get_name_by_key(
                    self.nomenclature_cache, "Catalog_Номенклатура", nom_key
                )
                
                quantity = float(item.get('Количество', 0) or 0)
                price = float(item.get('Цена', 0) or 0)
                summa = float(item.get('СуммаСНДС', 0) or item.get('Сумма', 0) or 0)
                
                if quantity > 0:
                    records.append({
                        'doc_date': doc_date,
                        'doc_number': doc_number,
                        'contractor_id': contractor_key if contractor_key != EMPTY_UUID else None,
                        'contractor_name': contractor_name,
                        'nomenclature_id': nom_key if nom_key != EMPTY_UUID else None,
                        'nomenclature_name': nom_name,
                        'quantity': round(quantity, 3),
                        'price': round(price, 2),
                        'sum_total': round(summa, 2),
                    })
        
        print(f"  Извлечено {len(records)} записей о закупках")
        
        if records:
            self._save_purchases(conn, records, date_from, date_to)
    
    def _save_purchases(self, conn, records, date_from, date_to):
        """Сохранение закупок в PostgreSQL"""
        cur = conn.cursor()
        
        cur.execute(
            "DELETE FROM purchase_prices WHERE doc_date BETWEEN %s AND %s",
            (date_from, date_to)
        )
        
        values = [
            (
                r['doc_date'], r['doc_number'], r['contractor_id'], r['contractor_name'],
                r['nomenclature_id'], r['nomenclature_name'], r['quantity'], r['price'], r['sum_total']
            )
            for r in records
        ]
        
        execute_values(
            cur,
            """INSERT INTO purchase_prices 
               (doc_date, doc_number, contractor_id, contractor_name, 
                nomenclature_id, nomenclature_name, quantity, price, sum_total)
               VALUES %s""",
            values
        )
        
        conn.commit()
        cur.close()
        print(f"  Сохранено {len(records)} записей о закупках")


# ============================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================

def main_full(sync, conn):
    """Полная синхронизация."""
    # Период синхронизации
    date_to = datetime.now().date()
    date_from = date_to - timedelta(days=365)
    print(f"\n[3] Период: {date_from} — {date_to}")
    
    # Синхронизация справочников (старые)
    print("\n" + "=" * 60)
    print("СПРАВОЧНИКИ (базовые)")
    print("=" * 60)
    
    sync.sync_nomenclature_types(conn)
    sync.sync_nomenclature(conn)
    sync.sync_clients(conn)
    
    # Синхронизация новых справочников
    sync.sync_all_catalogs(conn)
    
    # Синхронизация документов
    print("\n" + "=" * 60)
    print("ДОКУМЕНТЫ")
    print("=" * 60)
    
    sync.sync_purchases(conn, date_from, date_to)
    sync.sync_sales(conn, date_from, date_to)


def main_incremental(sync, conn):
    """Инкрементальная синхронизация (только новые документы)."""
    ensure_sync_status_table(conn)
    
    print("\n" + "=" * 60)
    print("ИНКРЕМЕНТАЛЬНАЯ СИНХРОНИЗАЦИЯ")
    print("=" * 60)
    
    # Закупки
    print("\n[Закупки]")
    last_sync = get_last_sync_date(conn, "purchases")
    docs = sync.get_documents_since("Document_ПриобретениеТоваровУслуг", last_sync)
    if docs:
        date_from = last_sync.date()
        date_to = datetime.now().date()
        # Используем существующий метод, но он загрузит все — это временное решение
        # В будущем можно оптимизировать, передавая уже загруженные docs
        sync.sync_purchases(conn, date_from, date_to)
        update_last_sync_date(conn, "purchases", len(docs))
    else:
        print("    Новых документов нет")
    
    # Продажи
    print("\n[Продажи]")
    last_sync = get_last_sync_date(conn, "sales")
    docs = sync.get_documents_since("Document_РеализацияТоваровУслуг", last_sync)
    if docs:
        date_from = last_sync.date()
        date_to = datetime.now().date()
        sync.sync_sales(conn, date_from, date_to)
        update_last_sync_date(conn, "sales", len(docs))
    else:
        print("    Новых документов нет")


def main():
    parser = argparse.ArgumentParser(description='Синхронизация 1С → PostgreSQL')
    parser.add_argument('--incremental', '-i', action='store_true', 
                        help='Инкрементальная синхронизация (только новые документы)')
    parser.add_argument('--full', '-f', action='store_true',
                        help='Полная синхронизация (все документы за год)')
    args = parser.parse_args()
    
    mode = "incremental" if args.incremental else "full"
    
    print("=" * 60)
    print("СИНХРОНИЗАЦИЯ 1С → PostgreSQL")
    print(f"Режим: {mode.upper()}")
    print("=" * 60)
    
    sync = Sync1C()
    
    # Проверка подключения к 1С
    print("\n[1] Проверка подключения к 1С...")
    if not sync.test_connection():
        print("ОШИБКА: Не удалось подключиться к 1С")
        return
    print("OK")
    
    # Подключение к PostgreSQL
    print("\n[2] Подключение к PostgreSQL...")
    try:
        conn = psycopg2.connect(**CONFIG_PG)
        print("OK")
    except Exception as e:
        print(f"ОШИБКА: {e}")
        return
    
    try:
        if args.incremental:
            main_incremental(sync, conn)
        else:
            main_full(sync, conn)
    finally:
        conn.close()
    
    print("\n" + "=" * 60)
    print("ГОТОВО!")
    print("=" * 60)


if __name__ == "__main__":
    main()
