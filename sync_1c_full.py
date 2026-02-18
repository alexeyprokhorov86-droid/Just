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
import re

def sanitize_string(value):
    """Очищает строку от битых символов."""
    if value is None:
        return None
    if not isinstance(value, str):
        return value
    
    # Убираем BOM
    value = value.replace('\ufeff', '')
    
    # Убираем нулевые байты
    value = value.replace('\x00', '')
    
    # Убираем управляющие символы (кроме \n, \r, \t)
    value = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', value)
    
    # Убираем невалидные суррогатные пары Unicode
    value = value.encode('utf-8', errors='surrogateescape').decode('utf-8', errors='replace')
    
    # Заменяем символ замены на пустую строку
    value = value.replace('\ufffd', '')
    
    return value.strip()


def sanitize_dict(data: dict) -> dict:
    """Рекурсивно очищает все строковые значения в словаре."""
    if not isinstance(data, dict):
        return data
    
    result = {}
    for key, value in data.items():
        if isinstance(value, str):
            result[key] = sanitize_string(value)
        elif isinstance(value, dict):
            result[key] = sanitize_dict(value)
        elif isinstance(value, list):
            result[key] = [sanitize_dict(item) if isinstance(item, dict) else 
                          sanitize_string(item) if isinstance(item, str) else item 
                          for item in value]
        else:
            result[key] = value
    return result

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

def ensure_production_tables(conn):
    """Создаёт таблицы для документов производства."""
    try:
        with conn.cursor() as cur:
            # Производство без заказа
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_production (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    department_key VARCHAR(50),
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Выходные изделия (табличная часть производства)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_production_items (
                    id SERIAL PRIMARY KEY,
                    production_key VARCHAR(50) NOT NULL,
                    line_number INTEGER,
                    nomenclature_key VARCHAR(50),
                    specification_key VARCHAR(50),
                    quantity NUMERIC(15,3),
                    price NUMERIC(15,2),
                    sum_total NUMERIC(15,2),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_production_items_prod_key 
                ON c1_production_items(production_key)
            """)
            
            # Списание затрат на выпуск
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_cost_allocation (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    department_key VARCHAR(50),
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Материалы списания затрат (табличная часть)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_cost_allocation_materials (
                    id SERIAL PRIMARY KEY,
                    doc_key VARCHAR(50) NOT NULL,
                    line_number INTEGER,
                    nomenclature_key VARCHAR(50),
                    quantity NUMERIC(15,3),
                    sum_total NUMERIC(15,2),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_cost_alloc_mat_doc_key 
                ON c1_cost_allocation_materials(doc_key)
            """)
            
            # Заказ материалов в производство
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_material_orders (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    department_key VARCHAR(50),
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Товары заказа материалов (табличная часть)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_material_order_items (
                    id SERIAL PRIMARY KEY,
                    order_key VARCHAR(50) NOT NULL,
                    line_number INTEGER,
                    nomenclature_key VARCHAR(50),
                    quantity NUMERIC(15,3),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_mat_order_items_order_key 
                ON c1_material_order_items(order_key)
            """)
            
            # Передача материалов в производство
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_material_transfers (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    department_key VARCHAR(50),
                    warehouse_key VARCHAR(50),
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Товары передачи материалов (табличная часть)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_material_transfer_items (
                    id SERIAL PRIMARY KEY,
                    transfer_key VARCHAR(50) NOT NULL,
                    line_number INTEGER,
                    nomenclature_key VARCHAR(50),
                    quantity NUMERIC(15,3),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_mat_transfer_items_key 
                ON c1_material_transfer_items(transfer_key)
            """)
            
            conn.commit()
            print("✅ Таблицы производства готовы")
    except Exception as e:
        print(f"Ошибка создания таблиц производства: {e}")

def ensure_warehouse_tables(conn):
    """Создаёт таблицы для складских документов."""
    try:
        with conn.cursor() as cur:
            # Пересчет товаров
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_inventory_count (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    warehouse_key VARCHAR(50),
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_inventory_count_items (
                    id SERIAL PRIMARY KEY,
                    doc_key VARCHAR(50) NOT NULL,
                    line_number INTEGER,
                    nomenclature_key VARCHAR(50),
                    quantity_fact NUMERIC(15,3),
                    quantity_account NUMERIC(15,3),
                    deviation NUMERIC(15,3),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_inv_count_items_key 
                ON c1_inventory_count_items(doc_key)
            """)
            
            # Оприходование излишков
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_surplus (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    warehouse_key VARCHAR(50),
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_surplus_items (
                    id SERIAL PRIMARY KEY,
                    doc_key VARCHAR(50) NOT NULL,
                    line_number INTEGER,
                    nomenclature_key VARCHAR(50),
                    quantity NUMERIC(15,3),
                    price NUMERIC(15,2),
                    sum_total NUMERIC(15,2),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_surplus_items_key 
                ON c1_surplus_items(doc_key)
            """)
            
            # Пересортица товаров
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_regrade (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    warehouse_key VARCHAR(50),
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_regrade_items (
                    id SERIAL PRIMARY KEY,
                    doc_key VARCHAR(50) NOT NULL,
                    line_number INTEGER,
                    nomenclature_from_key VARCHAR(50),
                    nomenclature_to_key VARCHAR(50),
                    quantity NUMERIC(15,3),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_regrade_items_key 
                ON c1_regrade_items(doc_key)
            """)
            
            # Списание недостач
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_shortage (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    warehouse_key VARCHAR(50),
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_shortage_items (
                    id SERIAL PRIMARY KEY,
                    doc_key VARCHAR(50) NOT NULL,
                    line_number INTEGER,
                    nomenclature_key VARCHAR(50),
                    quantity NUMERIC(15,3),
                    price NUMERIC(15,2),
                    sum_total NUMERIC(15,2),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_shortage_items_key 
                ON c1_shortage_items(doc_key)
            """)
            
            conn.commit()
            print("✅ Таблицы складских документов готовы")
    except Exception as e:
        print(f"Ошибка создания таблиц складских документов: {e}")

def ensure_finance_tables(conn):
    """Создаёт таблицы для финансовых документов и заказов."""
    try:
        with conn.cursor() as cur:
            # Списание безналичных ДС
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_bank_expenses (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    bank_account_key VARCHAR(50),
                    counterparty_key VARCHAR(50),
                    amount NUMERIC(15,2),
                    purpose TEXT,
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Заказы клиентов
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_customer_orders (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    partner_key VARCHAR(50),
                    warehouse_key VARCHAR(50),
                    amount NUMERIC(15,2),
                    status VARCHAR(100),
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_customer_order_items (
                    id SERIAL PRIMARY KEY,
                    order_key VARCHAR(50) NOT NULL,
                    line_number INTEGER,
                    nomenclature_key VARCHAR(50),
                    quantity NUMERIC(15,3),
                    price NUMERIC(15,2),
                    sum_total NUMERIC(15,2),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_cust_order_items_key 
                ON c1_customer_order_items(order_key)
            """)
            
            # Заказы поставщикам
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_supplier_orders (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    partner_key VARCHAR(50),
                    warehouse_key VARCHAR(50),
                    amount NUMERIC(15,2),
                    status VARCHAR(100),
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_supplier_order_items (
                    id SERIAL PRIMARY KEY,
                    order_key VARCHAR(50) NOT NULL,
                    line_number INTEGER,
                    nomenclature_key VARCHAR(50),
                    quantity NUMERIC(15,3),
                    price NUMERIC(15,2),
                    sum_total NUMERIC(15,2),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_supp_order_items_key 
                ON c1_supplier_order_items(order_key)
            """)
            
            # План производства
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_production_plan (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    department_key VARCHAR(50),
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_production_plan_items (
                    id SERIAL PRIMARY KEY,
                    plan_key VARCHAR(50) NOT NULL,
                    line_number INTEGER,
                    nomenclature_key VARCHAR(50),
                    quantity NUMERIC(15,3),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_prod_plan_items_key 
                ON c1_production_plan_items(plan_key)
            """)
            
            # План закупок
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_purchase_plan (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_purchase_plan_items (
                    id SERIAL PRIMARY KEY,
                    plan_key VARCHAR(50) NOT NULL,
                    line_number INTEGER,
                    nomenclature_key VARCHAR(50),
                    quantity NUMERIC(15,3),
                    price NUMERIC(15,2),
                    sum_total NUMERIC(15,2),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_purch_plan_items_key 
                ON c1_purchase_plan_items(plan_key)
            """)

            # План продаж
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_sales_plan (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    partner_key VARCHAR(50),
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_sales_plan_items (
                    id SERIAL PRIMARY KEY,
                    plan_key VARCHAR(50) NOT NULL,
                    line_number INTEGER,
                    nomenclature_key VARCHAR(50),
                    quantity NUMERIC(15,3),
                    price NUMERIC(15,2),
                    sum_total NUMERIC(15,2),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_sales_plan_items_key 
                ON c1_sales_plan_items(plan_key)
            """)
          
            # Внутреннее потребление
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_internal_consumption (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    department_key VARCHAR(50),
                    warehouse_key VARCHAR(50),
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_internal_consumption_items (
                    id SERIAL PRIMARY KEY,
                    doc_key VARCHAR(50) NOT NULL,
                    line_number INTEGER,
                    nomenclature_key VARCHAR(50),
                    quantity NUMERIC(15,3),
                    sum_total NUMERIC(15,2),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_int_cons_items_key 
                ON c1_internal_consumption_items(doc_key)
            """)
            
            # Взаимозачет задолженности
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_debt_offset (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    doc_number VARCHAR(50),
                    doc_date DATE,
                    posted BOOLEAN DEFAULT FALSE,
                    organization_key VARCHAR(50),
                    counterparty_key VARCHAR(50),
                    amount_debit NUMERIC(15,2),
                    amount_credit NUMERIC(15,2),
                    comment TEXT,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            conn.commit()
            print("✅ Таблицы финансовых документов готовы")
    except Exception as e:
        print(f"Ошибка создания таблиц финансовых документов: {e}")

def ensure_units_table(conn):
    """Создаёт таблицу для единиц измерения и обновляет c1_specifications."""
    try:
        with conn.cursor() as cur:
            # Таблица единиц измерения
            cur.execute("""
                CREATE TABLE IF NOT EXISTS c1_units (
                    id SERIAL PRIMARY KEY,
                    ref_key VARCHAR(50) UNIQUE NOT NULL,
                    code VARCHAR(50),
                    name VARCHAR(100),
                    full_name VARCHAR(500),
                    international_abbr VARCHAR(50),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Добавляем недостающие колонки в c1_specifications
            cur.execute("""
                ALTER TABLE c1_specifications 
                ADD COLUMN IF NOT EXISTS product_key VARCHAR(50),
                ADD COLUMN IF NOT EXISTS product_quantity NUMERIC(15,6),
                ADD COLUMN IF NOT EXISTS status VARCHAR(50),
                ADD COLUMN IF NOT EXISTS auto_select VARCHAR(50),
                ADD COLUMN IF NOT EXISTS has_nested BOOLEAN DEFAULT FALSE
            """)
            
            # Индексы для быстрого поиска
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_c1_spec_product_key 
                ON c1_specifications(product_key)
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_c1_spec_status 
                ON c1_specifications(status)
            """)
            
            conn.commit()
            print("✅ Таблицы единиц измерения и спецификаций обновлены")
    except Exception as e:
        print(f"Ошибка создания таблиц: {e}")
        conn.rollback()

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
                    batch = r.json().get('value', [])
                    return [sanitize_dict(doc) for doc in batch]
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
                # Очищаем данные от битых символов
                batch = [sanitize_dict(doc) for doc in batch]
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
                # Очищаем данные от битых символов
                batch = [sanitize_dict(doc) for doc in batch]
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

    def sync_units(self, conn):
        """Синхронизация единиц измерения."""
        print("\n[Единицы измерения]")
        items = self.get_catalog_items("Catalog_УпаковкиЕдиницыИзмерения")
        
        if not items:
            print("  Нет данных")
            return 0
        
        count = 0
        with conn.cursor() as cur:
            for item in items:
                try:
                    ref_key = item.get('Ref_Key')
                    if not ref_key or ref_key == EMPTY_UUID:
                        continue
                    
                    cur.execute("""
                        INSERT INTO c1_units (ref_key, code, name, full_name, international_abbr, updated_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (ref_key) DO UPDATE SET
                            code = EXCLUDED.code,
                            name = EXCLUDED.name,
                            full_name = EXCLUDED.full_name,
                            international_abbr = EXCLUDED.international_abbr,
                            updated_at = NOW()
                    """, (
                        ref_key,
                        item.get('Code', ''),
                        item.get('Description', ''),
                        item.get('НаименованиеПолное', ''),
                        item.get('МеждународноеСокращение', ''),
                    ))
                    count += 1
                except Exception as e:
                    print(f"    Ошибка записи: {e}")
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {count} единиц измерения")
        return count

  
    def sync_specifications(self, conn):
        """Синхронизация ресурсных спецификаций с полными данными."""
        print("\n[Ресурсные спецификации]")
        
        items = self.get_catalog_items("Catalog_РесурсныеСпецификации")
        
        if not items:
            print("  Нет данных")
            return 0
        
        count = 0
        with conn.cursor() as cur:
            for item in items:
                try:
                    ref_key = item.get('Ref_Key')
                    if not ref_key or ref_key == EMPTY_UUID:
                        continue
                    
                    # Статус
                    status_raw = item.get('Статус', '')
                    status = status_raw if isinstance(status_raw, str) else ''
                    
                    # Автоматический выбор (инвертируем флаг исключения)
                    exclude_auto = item.get('ИсключитьАвтоматическийВыборВДокументах', False)
                    auto_select = 'Вручную' if exclude_auto else 'Автоматически'
                    
                    # Основное изделие (номенклатура) - КЛЮЧЕВОЕ ПОЛЕ!
                    product_key = item.get('ОсновноеИзделиеНоменклатура_Key')
                    if product_key == EMPTY_UUID:
                        product_key = None
                    
                    # Количество выхода
                    product_qty = item.get('ОсновноеИзделиеКоличествоУпаковок', 0)
                    try:
                        product_qty = float(product_qty) if product_qty else 0
                    except:
                        product_qty = 0
                    
                    # Есть вложенные спецификации
                    has_nested = item.get('ЕстьВложенныеСпецификации', False)
                    
                    cur.execute("""
                        INSERT INTO c1_specifications 
                        (ref_key, code, name, owner_key, is_deleted, updated_at,
                         product_key, product_quantity, status, auto_select, has_nested)
                        VALUES (%s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s)
                        ON CONFLICT (ref_key) DO UPDATE SET
                            code = EXCLUDED.code,
                            name = EXCLUDED.name,
                            owner_key = EXCLUDED.owner_key,
                            is_deleted = EXCLUDED.is_deleted,
                            updated_at = NOW(),
                            product_key = EXCLUDED.product_key,
                            product_quantity = EXCLUDED.product_quantity,
                            status = EXCLUDED.status,
                            auto_select = EXCLUDED.auto_select,
                            has_nested = EXCLUDED.has_nested
                    """, (
                        ref_key,
                        item.get('Code', ''),
                        item.get('Description', ''),
                        item.get('Owner_Key') if item.get('Owner_Key') != EMPTY_UUID else None,
                        item.get('DeletionMark', False),
                        product_key,
                        product_qty,
                        status,
                        auto_select,
                        has_nested
                    ))
                    count += 1
                except Exception as e:
                    print(f"    Ошибка записи спецификации: {e}")
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {count} спецификаций")
        return count

    def sync_all_catalogs(self, conn):
        """Синхронизация всех справочников."""
        print("\n" + "=" * 60)
        print("СПРАВОЧНИКИ (новые)")
        print("=" * 60)
        
        ensure_catalog_tables(conn)
        ensure_units_table(conn)  # <-- ДОБАВЛЕНО
        
        self.sync_departments(conn)
        self.sync_positions(conn)
        self.sync_employees(conn)
        self.sync_cash_flow_items(conn)
        self.sync_units(conn)        # <-- ДОБАВЛЕНО
        self.sync_specifications(conn)

    def sync_production(self, conn, date_from, date_to):
        """Синхронизация документов 'Производство без заказа'."""
        from urllib.parse import quote
        
        print("\n[Производство без заказа]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ПроизводствоБезЗаказа", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    print(f"    Ошибка HTTP {r.status_code}")
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        # Сохраняем в БД
        with conn.cursor() as cur:
            # Очищаем за период
            cur.execute(
                "DELETE FROM c1_production WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            cur.execute(
                """DELETE FROM c1_production_items WHERE production_key IN 
                   (SELECT ref_key FROM c1_production WHERE doc_date BETWEEN %s AND %s)""",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_production (ref_key, doc_number, doc_date, posted, 
                        organization_key, department_key, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        posted = EXCLUDED.posted,
                        organization_key = EXCLUDED.organization_key,
                        department_key = EXCLUDED.department_key,
                        comment = EXCLUDED.comment,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Подразделение_Key') if doc.get('Подразделение_Key') != EMPTY_UUID else None,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
                
                # Табличная часть - выходные изделия
                for item in doc.get('ВыходныеИзделия', []):
                    cur.execute("""
                        INSERT INTO c1_production_items (production_key, line_number,
                            nomenclature_key, specification_key, quantity, price, sum_total)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (production_key, line_number) DO NOTHING
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('Спецификация_Key') if item.get('Спецификация_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0),
                        item.get('Цена', 0),
                        item.get('Сумма', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} документов производства")
        return len(all_docs)

    def sync_cost_allocation(self, conn, date_from, date_to):
        """Синхронизация документов 'Списание затрат на выпуск'."""
        from urllib.parse import quote
        
        print("\n[Списание затрат на выпуск]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_СписаниеЗатратНаВыпуск", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_cost_allocation WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_cost_allocation (ref_key, doc_number, doc_date, posted,
                        organization_key, department_key, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Подразделение_Key') if doc.get('Подразделение_Key') != EMPTY_UUID else None,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
                
                # Материалы
                for item in doc.get('МатериалыИУслуги', []):
                    cur.execute("""
                        INSERT INTO c1_cost_allocation_materials (doc_key, line_number,
                            nomenclature_key, quantity, sum_total)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0),
                        item.get('Сумма', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} документов списания затрат")
        return len(all_docs)

    def sync_material_orders(self, conn, date_from, date_to):
        """Синхронизация документов 'Заказ материалов в производство'."""
        from urllib.parse import quote
        
        print("\n[Заказ материалов в производство]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ЗаказМатериаловВПроизводство", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_material_orders WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_material_orders (ref_key, doc_number, doc_date, posted,
                        organization_key, department_key, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Подразделение_Key') if doc.get('Подразделение_Key') != EMPTY_UUID else None,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
                
                # Удаляем старые позиции
                cur.execute("DELETE FROM c1_material_order_items WHERE order_key = %s", (ref_key,))

                for item in doc.get('Товары', []):
                    cur.execute("""
                        INSERT INTO c1_material_order_items (order_key, line_number,
                            nomenclature_key, quantity)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} заказов материалов")
        return len(all_docs)

    def sync_material_transfers(self, conn, date_from, date_to):
        """Синхронизация документов 'Передача материалов в производство'."""
        from urllib.parse import quote
        
        print("\n[Передача материалов в производство]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ПередачаМатериаловВПроизводство", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_material_transfers WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_material_transfers (ref_key, doc_number, doc_date, posted,
                        organization_key, department_key, warehouse_key, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Подразделение_Key') if doc.get('Подразделение_Key') != EMPTY_UUID else None,
                    doc.get('Склад_Key') if doc.get('Склад_Key') != EMPTY_UUID else None,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))

                # Удаляем старые позиции
                cur.execute("DELETE FROM c1_material_transfer_items WHERE transfer_key = %s", (ref_key,))
                
                for item in doc.get('Товары', []):
                    cur.execute("""
                        INSERT INTO c1_material_transfer_items (transfer_key, line_number,
                            nomenclature_key, quantity)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} передач материалов")
        return len(all_docs)

    def sync_all_production(self, conn, date_from, date_to):
        """Синхронизация всех документов производства."""
        print("\n" + "=" * 60)
        print("ДОКУМЕНТЫ ПРОИЗВОДСТВА")
        print("=" * 60)
        
        ensure_production_tables(conn)
        
        self.sync_production(conn, date_from, date_to)
        self.sync_cost_allocation(conn, date_from, date_to)
        self.sync_material_orders(conn, date_from, date_to)
        self.sync_material_transfers(conn, date_from, date_to)
  
    def sync_inventory_count(self, conn, date_from, date_to):
        """Синхронизация документов 'Пересчет товаров'."""
        from urllib.parse import quote
        
        print("\n[Пересчет товаров]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ПересчетТоваров", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_inventory_count WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_inventory_count (ref_key, doc_number, doc_date, posted,
                        organization_key, warehouse_key, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Склад_Key') if doc.get('Склад_Key') != EMPTY_UUID else None,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))

                # Удаляем старые позиции
                cur.execute("DELETE FROM c1_inventory_count_items WHERE doc_key = %s", (ref_key,))
              
                for item in doc.get('Товары', []):
                    cur.execute("""
                        INSERT INTO c1_inventory_count_items (doc_key, line_number,
                            nomenclature_key, quantity_fact, quantity_account, deviation)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('КоличествоФакт', 0),
                        item.get('КоличествоУчет', 0),
                        item.get('Отклонение', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} пересчетов")
        return len(all_docs)

    def sync_surplus(self, conn, date_from, date_to):
        """Синхронизация документов 'Оприходование излишков товаров'."""
        from urllib.parse import quote
        
        print("\n[Оприходование излишков]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ОприходованиеИзлишковТоваров", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_surplus WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_surplus (ref_key, doc_number, doc_date, posted,
                        organization_key, warehouse_key, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Склад_Key') if doc.get('Склад_Key') != EMPTY_UUID else None,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
                
                # Удаляем старые позиции
                cur.execute("DELETE FROM c1_surplus_items WHERE doc_key = %s", (ref_key,))

                for item in doc.get('Товары', []):
                    cur.execute("""
                        INSERT INTO c1_surplus_items (doc_key, line_number,
                            nomenclature_key, quantity, price, sum_total)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0),
                        item.get('Цена', 0),
                        item.get('Сумма', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} оприходований")
        return len(all_docs)

    def sync_regrade(self, conn, date_from, date_to):
        """Синхронизация документов 'Пересортица товаров'."""
        from urllib.parse import quote
        
        print("\n[Пересортица товаров]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ПересортицаТоваров", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_regrade WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_regrade (ref_key, doc_number, doc_date, posted,
                        organization_key, warehouse_key, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Склад_Key') if doc.get('Склад_Key') != EMPTY_UUID else None,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
                
                # Удаляем старые позиции
                cur.execute("DELETE FROM c1_regrade_items WHERE doc_key = %s", (ref_key,))

                for item in doc.get('Товары', []):
                    cur.execute("""
                        INSERT INTO c1_regrade_items (doc_key, line_number,
                            nomenclature_from_key, nomenclature_to_key, quantity)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('НоменклатураСписание_Key') if item.get('НоменклатураСписание_Key') != EMPTY_UUID else None,
                        item.get('НоменклатураОприходование_Key') if item.get('НоменклатураОприходование_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} пересортиц")
        return len(all_docs)

    def sync_shortage(self, conn, date_from, date_to):
        """Синхронизация документов 'Списание недостач товаров'."""
        from urllib.parse import quote
        
        print("\n[Списание недостач]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_СписаниеНедостачТоваров", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_shortage WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_shortage (ref_key, doc_number, doc_date, posted,
                        organization_key, warehouse_key, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Склад_Key') if doc.get('Склад_Key') != EMPTY_UUID else None,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
                
                # Удаляем старые позиции
                cur.execute("DELETE FROM c1_shortage_items WHERE doc_key = %s", (ref_key,))

                for item in doc.get('Товары', []):
                    cur.execute("""
                        INSERT INTO c1_shortage_items (doc_key, line_number,
                            nomenclature_key, quantity, price, sum_total)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0),
                        item.get('Цена', 0),
                        item.get('Сумма', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} списаний недостач")
        return len(all_docs)

    def sync_all_warehouse(self, conn, date_from, date_to):
        """Синхронизация всех складских документов."""
        print("\n" + "=" * 60)
        print("СКЛАДСКИЕ ДОКУМЕНТЫ")
        print("=" * 60)
        
        ensure_warehouse_tables(conn)
        
        self.sync_inventory_count(conn, date_from, date_to)
        self.sync_surplus(conn, date_from, date_to)
        self.sync_regrade(conn, date_from, date_to)
        self.sync_shortage(conn, date_from, date_to)

    def sync_bank_expenses(self, conn, date_from, date_to):
        """Синхронизация документов 'Списание безналичных ДС'."""
        from urllib.parse import quote
        
        print("\n[Списание безналичных ДС]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_СписаниеБезналичныхДенежныхСредств", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_bank_expenses WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_bank_expenses (ref_key, doc_number, doc_date, posted,
                        organization_key, bank_account_key, counterparty_key, amount, 
                        purpose, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        amount = EXCLUDED.amount,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('БанковскийСчетОрганизации_Key') if doc.get('БанковскийСчетОрганизации_Key') != EMPTY_UUID else None,
                    doc.get('Контрагент_Key') if doc.get('Контрагент_Key') != EMPTY_UUID else None,
                    doc.get('СуммаДокумента', 0),
                    doc.get('НазначениеПлатежа', ''),
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} списаний ДС")
        return len(all_docs)

    def sync_customer_orders(self, conn, date_from, date_to):
        """Синхронизация документов 'Заказ клиента'."""
        from urllib.parse import quote
        
        print("\n[Заказы клиентов]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ЗаказКлиента", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_customer_orders WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                shipment_date_raw = doc.get('ДатаОтгрузки', '')
                shipment_date = shipment_date_raw[:10] if shipment_date_raw and shipment_date_raw[:4] != '0001' else None

                cur.execute("""
                    INSERT INTO c1_customer_orders (ref_key, doc_number, doc_date, posted,
                        organization_key, partner_key, warehouse_key, amount, status, 
                        comment, is_deleted, deletion_mark, shipment_date, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        amount = EXCLUDED.amount,
                        status = EXCLUDED.status,
                        is_deleted = EXCLUDED.is_deleted,
                        deletion_mark = EXCLUDED.deletion_mark,
                        shipment_date = EXCLUDED.shipment_date,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Партнер_Key') if doc.get('Партнер_Key') != EMPTY_UUID else None,
                    doc.get('Склад_Key') if doc.get('Склад_Key') != EMPTY_UUID else None,
                    doc.get('СуммаДокумента', 0),
                    doc.get('Статус', ''),
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False),
                    doc.get('DeletionMark', False),
                    shipment_date
                ))

                # Удаляем старые позиции
                cur.execute("DELETE FROM c1_customer_order_items WHERE order_key = %s", (ref_key,))
              
                for item in doc.get('Товары', []):
                    cur.execute("""
                        INSERT INTO c1_customer_order_items (order_key, line_number,
                            nomenclature_key, quantity, price, sum_total)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (order_key, line_number) DO NOTHING
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0),
                        item.get('Цена', 0),
                        item.get('Сумма', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} заказов клиентов")
        return len(all_docs)

    def sync_sales_plan_light(self, conn, date_from, date_to):
        """Синхронизация планов продаж с маленьким batch_size."""
        from urllib.parse import quote
        
        print("\n[План продаж]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ПланПродаж", safe='_')
        all_docs = []
        skip = 0
        batch_size = 20  # Маленький batch для тяжёлых документов
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=180)  # Увеличенный таймаут
                if r.status_code != 200:
                    print(f"    Ошибка HTTP {r.status_code}")
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.5)  # Пауза между запросами
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_sales_plan WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_sales_plan (ref_key, doc_number, doc_date, posted,
                        organization_key, partner_key, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Партнер_Key') if doc.get('Партнер_Key') != EMPTY_UUID else None,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
                
                # Удаляем старые позиции
                cur.execute("DELETE FROM c1_sales_plan_items WHERE plan_key = %s", (ref_key,))

                for item in doc.get('Товары', []):
                    cur.execute("""
                        INSERT INTO c1_sales_plan_items (plan_key, line_number,
                            nomenclature_key, quantity, price, sum_total)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0),
                        item.get('Цена', 0),
                        item.get('Сумма', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} планов продаж")
        return len(all_docs)

    def sync_production_plan_light(self, conn, date_from, date_to):
        """Синхронизация планов производства с маленьким batch_size."""
        from urllib.parse import quote
        
        print("\n[План производства]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ПланПроизводства", safe='_')
        all_docs = []
        skip = 0
        batch_size = 20
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=180)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.5)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_production_plan WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_production_plan (ref_key, doc_number, doc_date, posted,
                        organization_key, department_key, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Подразделение_Key') if doc.get('Подразделение_Key') != EMPTY_UUID else None,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
                
                # Удаляем старые позиции
                cur.execute("DELETE FROM c1_production_plan_items WHERE plan_key = %s", (ref_key,))

                for item in doc.get('Продукция', []):
                    cur.execute("""
                        INSERT INTO c1_production_plan_items (plan_key, line_number,
                            nomenclature_key, quantity)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} планов производства")
        return len(all_docs)

    def sync_purchase_plan_light(self, conn, date_from, date_to):
        """Синхронизация планов закупок с маленьким batch_size."""
        from urllib.parse import quote
        
        print("\n[План закупок]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ПланЗакупок", safe='_')
        all_docs = []
        skip = 0
        batch_size = 20
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=180)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.5)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_purchase_plan WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_purchase_plan (ref_key, doc_number, doc_date, posted,
                        organization_key, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
                
                # Удаляем старые позиции
                cur.execute("DELETE FROM c1_purchase_plan_items WHERE plan_key = %s", (ref_key,))

                for item in doc.get('Товары', []):
                    cur.execute("""
                        INSERT INTO c1_purchase_plan_items (plan_key, line_number,
                            nomenclature_key, quantity, price, sum_total)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0),
                        item.get('Цена', 0),
                        item.get('Сумма', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} планов закупок")
        return len(all_docs)

    def sync_supplier_orders(self, conn, date_from, date_to):
        """Синхронизация документов 'Заказ поставщику'."""
        from urllib.parse import quote
        
        print("\n[Заказы поставщикам]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ЗаказПоставщику", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_supplier_orders WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_supplier_orders (ref_key, doc_number, doc_date, posted,
                        organization_key, partner_key, warehouse_key, amount, status, 
                        comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        amount = EXCLUDED.amount,
                        status = EXCLUDED.status,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Партнер_Key') if doc.get('Партнер_Key') != EMPTY_UUID else None,
                    doc.get('Склад_Key') if doc.get('Склад_Key') != EMPTY_UUID else None,
                    doc.get('СуммаДокумента', 0),
                    doc.get('Статус', ''),
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
                
                # Удаляем старые позиции
                cur.execute("DELETE FROM c1_supplier_order_items WHERE order_key = %s", (ref_key,))

                for item in doc.get('Товары', []):
                    cur.execute("""
                        INSERT INTO c1_supplier_order_items (order_key, line_number,
                            nomenclature_key, quantity, price, sum_total)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0),
                        item.get('Цена', 0),
                        item.get('Сумма', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} заказов поставщикам")
        return len(all_docs)

    def sync_production_plan(self, conn, date_from, date_to):
        """Синхронизация документов 'План производства'."""
        from urllib.parse import quote
        
        print("\n[План производства]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ПланПроизводства", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_production_plan WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_production_plan (ref_key, doc_number, doc_date, posted,
                        organization_key, department_key, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Подразделение_Key') if doc.get('Подразделение_Key') != EMPTY_UUID else None,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
                
                # Удаляем старые позиции
                cur.execute("DELETE FROM c1_production_plan_items WHERE plan_key = %s", (ref_key,))

                for item in doc.get('Продукция', []):
                    cur.execute("""
                        INSERT INTO c1_production_plan_items (plan_key, line_number,
                            nomenclature_key, quantity)
                        VALUES (%s, %s, %s, %s)
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} планов производства")
        return len(all_docs)

    def sync_purchase_plan(self, conn, date_from, date_to):
        """Синхронизация документов 'План закупок'."""
        from urllib.parse import quote
        
        print("\n[План закупок]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ПланЗакупок", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_purchase_plan WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_purchase_plan (ref_key, doc_number, doc_date, posted,
                        organization_key, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
                
                # Удаляем старые позиции
                cur.execute("DELETE FROM c1_purchase_plan_items WHERE plan_key = %s", (ref_key,))

                for item in doc.get('Товары', []):
                    cur.execute("""
                        INSERT INTO c1_purchase_plan_items (plan_key, line_number,
                            nomenclature_key, quantity, price, sum_total)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0),
                        item.get('Цена', 0),
                        item.get('Сумма', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} планов закупок")
        return len(all_docs)

    def sync_internal_consumption(self, conn, date_from, date_to):
        """Синхронизация документов 'Внутреннее потребление'."""
        from urllib.parse import quote
        
        print("\n[Внутреннее потребление]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ВнутреннееПотребление", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_internal_consumption WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_internal_consumption (ref_key, doc_number, doc_date, posted,
                        organization_key, department_key, warehouse_key, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Подразделение_Key') if doc.get('Подразделение_Key') != EMPTY_UUID else None,
                    doc.get('Склад_Key') if doc.get('Склад_Key') != EMPTY_UUID else None,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
                
                # Удаляем старые позиции
                cur.execute("DELETE FROM c1_internal_consumption_items WHERE doc_key = %s", (ref_key,))

                for item in doc.get('Товары', []):
                    cur.execute("""
                        INSERT INTO c1_internal_consumption_items (doc_key, line_number,
                            nomenclature_key, quantity, sum_total)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0),
                        item.get('Сумма', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} внутренних потреблений")
        return len(all_docs)

    def sync_debt_offset(self, conn, date_from, date_to):
        """Синхронизация документов 'Взаимозачет задолженности'."""
        from urllib.parse import quote
        
        print("\n[Взаимозачет задолженности]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ВзаимозачетЗадолженности", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_debt_offset WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                # Считаем суммы из табличных частей
                debit_sum = sum(float(item.get('Сумма', 0) or 0) for item in doc.get('ДебиторскаяЗадолженность', []))
                credit_sum = sum(float(item.get('Сумма', 0) or 0) for item in doc.get('КредиторскаяЗадолженность', []))
                
                cur.execute("""
                    INSERT INTO c1_debt_offset (ref_key, doc_number, doc_date, posted,
                        organization_key, counterparty_key, amount_debit, amount_credit,
                        comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        amount_debit = EXCLUDED.amount_debit,
                        amount_credit = EXCLUDED.amount_credit,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Контрагент_Key') if doc.get('Контрагент_Key') != EMPTY_UUID else None,
                    debit_sum,
                    credit_sum,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} взаимозачетов")
        return len(all_docs)

    def sync_sales_plan(self, conn, date_from, date_to):
        """Синхронизация документов 'План продаж'."""
        from urllib.parse import quote
        
        print("\n[План продаж]")
        
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        encoded = quote("Document_ПланПродаж", safe='_')
        all_docs = []
        skip = 0
        batch_size = 100
        
        while True:
            url = (
                f"{self.base_url}/{encoded}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                all_docs.extend(docs)
                print(f"    Загружено: {len(all_docs)}...")
                
                if len(docs) < batch_size:
                    break
                
                skip += batch_size
                time.sleep(0.2)
            except Exception as e:
                print(f"    Ошибка: {e}")
                break
        
        print(f"    Всего документов: {len(all_docs)}")
        
        if not all_docs:
            return 0
        
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM c1_sales_plan WHERE doc_date BETWEEN %s AND %s",
                (date_from, date_to)
            )
            
            for doc in all_docs:
                ref_key = doc.get('Ref_Key')
                
                cur.execute("""
                    INSERT INTO c1_sales_plan (ref_key, doc_number, doc_date, posted,
                        organization_key, partner_key, comment, is_deleted, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (ref_key) DO UPDATE SET
                        doc_number = EXCLUDED.doc_number,
                        doc_date = EXCLUDED.doc_date,
                        updated_at = NOW()
                """, (
                    ref_key,
                    doc.get('Number', '').strip(),
                    doc.get('Date', '')[:10],
                    doc.get('Posted', False),
                    doc.get('Организация_Key') if doc.get('Организация_Key') != EMPTY_UUID else None,
                    doc.get('Партнер_Key') if doc.get('Партнер_Key') != EMPTY_UUID else None,
                    doc.get('Комментарий', ''),
                    doc.get('DeletionMark', False)
                ))
                
                # Удаляем старые позиции
                cur.execute("DELETE FROM c1_sales_plan_items WHERE plan_key = %s", (ref_key,))

                for item in doc.get('Товары', []):
                    cur.execute("""
                        INSERT INTO c1_sales_plan_items (plan_key, line_number,
                            nomenclature_key, quantity, price, sum_total)
                        VALUES (%s, %s, %s, %s, %s, %s)
                    """, (
                        ref_key,
                        item.get('LineNumber'),
                        item.get('Номенклатура_Key') if item.get('Номенклатура_Key') != EMPTY_UUID else None,
                        item.get('Количество', 0),
                        item.get('Цена', 0),
                        item.get('Сумма', 0)
                    ))
            
            conn.commit()
        
        print(f"  ✅ Сохранено: {len(all_docs)} планов продаж")
        return len(all_docs)
  
    def sync_all_finance(self, conn, date_from, date_to):
        """Синхронизация всех финансовых документов и заказов."""
        print("\n" + "=" * 60)
        print("ФИНАНСОВЫЕ ДОКУМЕНТЫ И ЗАКАЗЫ")
        print("=" * 60)
        
        ensure_finance_tables(conn)
        
        self.sync_bank_expenses(conn, date_from, date_to)
        self.sync_customer_orders(conn, date_from, date_to)
        self.sync_supplier_orders(conn, date_from, date_to)
        self.sync_sales_plan(conn, date_from, date_to)
        self.sync_production_plan(conn, date_from, date_to)
        self.sync_purchase_plan(conn, date_from, date_to)
        self.sync_internal_consumption(conn, date_from, date_to)
        self.sync_debt_offset(conn, date_from, date_to)
  
    def get_catalog(self, catalog_name, batch_size=1000):
        """Загрузка справочника"""
        from urllib.parse import quote
        
        encoded_catalog = quote(catalog_name, safe='_')
        
        params = {
          "$format": "json",
          "$top": str(batch_size),
          "$orderby": "Ref_Key"
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
                items = [sanitize_dict(item) for item in items]
                
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
        """Синхронизация номенклатуры с единицами измерения."""
        print("\n[НОМЕНКЛАТУРА]")
        items = self.get_catalog("Catalog_Номенклатура")
        print(f"  Получено {len(items)} позиций")
        
        if not items:
            return
        
        # Загружаем справочник единиц измерения в кэш
        units_cache = {}
        try:
            cur = conn.cursor()
            cur.execute("SELECT ref_key, name FROM c1_units")
            for row in cur.fetchall():
                units_cache[row[0]] = row[1]
            cur.close()
            print(f"  Загружено {len(units_cache)} единиц измерения в кэш")
        except Exception as e:
            print(f"  Предупреждение: не удалось загрузить единицы измерения: {e}")
        
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
            
            # Получаем название единицы измерения из кэша
            unit_name = units_cache.get(unit_key, '') if unit_key else ''
            
            # Вес и единица веса
            weight = None
            weight_unit_key = item.get('ВесЕдиницаИзмерения_Key')
            weight_unit = units_cache.get(weight_unit_key, '') if weight_unit_key and weight_unit_key != EMPTY_UUID else ''
            
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
                unit_name,
                weight,
                weight_unit,
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
                   type_id=EXCLUDED.type_id, unit_id=EXCLUDED.unit_id,
                   unit_name=EXCLUDED.unit_name, weight=EXCLUDED.weight,
                   weight_unit=EXCLUDED.weight_unit""",
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
        """Синхронизация продаж с фильтрацией по дате на стороне 1С"""
        from urllib.parse import quote
        
        print("\n[ПРОДАЖИ]")
        
        # Формируем даты для фильтра
        date_from_str = date_from.strftime("%Y-%m-%dT00:00:00")
        date_to_str = date_to.strftime("%Y-%m-%dT23:59:59")
        
        # Загружаем реализации порциями
        print("  Загрузка реализаций...")
        
        encoded_entity = quote("Document_РеализацияТоваровУслуг", safe='_')
        
        sales_docs = []
        skip = 0
        batch_size = 100
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        while consecutive_errors < max_consecutive_errors:
            # Формируем URL вручную (requests кодирует $ что ломает запрос)
            url = (
                f"{self.base_url}/{encoded_entity}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url, timeout=120)
                
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
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                # Данные уже отфильтрованы на стороне 1С
                sales_docs.extend(docs)
                
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
        
        corrections = []
        skip = 0
        consecutive_errors = 0
        
        while consecutive_errors < max_consecutive_errors:
            # Формируем URL вручную
            url_corr = (
                f"{self.base_url}/{encoded_corr}"
                f"?$format=json"
                f"&$top={batch_size}"
                f"&$skip={skip}"
                f"&$filter=Date%20ge%20datetime'{date_from_str}'%20and%20Date%20le%20datetime'{date_to_str}'%20and%20Posted%20eq%20true"
                f"&$orderby=Date%20desc"
            )
            
            try:
                r = self.session.get(url_corr, timeout=120)
                
                if r.status_code == 500:
                    skip += batch_size
                    consecutive_errors += 1
                    continue
                
                if r.status_code != 200:
                    break
                
                docs = r.json().get('value', [])
                docs = [sanitize_dict(doc) for doc in docs]
                
                if not docs:
                    break
                
                # Данные уже отфильтрованы на стороне 1С
                corrections.extend(docs)
                
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
    
    # Синхронизация справочников (базовые)
    print("\n" + "=" * 60)
    print("СПРАВОЧНИКИ (базовые)")
    print("=" * 60)
    
    sync.sync_nomenclature_types(conn)
    # Синхронизация новых справочников
    sync.sync_all_catalogs(conn)
    sync.sync_nomenclature(conn)
    sync.sync_clients(conn)
    
    
    
    # Синхронизация документов
    print("\n" + "=" * 60)
    print("ДОКУМЕНТЫ")
    print("=" * 60)
    
    sync.sync_purchases(conn, date_from, date_to)
    sync.sync_sales(conn, date_from, date_to)
    sync.sync_all_production(conn, date_from, date_to)
    sync.sync_all_warehouse(conn, date_from, date_to)
    sync.sync_all_finance(conn, date_from, date_to)

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

def main_quick(sync, conn):
    """Быстрая синхронизация каждые 5 минут — продажи и заказы за 3 дня."""
    print("\n" + "=" * 60)
    print("БЫСТРАЯ СИНХРОНИЗАЦИЯ (5 мин)")
    print("=" * 60)
    
    date_to = datetime.now().date()
    date_from = date_to - timedelta(days=3)
    print(f"Период: {date_from} — {date_to}")
    
    sync.sync_sales(conn, date_from, date_to)
    sync.sync_customer_orders(conn, date_from, date_to)


def main_hourly(sync, conn):
    """Часовая синхронизация — закупки, производство, материалы за 3 дня."""
    print("\n" + "=" * 60)
    print("ЧАСОВАЯ СИНХРОНИЗАЦИЯ (30 мин)")
    print("=" * 60)
    
    date_to = datetime.now().date()
    date_from = date_to - timedelta(days=3)
    print(f"Период: {date_from} — {date_to}")
    
    ensure_production_tables(conn)
    
    sync.sync_purchases(conn, date_from, date_to)
    sync.sync_production(conn, date_from, date_to)
    sync.sync_cost_allocation(conn, date_from, date_to)
    sync.sync_material_orders(conn, date_from, date_to)
    sync.sync_material_transfers(conn, date_from, date_to)


def main_daily(sync, conn):
    """Ежедневная синхронизация — все документы за 7 дней."""
    print("\n" + "=" * 60)
    print("ЕЖЕДНЕВНАЯ СИНХРОНИЗАЦИЯ")
    print("=" * 60)
    
    date_to = datetime.now().date()
    date_from = date_to - timedelta(days=7)
    print(f"Период: {date_from} — {date_to}")
    
    # Таблицы
    ensure_production_tables(conn)
    ensure_warehouse_tables(conn)
    ensure_finance_tables(conn)
    
    # Продажи и закупки
    sync.sync_sales(conn, date_from, date_to)
    sync.sync_purchases(conn, date_from, date_to)
    
    # Производство
    sync.sync_production(conn, date_from, date_to)
    sync.sync_cost_allocation(conn, date_from, date_to)
    sync.sync_material_orders(conn, date_from, date_to)
    sync.sync_material_transfers(conn, date_from, date_to)
    
    # Складские
    sync.sync_inventory_count(conn, date_from, date_to)
    sync.sync_surplus(conn, date_from, date_to)
    sync.sync_regrade(conn, date_from, date_to)
    sync.sync_shortage(conn, date_from, date_to)
    
    # Финансовые (кроме тяжёлых планов)
    sync.sync_bank_expenses(conn, date_from, date_to)
    sync.sync_customer_orders(conn, date_from, date_to)
    sync.sync_supplier_orders(conn, date_from, date_to)
    sync.sync_internal_consumption(conn, date_from, date_to)
    sync.sync_debt_offset(conn, date_from, date_to)
    
    # Тяжёлые планы — с маленьким batch_size
    sync.sync_sales_plan_light(conn, date_from, date_to)
    sync.sync_production_plan_light(conn, date_from, date_to)
    sync.sync_purchase_plan_light(conn, date_from, date_to)

def main():
    parser = argparse.ArgumentParser(description='Синхронизация 1С → PostgreSQL')
    parser.add_argument('--incremental', '-i', action='store_true', 
                        help='Инкрементальная синхронизация (только новые документы)')
    parser.add_argument('--full', '-f', action='store_true',
                        help='Полная синхронизация (все документы за год)')
    parser.add_argument('--quick', '-q', action='store_true',
                        help='Быстрая синхронизация (продажи и заказы за 3 дня)')
    parser.add_argument('--hourly', action='store_true',
                        help='Часовая синхронизация (закупки, производство за 3 дня)')
    parser.add_argument('--daily', '-d', action='store_true',
                        help='Ежедневная синхронизация (все документы за 7 дней)')
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
        if args.quick:
            main_quick(sync, conn)
        elif args.hourly:
            main_hourly(sync, conn)
        elif args.daily:
            main_daily(sync, conn)
        elif args.incremental:
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
