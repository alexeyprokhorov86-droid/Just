"""
Синхронизация данных из 1С:КА 2.5 в PostgreSQL
"""

import requests
from requests.auth import HTTPBasicAuth
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import time

# ============================================================
# НАСТРОЙКИ - ИЗМЕНИ ПОД СЕБЯ
# ============================================================

CONFIG_1C = {
    "base_url": "http://185.126.95.33:81/NB_KA/odata/standard.odata",
    "username": "odata.user",
    "password": "gE9tibul",
}

CONFIG_PG = {
    "host": "localhost",
    "port": 5432,
    "database": "knowledge_base",
    "user": "knowledge",
    "password": "Prokhorov2025Secure",
}

# ============================================================
# КЛАСС ДЛЯ РАБОТЫ С 1С
# ============================================================

class Sync1C:
    def __init__(self):
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(
            CONFIG_1C["username"], 
            CONFIG_1C["password"]
        )
        self.session.headers.update({
            'Accept': 'application/json',
            'Content-Type': 'application/json; charset=utf-8'
        })
        self.base_url = CONFIG_1C["base_url"]
        
        # Кэш
        self.contractors_cache = {}
        self.nomenclature_cache = {}
    
    def test_connection(self):
        """Проверка подключения к 1С"""
        try:
            r = self.session.get(
                f"{self.base_url}/Catalog_Контрагенты?$top=1&$format=json",
                timeout=30
            )
            return r.status_code == 200
        except:
            return False
    
    def get_contractor_name(self, key):
        """Получить название контрагента"""
        if not key or key == "00000000-0000-0000-0000-000000000000":
            return "Не указан"
        
        if key in self.contractors_cache:
            return self.contractors_cache[key]
        
        try:
            r = self.session.get(
                f"{self.base_url}/Catalog_Контрагенты(guid'{key}')?$format=json",
                timeout=30
            )
            if r.status_code == 200:
                name = r.json().get('Description', 'Неизвестный')
                self.contractors_cache[key] = name
                return name
        except:
            pass
        return "Неизвестный"
    
    def get_nomenclature_name(self, key):
        """Получить название номенклатуры"""
        if not key or key == "00000000-0000-0000-0000-000000000000":
            return "Не указана"
        
        if key in self.nomenclature_cache:
            return self.nomenclature_cache[key]
        
        try:
            r = self.session.get(
                f"{self.base_url}/Catalog_Номенклатура(guid'{key}')?$format=json",
                timeout=30
            )
            if r.status_code == 200:
                name = r.json().get('Description', 'Неизвестная')
                self.nomenclature_cache[key] = name
                return name
        except:
            pass
        return "Неизвестная"
    
    def get_purchases(self, date_from, date_to):
        """Получить документы приобретения"""
        params = {
            "$filter": "Posted eq true",
            "$format": "json"
        }
        
        url = f"{self.base_url}/Document_ПриобретениеТоваровУслуг"
        all_docs = []
        
        while url:
            try:
                r = self.session.get(url, params=params, timeout=60)
                if r.status_code != 200:
                    print(f"Ошибка HTTP: {r.status_code}")
                    break
                
                data = r.json()
                docs = data.get('value', [])
                all_docs.extend(docs)
                print(f"  Загружено {len(all_docs)} документов...")
                
                url = data.get('odata.nextLink') or data.get('@odata.nextLink')
                params = {}
                time.sleep(0.3)
            except Exception as e:
                print(f"Ошибка: {e}")
                break
        
        # Фильтруем по дате
        filtered = []
        for doc in all_docs:
            doc_date_str = doc.get('Date', '')[:10]
            try:
                doc_date = datetime.strptime(doc_date_str, "%Y-%m-%d").date()
                if date_from <= doc_date <= date_to:
                    filtered.append(doc)
            except:
                continue
        
        print(f"  После фильтрации по дате: {len(filtered)} документов")
        return filtered
    
    def extract_prices(self, date_from, date_to):
        """Извлечь данные о ценах"""
        docs = self.get_purchases(date_from, date_to)
        
        records = []
        for doc in docs:
            doc_date = doc.get('Date', '')[:10]
            doc_number = doc.get('Number', '').strip()
            contractor_key = doc.get('Контрагент_Key', '')
            contractor_name = self.get_contractor_name(contractor_key)
            
            for item in doc.get('Товары', []):
                nom_key = item.get('Номенклатура_Key', '')
                nom_name = self.get_nomenclature_name(nom_key)
                
                quantity = item.get('Количество', 0) or 0
                price = item.get('Цена', 0) or 0
                summa = item.get('СуммаСНДС', 0) or item.get('Сумма', 0) or 0
                
                if quantity > 0:
                    records.append({
                        'doc_date': doc_date,
                        'doc_number': doc_number,
                        'contractor_id': contractor_key if contractor_key != "00000000-0000-0000-0000-000000000000" else None,
                        'contractor_name': contractor_name,
                        'nomenclature_id': nom_key if nom_key != "00000000-0000-0000-0000-000000000000" else None,
                        'nomenclature_name': nom_name,
                        'quantity': round(quantity, 3),
                        'price': round(price, 2),
                        'sum_total': round(summa, 2),
                    })
        
        print(f"  Извлечено {len(records)} записей о ценах")
        return records


# ============================================================
# ЗАПИСЬ В POSTGRESQL
# ============================================================

def save_to_postgres(records):
    """Сохранить записи в PostgreSQL"""
    if not records:
        print("Нет данных для сохранения")
        return
    
    conn = psycopg2.connect(**CONFIG_PG)
    cur = conn.cursor()
    
    # Очищаем старые данные за период
    dates = [r['doc_date'] for r in records]
    min_date, max_date = min(dates), max(dates)
    
    cur.execute(
        "DELETE FROM purchase_prices WHERE doc_date BETWEEN %s AND %s",
        (min_date, max_date)
    )
    print(f"  Удалено старых записей за {min_date} - {max_date}")
    
    # Вставляем новые
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
    conn.close()
    print(f"  Сохранено {len(records)} записей в PostgreSQL")


# ============================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================

def main():
    print("=" * 60)
    print("СИНХРОНИЗАЦИЯ 1С → PostgreSQL")
    print("=" * 60)
    
    sync = Sync1C()
    
    # Проверка подключения к 1С
    print("\n[1] Проверка подключения к 1С...")
    if not sync.test_connection():
        print("ОШИБКА: Не удалось подключиться к 1С")
        return
    print("OK")
    
    # Период синхронизации (последний год)
    date_to = datetime.now().date()
    date_from = date_to - timedelta(days=365)
    
    print(f"\n[2] Загрузка данных за {date_from} - {date_to}...")
    records = sync.extract_prices(date_from, date_to)
    
    print(f"\n[3] Сохранение в PostgreSQL...")
    save_to_postgres(records)
    
    print("\n" + "=" * 60)
    print("ГОТОВО!")
    print("=" * 60)


if __name__ == "__main__":
    main()

