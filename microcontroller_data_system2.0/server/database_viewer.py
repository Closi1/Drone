import sqlite3
import pandas as pd
from datetime import datetime

class DatabaseViewer:
    def __init__(self):
        self.db_path = "sensor_data.db"
    
    def show_all_data(self):
        """Показывает все данные из базы"""
        try:
            conn = sqlite3.connect(self.db_path)
            
            # Используем pandas для красивого вывода
            df = pd.read_sql_query("SELECT * FROM sensor_data", conn)
            
            print("📊 ВСЕ ДАННЫЕ ИЗ БАЗЫ:")
            print("=" * 80)
            
            if len(df) > 0:
                print(df.to_string(index=False))
            else:
                print("База данных пуста")
                
            print(f"\n📈 Всего записей: {len(df)}")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Ошибка чтения базы: {e}")
    
    def show_statistics(self):
        """Показывает статистику по данным"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Общее количество записей
            cursor.execute("SELECT COUNT(*) FROM sensor_data")
            total_records = cursor.fetchone()[0]
            
            # Количество устройств
            cursor.execute("SELECT COUNT(DISTINCT device_id) FROM sensor_data")
            unique_devices = cursor.fetchone()[0]
            
            # Последняя запись
            cursor.execute("SELECT MAX(received_at) FROM sensor_data")
            last_record = cursor.fetchone()[0]
            
            # Несинхронизированные записи
            cursor.execute("SELECT COUNT(*) FROM sensor_data WHERE synced = 0")
            unsynced_records = cursor.fetchone()[0]
            
            print("\n📈 СТАТИСТИКА БАЗЫ ДАННЫХ:")
            print("=" * 40)
            print(f"📋 Всего записей: {total_records}")
            print(f"📟 Уникальных устройств: {unique_devices}")
            print(f"🔄 Несинхронизировано: {unsynced_records}")
            print(f"🕒 Последняя запись: {last_record}")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Ошибка статистики: {e}")

if __name__ == "__main__":
    viewer = DatabaseViewer()
    viewer.show_all_data()
    viewer.show_statistics()