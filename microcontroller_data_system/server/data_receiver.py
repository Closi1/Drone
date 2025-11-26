import socket
import json
import sqlite3
from datetime import datetime
import threading

class DataReceiver:
    def __init__(self):
        self.host = "localhost"
        self.port = 8888
        self.db_path = "sensor_data.db"
        self.setup_database()
    
    def setup_database(self):
        """Создаёт базу данных SQLite и таблицу"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sensor_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                device_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                temperature REAL,
                humidity REAL,
                pressure REAL,
                voltage REAL,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                synced INTEGER DEFAULT 0
            )
        ''')
        
        conn.commit()
        conn.close()
        print(f"✅ База данных создана: {self.db_path}")
    
    def save_to_database(self, data):
        """Сохраняет данные в SQLite"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute('''
                INSERT INTO sensor_data 
                (device_id, timestamp, temperature, humidity, pressure, voltage)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                data['device_id'],
                data['timestamp'],
                data['temperature'],
                data['humidity'],
                data['pressure'],
                data['voltage']
            ))
            
            conn.commit()
            conn.close()
            print(f"💾 Данные сохранены в базу от устройства: {data['device_id']}")
            return True
            
        except Exception as e:
            print(f"❌ Ошибка сохранения в базу: {e}")
            return False
    
    def handle_client(self, client_socket, address):
        """Обрабатывает подключение клиента"""
        try:
            print(f"🔌 Подключился клиент: {address}")
            
            # Получаем данные
            data = client_socket.recv(1024).decode('utf-8')
            print(f"📥 Получены данные: {data}")
            
            # Парсим JSON
            sensor_data = json.loads(data)
            
            # Сохраняем в базу
            success = self.save_to_database(sensor_data)
            
            # Отправляем ответ клиенту
            if success:
                response = "OK: Data saved successfully"
            else:
                response = "ERROR: Failed to save data"
                
            client_socket.send(response.encode('utf-8'))
            
        except json.JSONDecodeError:
            print("❌ Ошибка: Неверный JSON формат")
            client_socket.send("ERROR: Invalid JSON format".encode('utf-8'))
        except Exception as e:
            print(f"❌ Ошибка обработки: {e}")
            client_socket.send("ERROR: Processing failed".encode('utf-8'))
        finally:
            client_socket.close()
    
    def start_server(self):
        """Запускает сервер"""
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        try:
            server_socket.bind((self.host, self.port))
            server_socket.listen(5)
            
            print("🖥️ СЕРВЕР ДЛЯ ПРИЁМА ДАННЫХ ЗАПУЩЕН")
            print(f"📍 Адрес: {self.host}:{self.port}")
            print(f"💾 База данных: {self.db_path}")
            print("=" * 50)
            print("⏳ Ожидаем подключения микроконтроллеров...")
            
            while True:
                client_socket, address = server_socket.accept()
                
                # Обрабатываем каждого клиента в отдельном потоке
                client_thread = threading.Thread(
                    target=self.handle_client, 
                    args=(client_socket, address)
                )
                client_thread.daemon = True
                client_thread.start()
                
        except KeyboardInterrupt:
            print("\n🛑 Сервер остановлен")
        except Exception as e:
            print(f"❌ Ошибка сервера: {e}")
        finally:
            server_socket.close()

# Запускаем сервер
if __name__ == "__main__":
    receiver = DataReceiver()
    receiver.start_server()