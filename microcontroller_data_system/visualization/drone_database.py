import sqlite3
import json
from datetime import datetime
import numpy as np
import pandas as pd  # Добавьте эту строку

class DroneDatabase:
    def __init__(self, db_path="drone_flight_data.db"):
        self.db_path = db_path
        self.setup_database()
    
    def setup_database(self):
        """Создаёт базу данных и таблицы для хранения данных дрона"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Основная таблица с данными о полёте
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS flight_sessions (
                session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                end_time TIMESTAMP,
                total_flight_time REAL DEFAULT 0,
                total_distance REAL DEFAULT 0,
                max_altitude REAL DEFAULT 0,
                max_speed REAL DEFAULT 0,
                status TEXT DEFAULT 'COMPLETED'
            )
        ''')
        
        # Таблица с данными о позиции дрона в реальном времени
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS drone_positions (
                position_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                pos_x REAL,
                pos_y REAL, 
                pos_z REAL,
                velocity_x REAL,
                velocity_y REAL,
                velocity_z REAL,
                roll REAL,
                pitch REAL,
                yaw REAL,
                thrust REAL,
                control_mode TEXT,
                FOREIGN KEY (session_id) REFERENCES flight_sessions (session_id)
            )
        ''')
        
        # Таблица с событиями (взлёт, посадка, смена режима)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS flight_events (
                event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER,
                event_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                event_type TEXT,
                event_data TEXT,
                FOREIGN KEY (session_id) REFERENCES flight_sessions (session_id)
            )
        ''')
        
        # Таблица со статистикой полёта
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS flight_statistics (
                stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                altitude REAL,
                speed REAL,
                battery_usage REAL DEFAULT 0,
                temperature REAL DEFAULT 25.0,
                FOREIGN KEY (session_id) REFERENCES flight_sessions (session_id)
            )
        ''')
        
        # Таблица с данными пропеллеров
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS propeller_data (
                propeller_id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                propeller_1_thrust REAL,
                propeller_2_thrust REAL,
                propeller_3_thrust REAL,
                propeller_4_thrust REAL,
                propeller_1_speed REAL,
                propeller_2_speed REAL,
                propeller_3_speed REAL,
                propeller_4_speed REAL,
                FOREIGN KEY (session_id) REFERENCES flight_sessions (session_id)
            )
        ''')
        
        conn.commit()
        conn.close()
        print(f"✅ База данных дрона создана: {self.db_path}")
    
    def start_new_flight_session(self):
        """Начинает новую сессию полёта и возвращает её ID"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO flight_sessions (start_time, status) 
            VALUES (datetime('now'), 'IN_PROGRESS')
        ''')
        
        session_id = cursor.lastrowid
        
        # Записываем событие взлёта
        cursor.execute('''
            INSERT INTO flight_events (session_id, event_type, event_data)
            VALUES (?, 'TAKEOFF', 'Дрон взлетел')
        ''', (session_id,))
        
        conn.commit()
        conn.close()
        
        print(f"🚀 Начата новая сессия полёта #{session_id}")
        return session_id
    
    def save_drone_position(self, session_id, drone):
        """Сохраняет текущую позицию и состояние дрона"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Сохраняем позицию
        cursor.execute('''
            INSERT INTO drone_positions 
            (session_id, pos_x, pos_y, pos_z, velocity_x, velocity_y, velocity_z, 
             roll, pitch, yaw, thrust, control_mode)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            session_id,
            float(drone.position[0]),
            float(drone.position[1]), 
            float(drone.position[2]),
            float(drone.velocity[0]),
            float(drone.velocity[1]),
            float(drone.velocity[2]),
            float(drone.orientation[0]),
            float(drone.orientation[1]),
            float(drone.orientation[2]),
            float(drone.forces['thrust']),
            drone.control_mode
        ))
        
        # Сохраняем статистику
        altitude = float(drone.position[2])
        speed = float(np.linalg.norm(drone.velocity))
        
        cursor.execute('''
            INSERT INTO flight_statistics 
            (session_id, altitude, speed)
            VALUES (?, ?, ?)
        ''', (session_id, altitude, speed))
        
        # Сохраняем данные пропеллеров
        cursor.execute('''
            INSERT INTO propeller_data 
            (session_id, 
             propeller_1_thrust, propeller_2_thrust, propeller_3_thrust, propeller_4_thrust,
             propeller_1_speed, propeller_2_speed, propeller_3_speed, propeller_4_speed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            session_id,
            float(drone.propeller_thrusts[0]),
            float(drone.propeller_thrusts[1]),
            float(drone.propeller_thrusts[2]),
            float(drone.propeller_thrusts[3]),
            float(drone.propeller_speeds[0]),
            float(drone.propeller_speeds[1]),
            float(drone.propeller_speeds[2]),
            float(drone.propeller_speeds[3])
        ))
        
        conn.commit()
        conn.close()
    
    def record_flight_event(self, session_id, event_type, event_data=""):
        """Записывает событие полёта"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO flight_events (session_id, event_type, event_data)
            VALUES (?, ?, ?)
        ''', (session_id, event_type, event_data))
        
        conn.commit()
        conn.close()
        
        print(f"📝 Событие: {event_type} - {event_data}")
    
    def end_flight_session(self, session_id, total_time, total_distance, max_altitude, max_speed):
        """Завершает сессию полёта и сохраняет итоговую статистику"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE flight_sessions 
            SET end_time = datetime('now'),
                total_flight_time = ?,
                total_distance = ?,
                max_altitude = ?,
                max_speed = ?,
                status = 'COMPLETED'
            WHERE session_id = ?
        ''', (total_time, total_distance, max_altitude, max_speed, session_id))
        
        # Записываем событие посадки
        cursor.execute('''
            INSERT INTO flight_events (session_id, event_type, event_data)
            VALUES (?, 'LANDING', 'Дрон приземлился')
        ''', (session_id,))
        
        conn.commit()
        conn.close()
        
        print(f"🛬 Завершена сессия полёта #{session_id}")
        print(f"   ⏱️ Время: {total_time:.1f} сек")
        print(f"   📏 Дистанция: {total_distance:.1f} м")
        print(f"   📈 Макс. высота: {max_altitude:.1f} м")
        print(f"   🚀 Макс. скорость: {max_speed:.1f} м/с")
    
    def get_session_info(self, session_id):
        """Возвращает основную информацию о сессии"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('SELECT * FROM flight_sessions WHERE session_id = ?', (session_id,))
        session_data = cursor.fetchone()
        
        conn.close()
        
        if session_data:
            return {
                'session_id': session_data[0],
                'start_time': session_data[1],
                'end_time': session_data[2],
                'total_flight_time': session_data[3],
                'total_distance': session_data[4],
                'max_altitude': session_data[5],
                'max_speed': session_data[6],
                'status': session_data[7]
            }
        return None
    
    def get_flight_statistics(self, session_id):
        """Возвращает статистику полёта"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Получаем количество записей в каждой таблице
        cursor.execute('SELECT COUNT(*) FROM drone_positions WHERE session_id = ?', (session_id,))
        position_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM flight_events WHERE session_id = ?', (session_id,))
        event_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM flight_statistics WHERE session_id = ?', (session_id,))
        stats_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM propeller_data WHERE session_id = ?', (session_id,))
        propeller_count = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            'position_count': position_count,
            'event_count': event_count,
            'stats_count': stats_count,
            'propeller_count': propeller_count
        }
    
    def get_recent_flights(self, limit=10):
        """Возвращает последние полёты"""
        conn = sqlite3.connect(self.db_path)
        
        df_flights = pd.read_sql_query(f'''
            SELECT session_id, start_time, end_time, total_flight_time, 
                   total_distance, max_altitude, max_speed, status
            FROM flight_sessions 
            ORDER BY start_time DESC 
            LIMIT {limit}
        ''', conn)
        
        conn.close()
        return df_flights
    
    def get_flight_events(self, session_id):
        """Возвращает события полёта"""
        conn = sqlite3.connect(self.db_path)
        
        df_events = pd.read_sql_query('''
            SELECT event_time, event_type, event_data 
            FROM flight_events 
            WHERE session_id = ? 
            ORDER BY event_time
        ''', conn, params=(session_id,))
        
        conn.close()
        return df_events
    
    def get_flight_positions(self, session_id):
        """Возвращает позиции полёта"""
        conn = sqlite3.connect(self.db_path)
        
        df_positions = pd.read_sql_query('''
            SELECT timestamp, pos_x, pos_y, pos_z, velocity_x, velocity_y, velocity_z,
                   roll, pitch, yaw, thrust, control_mode
            FROM drone_positions 
            WHERE session_id = ?
            ORDER BY timestamp
        ''', conn, params=(session_id,))
        
        conn.close()
        return df_positions
    
    def get_propeller_data(self, session_id):
        """Возвращает данные пропеллеров"""
        conn = sqlite3.connect(self.db_path)
        
        df_propellers = pd.read_sql_query('''
            SELECT timestamp,
                   propeller_1_thrust, propeller_2_thrust, propeller_3_thrust, propeller_4_thrust,
                   propeller_1_speed, propeller_2_speed, propeller_3_speed, propeller_4_speed
            FROM propeller_data 
            WHERE session_id = ?
            ORDER BY timestamp
        ''', conn, params=(session_id,))
        
        conn.close()
        return df_propellers
    
    def export_flight_data(self, session_id, filename=None):
        """Экспортирует данные полёта в JSON файл"""
        if filename is None:
            filename = f"flight_session_{session_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Получаем все данные
        session_info = self.get_session_info(session_id)
        if not session_info:
            print(f"❌ Сессия #{session_id} не найдена")
            return None
        
        statistics = self.get_flight_statistics(session_id)
        events = self.get_flight_events(session_id)
        positions = self.get_flight_positions(session_id)
        propeller_data = self.get_propeller_data(session_id)
        
        # Формируем данные для экспорта
        export_data = {
            'session_info': session_info,
            'statistics': statistics,
            'events': events.to_dict('records'),
            'positions': positions.to_dict('records'),
            'propeller_data': propeller_data.to_dict('records'),
            'export_time': datetime.now().isoformat()
        }
        
        # Сохраняем в JSON файл
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"💾 Данные полёта #{session_id} экспортированы в {filename}")
        return filename
    
    def get_database_stats(self):
        """Возвращает статистику базы данных"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        tables = ['flight_sessions', 'drone_positions', 'flight_events', 'flight_statistics', 'propeller_data']
        stats = {}
        
        for table in tables:
            cursor.execute(f'SELECT COUNT(*) FROM {table}')
            count = cursor.fetchone()[0]
            stats[table] = count
        
        conn.close()
        
        print("📊 СТАТИСТИКА БАЗЫ ДАННЫХ:")
        print("=" * 40)
        for table, count in stats.items():
            print(f"   {table}: {count} записей")
        
        return stats