import sqlite3
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import numpy as np
from drone_database import DroneDatabase

class FlightDataViewer:
    def __init__(self, db_path="drone_flight_data.db"):
        self.db_path = db_path
        self.database = DroneDatabase(db_path)
    
    def show_database_stats(self):
        """Показывает статистику базы данных"""
        self.database.get_database_stats()
    
    def show_recent_flights(self):
        """Показывает последние полёты"""
        df_flights = self.database.get_recent_flights(10)
        
        print("📊 ПОСЛЕДНИЕ ПОЛЁТЫ:")
        print("=" * 120)
        
        if len(df_flights) > 0:
            # Форматируем вывод
            df_display = df_flights.copy()
            df_display['start_time'] = pd.to_datetime(df_display['start_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
            df_display['end_time'] = pd.to_datetime(df_display['end_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
            df_display['total_flight_time'] = df_display['total_flight_time'].round(1)
            df_display['total_distance'] = df_display['total_distance'].round(1)
            df_display['max_altitude'] = df_display['max_altitude'].round(1)
            df_display['max_speed'] = df_display['max_speed'].round(1)
            
            # Переименовываем колонки для красивого вывода
            df_display = df_display.rename(columns={
                'session_id': 'ID',
                'start_time': 'Начало',
                'end_time': 'Конец',
                'total_flight_time': 'Время (сек)',
                'total_distance': 'Дистанция (м)',
                'max_altitude': 'Макс. высота (м)',
                'max_speed': 'Макс. скорость (м/с)',
                'status': 'Статус'
            })
            
            print(df_display.to_string(index=False))
        else:
            print("Нет данных о полётах")
    
    def show_flight_details(self, session_id):
        """Показывает детали конкретного полёта"""
        session_info = self.database.get_session_info(session_id)
        
        if not session_info:
            print(f"❌ Сессия #{session_id} не найдена")
            return
        
        print(f"\n📈 ДЕТАЛИ ПОЛЁТА #{session_id}:")
        print("=" * 60)
        print(f"🕒 Начало: {session_info['start_time']}")
        print(f"🕒 Конец: {session_info['end_time']}")
        print(f"⏱️ Общее время: {session_info['total_flight_time']:.1f} сек")
        print(f"📏 Пройдено: {session_info['total_distance']:.1f} м")
        print(f"📈 Макс. высота: {session_info['max_altitude']:.1f} м")
        print(f"🚀 Макс. скорость: {session_info['max_speed']:.1f} м/с")
        print(f"📋 Статус: {session_info['status']}")
        
        # Получаем статистику
        stats = self.database.get_flight_statistics(session_id)
        print(f"\n📊 СТАТИСТИКА ЗАПИСЕЙ:")
        print(f"   📍 Позиций: {stats['position_count']}")
        print(f"   📝 Событий: {stats['event_count']}")
        print(f"   📈 Статистик: {stats['stats_count']}")
        print(f"   🚁 Данных пропеллеров: {stats['propeller_count']}")
        print(f"   🎯 Данных IMU: {stats['imu_count']}")
        
        # События полёта
        df_events = self.database.get_flight_events(session_id)
        
        print(f"\n📝 СОБЫТИЯ ПОЛЁТА ({len(df_events)}):")
        if len(df_events) > 0:
            for _, event in df_events.iterrows():
                time = pd.to_datetime(event['event_time']).strftime('%H:%M:%S')
                print(f"   {time} - {event['event_type']}: {event['event_data']}")
        else:
            print("   Нет событий")
    
    def show_propeller_data(self, session_id):
        """Показывает данные пропеллеров"""
        df_propellers = self.database.get_propeller_data(session_id)
        
        if len(df_propellers) == 0:
            print(f"❌ Нет данных о пропеллерах для сессии #{session_id}")
            return
        
        print(f"\n🚁 ДАННЫЕ ПРОПЕЛЛЕРОВ #{session_id}:")
        print("=" * 100)
        
        # Берем только последние 5 записей для показа
        df_display = df_propellers.tail(5).copy()
        df_display['timestamp'] = pd.to_datetime(df_display['timestamp']).dt.strftime('%H:%M:%S')
        
        # Округляем значения
        for col in df_display.columns:
            if 'thrust' in col or 'speed' in col:
                df_display[col] = df_display[col].round(2)
        
        print(df_display.to_string(index=False))
        print(f"\nВсего записей: {len(df_propellers)}")
    
    def show_imu_data(self, session_id):
        """Показывает данные IMU для сессии"""
        df_imu = self.database.get_imu_data(session_id)
        
        if len(df_imu) == 0:
            print(f"❌ Нет данных IMU для сессии #{session_id}")
            return
        
        print(f"\n🎯 ДАННЫЕ IMU СЕССИИ #{session_id}:")
        print("=" * 120)
        
        df_display = df_imu.tail(5).copy()
        df_display['timestamp'] = pd.to_datetime(df_display['timestamp']).dt.strftime('%H:%M:%S.%f')[:-3]
        
        # Округляем значения
        for col in df_display.columns:
            if col != 'timestamp':
                df_display[col] = df_display[col].round(4)
        
        print(df_display.to_string(index=False))
        print(f"\nВсего записей IMU: {len(df_imu)}")
    
    def plot_flight_trajectory(self, session_id):
        """Строит график траектории полёта"""
        df_positions = self.database.get_flight_positions(session_id)
        
        if len(df_positions) == 0:
            print(f"❌ Нет данных о позициях для сессии #{session_id}")
            return
        
        # Создаём 3D график
        fig = plt.figure(figsize=(16, 12))
        
        # 3D траектория
        ax1 = fig.add_subplot(221, projection='3d')
        ax1.plot(df_positions['pos_x'], df_positions['pos_y'], df_positions['pos_z'], 
                'b-', alpha=0.6, linewidth=2)
        ax1.scatter(df_positions['pos_x'].iloc[0], df_positions['pos_y'].iloc[0], df_positions['pos_z'].iloc[0],
                   c='green', s=100, marker='o', label='Старт')
        ax1.scatter(df_positions['pos_x'].iloc[-1], df_positions['pos_y'].iloc[-1], df_positions['pos_z'].iloc[-1],
                   c='red', s=100, marker='o', label='Финиш')
        ax1.set_xlabel('X (м)')
        ax1.set_ylabel('Y (м)')
        ax1.set_zlabel('Z (м)')
        ax1.set_title(f'3D Траектория полёта #{session_id}')
        ax1.legend()
        
        # Высота по времени
        ax2 = fig.add_subplot(222)
        df_positions['time_sec'] = (pd.to_datetime(df_positions['timestamp']) - 
                                  pd.to_datetime(df_positions['timestamp']).iloc[0]).dt.total_seconds()
        ax2.plot(df_positions['time_sec'], df_positions['pos_z'], 'g-', linewidth=2)
        ax2.set_xlabel('Время (сек)')
        ax2.set_ylabel('Высота (м)')
        ax2.set_title('Высота полёта')
        ax2.grid(True)
        
        # 2D траектория (вид сверху)
        ax3 = fig.add_subplot(223)
        scatter = ax3.scatter(df_positions['pos_x'], df_positions['pos_y'], 
                             c=df_positions['pos_z'], cmap='viridis', s=20)
        ax3.plot(df_positions['pos_x'], df_positions['pos_y'], 'k-', alpha=0.3)
        ax3.set_xlabel('X (м)')
        ax3.set_ylabel('Y (м)')
        ax3.set_title('Траектория (вид сверху)')
        plt.colorbar(scatter, ax=ax3, label='Высота (м)')
        ax3.grid(True)
        
        # Скорость по времени
        ax4 = fig.add_subplot(224)
        # Вычисляем скорость как производную позиции
        dt = np.diff(df_positions['time_sec'])
        dx = np.diff(df_positions['pos_x'])
        dy = np.diff(df_positions['pos_y'])
        dz = np.diff(df_positions['pos_z'])
        speed = np.sqrt(dx**2 + dy**2 + dz**2) / dt
        
        # Добавляем нулевое значение в начало для совпадения размеров
        speed = np.concatenate(([0], speed))
        
        ax4.plot(df_positions['time_sec'], speed, 'r-', linewidth=2)
        ax4.set_xlabel('Время (сек)')
        ax4.set_ylabel('Скорость (м/с)')
        ax4.set_title('Скорость полёта')
        ax4.grid(True)
        
        plt.tight_layout()
        plt.show()
    
    def plot_propeller_data(self, session_id):
        """Строит графики данных пропеллеров"""
        df_propellers = self.database.get_propeller_data(session_id)
        
        if len(df_propellers) == 0:
            print(f"❌ Нет данных о пропеллерах для сессии #{session_id}")
            return
        
        # Создаём графики
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
        
        # Время в секундах от начала
        df_propellers['time_sec'] = (pd.to_datetime(df_propellers['timestamp']) - 
                                   pd.to_datetime(df_propellers['timestamp']).iloc[0]).dt.total_seconds()
        
        # График тяги пропеллеров
        ax1.plot(df_propellers['time_sec'], df_propellers['propeller_1_thrust'], 'r-', label='Пропеллер 1', linewidth=2)
        ax1.plot(df_propellers['time_sec'], df_propellers['propeller_2_thrust'], 'b-', label='Пропеллер 2', linewidth=2)
        ax1.plot(df_propellers['time_sec'], df_propellers['propeller_3_thrust'], 'g-', label='Пропеллер 3', linewidth=2)
        ax1.plot(df_propellers['time_sec'], df_propellers['propeller_4_thrust'], 'orange', label='Пропеллер 4', linewidth=2)
        ax1.set_xlabel('Время (сек)')
        ax1.set_ylabel('Тяга (Н)')
        ax1.set_title('Тяга пропеллеров')
        ax1.legend()
        ax1.grid(True)
        
        # График скорости пропеллеров
        ax2.plot(df_propellers['time_sec'], df_propellers['propeller_1_speed'], 'r-', label='Пропеллер 1', linewidth=2)
        ax2.plot(df_propellers['time_sec'], df_propellers['propeller_2_speed'], 'b-', label='Пропеллер 2', linewidth=2)
        ax2.plot(df_propellers['time_sec'], df_propellers['propeller_3_speed'], 'g-', label='Пропеллер 3', linewidth=2)
        ax2.plot(df_propellers['time_sec'], df_propellers['propeller_4_speed'], 'orange', label='Пропеллер 4', linewidth=2)
        ax2.set_xlabel('Время (сек)')
        ax2.set_ylabel('Скорость (RPM)')
        ax2.set_title('Скорость пропеллеров')
        ax2.legend()
        ax2.grid(True)
        
        plt.tight_layout()
        plt.show()
    
    def plot_imu_data(self, session_id):
        """Строит графики данных IMU"""
        df_imu = self.database.get_imu_data(session_id)
        
        if len(df_imu) == 0:
            print(f"❌ Нет данных IMU для сессии #{session_id}")
            return
        
        # Создаём графики
        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
        
        # Время в секундах от начала
        df_imu['time_sec'] = (pd.to_datetime(df_imu['timestamp']) - 
                            pd.to_datetime(df_imu['timestamp']).iloc[0]).dt.total_seconds()
        
        # График гироскопа
        ax1.plot(df_imu['time_sec'], df_imu['gyro_roll_rate'], 'r-', label='Крен', linewidth=2)
        ax1.plot(df_imu['time_sec'], df_imu['gyro_pitch_rate'], 'b-', label='Тангаж', linewidth=2)
        ax1.plot(df_imu['time_sec'], df_imu['gyro_yaw_rate'], 'g-', label='Рыскание', linewidth=2)
        ax1.set_xlabel('Время (сек)')
        ax1.set_ylabel('Скорость (рад/с)')
        ax1.set_title('Гироскоп - угловые скорости')
        ax1.legend()
        ax1.grid(True)
        
        # График акселерометра
        ax2.plot(df_imu['time_sec'], df_imu['accel_x'], 'r-', label='X', linewidth=2)
        ax2.plot(df_imu['time_sec'], df_imu['accel_y'], 'b-', label='Y', linewidth=2)
        ax2.plot(df_imu['time_sec'], df_imu['accel_z'], 'g-', label='Z', linewidth=2)
        ax2.set_xlabel('Время (сек)')
        ax2.set_ylabel('Ускорение (м/с²)')
        ax2.set_title('Акселерометр - линейные ускорения')
        ax2.legend()
        ax2.grid(True)
        
        # График ориентации
        ax3.plot(df_imu['time_sec'], np.degrees(df_imu['estimated_roll']), 'r-', label='Крен', linewidth=2)
        ax3.plot(df_imu['time_sec'], np.degrees(df_imu['estimated_pitch']), 'b-', label='Тангаж', linewidth=2)
        ax3.plot(df_imu['time_sec'], np.degrees(df_imu['estimated_yaw']), 'g-', label='Рыскание', linewidth=2)
        ax3.set_xlabel('Время (сек)')
        ax3.set_ylabel('Угол (градусы)')
        ax3.set_title('Ориентация по данным IMU')
        ax3.legend()
        ax3.grid(True)
        
        # График уверенности ориентации
        ax4.plot(df_imu['time_sec'], df_imu['orientation_confidence'], 'purple', linewidth=2)
        ax4.set_xlabel('Время (сек)')
        ax4.set_ylabel('Уверенность')
        ax4.set_title('Уверенность оценки ориентации')
        ax4.grid(True)
        
        plt.tight_layout()
        plt.show()

if __name__ == "__main__":
    viewer = FlightDataViewer()
    
    print("📊 ПРОСМОТР ДАННЫХ ПОЛЁТОВ ДРОНА")
    print("=" * 50)
    
    while True:
        print("\nВыберите действие:")
        print("1 - Показать статистику базы данных")
        print("2 - Показать последние полёты")
        print("3 - Детали полёта")
        print("4 - Данные пропеллеров")
        print("5 - Данные IMU (гироскоп/акселерометр)")
        print("6 - График траектории")
        print("7 - График данных пропеллеров")
        print("8 - График данных IMU")
        print("9 - Экспорт данных полёта")
        print("0 - Выход")
        
        choice = input("Ваш выбор (0-9): ").strip()
        
        if choice == '1':
            viewer.show_database_stats()
        elif choice == '2':
            viewer.show_recent_flights()
        elif choice == '3':
            session_id = input("Введите ID сессии: ").strip()
            if session_id.isdigit():
                viewer.show_flight_details(int(session_id))
            else:
                print("❌ Неверный ID сессии")
        elif choice == '4':
            session_id = input("Введите ID сессии: ").strip()
            if session_id.isdigit():
                viewer.show_propeller_data(int(session_id))
            else:
                print("❌ Неверный ID сессии")
        elif choice == '5':
            session_id = input("Введите ID сессии: ").strip()
            if session_id.isdigit():
                viewer.show_imu_data(int(session_id))
            else:
                print("❌ Неверный ID сессии")
        elif choice == '6':
            session_id = input("Введите ID сессии: ").strip()
            if session_id.isdigit():
                viewer.plot_flight_trajectory(int(session_id))
            else:
                print("❌ Неверный ID сессии")
        elif choice == '7':
            session_id = input("Введите ID сессии: ").strip()
            if session_id.isdigit():
                viewer.plot_propeller_data(int(session_id))
            else:
                print("❌ Неверный ID сессии")
        elif choice == '8':
            session_id = input("Введите ID сессии: ").strip()
            if session_id.isdigit():
                viewer.plot_imu_data(int(session_id))
            else:
                print("❌ Неверный ID сессии")
        elif choice == '9':
            session_id = input("Введите ID сессии: ").strip()
            if session_id.isdigit():
                viewer.database.export_flight_data(int(session_id))
            else:
                print("❌ Неверный ID сессии")
        elif choice == '0':
            print("👋 Выход из программы")
            break
        else:
            print("❌ Неверный выбор")