import numpy as np
import math
from datetime import datetime
import random
from drone_database import DroneDatabase
import pandas as pd

class ControllableDrone:
    def __init__(self):
        # Позиция дрона в пространстве (x, y, z)
        self.position = np.array([0.0, 0.0, 3.0])
        
        # Углы ориентации (крен, тангаж, рыскание) в радианах
        self.orientation = np.array([0.0, 0.0, 0.0])  # [roll, pitch, yaw]
        
        # Скорости вращения
        self.angular_velocity = np.array([0.0, 0.0, 0.0])
        
        # Линейная скорость
        self.velocity = np.array([0.0, 0.0, 0.0])
        
        # Управляющие сигналы от пользователя
        self.control_input = {
            'thrust': 9.81,     # Базовая тяга (компенсирует гравитацию)
            'pitch': 0.0,       # Наклон вперед/назад
            'roll': 0.0,        # Наклон влево/вправо  
            'yaw': 0.0          # Поворот
        }
        
        # Индивидуальная тяга каждого пропеллера
        self.propeller_thrusts = np.array([2.4525, 2.4525, 2.4525, 2.4525])  # Равномерное распределение
        
        # Параметры пропеллеров
        self.propeller_speeds = np.array([1000.0, 1000.0, 1000.0, 1000.0])  # RPM
        self.propeller_max_thrust = 6.0  # Максимальная тяга одного пропеллера
        self.propeller_efficiency = np.array([0.95, 0.97, 0.96, 0.94])  # КПД каждого пропеллера
        
        # Силы, действующие на дрон
        self.forces = {
            'thrust': 9.81,     # Суммарная тяга двигателей
            'drag': 0.0,        # Сопротивление воздуха
            'gravity': 9.81,    # Гравитация
            'wind': np.array([0.0, 0.0, 0.0])  # Ветер
        }
        
        # Параметры области перемещения
        self.area_limits = {
            'x_min': -8.0,
            'x_max': 8.0,
            'y_min': -8.0, 
            'y_max': 8.0,
            'z_min': 0.5,
            'z_max': 15.0
        }
        
        # Режимы управления
        self.control_mode = 'MANUAL'  # 'MANUAL' или 'AUTO'
        
        # Для автономного режима
        self.target_position = self.generate_random_target()
        self.target_counter = 0
        
        # Геометрия дрона
        self.geometry = self._create_quadcopter_geometry()
        
        # Время последнего обновления
        self.last_update = datetime.now()
        
        # Статистика
        self.flight_time = 0.0
        self.distance_traveled = 0.0
        self.last_position = self.position.copy()
        self.max_altitude = 0.0
        self.max_speed = 0.0
        
        # Система сохранения данных
        self.database = DroneDatabase()
        self.current_session_id = None
        self.last_save_time = datetime.now()
        self.save_interval = 0.2  # Сохранять каждые 0.2 секунды
        
    def start_data_logging(self):
        """Начинает запись данных полёта"""
        if self.current_session_id is None:
            self.current_session_id = self.database.start_new_flight_session()
            print("📊 Начата запись данных полёта")
    
    def stop_data_logging(self):
        """Останавливает запись данных и сохраняет итоги"""
        if self.current_session_id is not None:
            self.database.end_flight_session(
                self.current_session_id,
                self.flight_time,
                self.distance_traveled,
                self.max_altitude,
                self.max_speed
            )
            self.current_session_id = None
            print("📊 Запись данных полёта завершена")
    
    def save_current_state(self):
        """Сохраняет текущее состояние дрона в базу данных"""
        if self.current_session_id is not None:
            current_time = datetime.now()
            if (current_time - self.last_save_time).total_seconds() >= self.save_interval:
                self.database.save_drone_position(self.current_session_id, self)
                self.last_save_time = current_time
    
    def generate_random_target(self):
        """Генерирует случайную целевую позицию в пределах области"""
        return np.array([
            random.uniform(-6.0, 6.0),   # X
            random.uniform(-6.0, 6.0),   # Y  
            random.uniform(2.0, 10.0)    # Z (высота)
        ])
    
    def _create_quadcopter_geometry(self):
        """Создаёт геометрию квадрокоптера с 4 пропеллерами"""
        geometry = {
            'body': np.array([
                [0, 0, 0],          # Центр
                [0.2, 0, 0],        # Право
                [-0.2, 0, 0],       # Лево
                [0, 0.2, 0],        # Перед
                [0, -0.2, 0],       # Зад
            ]),
            'arms': [
                np.array([[0.2, 0, 0], [0.5, 0, 0]]),    # Правая рука
                np.array([[-0.2, 0, 0], [-0.5, 0, 0]]),  # Левая рука
                np.array([[0, 0.2, 0], [0, 0.5, 0]]),    # Передняя рука
                np.array([[0, -0.2, 0], [0, -0.5, 0]]),  # Задняя рука
            ],
            'motors': [
                np.array([0.5, 0, 0]),    # Правый мотор (1)
                np.array([-0.5, 0, 0]),   # Левый мотор (2)
                np.array([0, 0.5, 0]),    # Передний мотор (3)
                np.array([0, -0.5, 0]),   # Задний мотор (4)
            ],
            'propellers': []
        }
        
        # Создаём пропеллеры для каждого мотора с индивидуальными параметрами
        for i, motor_pos in enumerate(geometry['motors']):
            propeller = self._create_propeller_geometry(motor_pos, i)
            geometry['propellers'].append(propeller)
        
        return geometry
    
    def _create_propeller_geometry(self, center, propeller_id):
        """Создаёт геометрию пропеллера с индивидуальными параметрами"""
        points = []
        radius = 0.15
        num_points = 8
        
        # Немного разные размеры пропеллеров для реалистичности
        size_variation = [1.0, 0.95, 1.05, 0.98]  # Коэффициенты размера для каждого пропеллера
        actual_radius = radius * size_variation[propeller_id]
        
        for i in range(num_points):
            angle = 2 * math.pi * i / num_points
            x = center[0] + actual_radius * math.cos(angle)
            y = center[1] + actual_radius * math.sin(angle)
            z = center[2]
            points.append([x, y, z])
        
        return {
            'points': np.array(points),
            'radius': actual_radius,
            'center': center,
            'id': propeller_id
        }
    
    def calculate_propeller_thrusts(self, total_thrust, roll, pitch, yaw):
        """Вычисляет индивидуальные тяги для 4 пропеллеров"""
        # Базовая тяга для висения
        base_thrust = total_thrust / 4.0
        
        # Корректировки для управления:
        # Roll (крен) - разница между левыми и правыми пропеллерами
        roll_correction = roll * 0.5
        
        # Pitch (тангаж) - разница между передними и задними пропеллерами
        pitch_correction = pitch * 0.5
        
        # Yaw (рыскание) - разница между пропеллерами по и против часовой стрелки
        yaw_correction = yaw * 0.3
        
        # Распределение по пропеллерам (X-конфигурация):
        # Пропеллер 0: правый-передний (CW)
        # Пропеллер 1: левый-задний (CW) 
        # Пропеллер 2: левый-передний (CCW)
        # Пропеллер 3: правый-задний (CCW)
        
        thrusts = np.array([
            base_thrust - roll_correction + pitch_correction - yaw_correction,  # Правый-передний
            base_thrust + roll_correction - pitch_correction - yaw_correction,  # Левый-задний
            base_thrust + roll_correction + pitch_correction + yaw_correction,  # Левый-передний
            base_thrust - roll_correction - pitch_correction + yaw_correction   # Правый-задний
        ])
        
        # Учитываем КПД каждого пропеллера
        thrusts = thrusts * self.propeller_efficiency
        
        # Ограничиваем максимальную тягу
        thrusts = np.clip(thrusts, 0.1, self.propeller_max_thrust)
        
        return thrusts
    
    def update_propeller_speeds(self):
        """Обновляет скорости вращения пропеллеров на основе тяги"""
        # Скорость пропорциональна квадратному корню из тяги
        for i in range(4):
            thrust_ratio = self.propeller_thrusts[i] / self.propeller_max_thrust
            self.propeller_speeds[i] = 500.0 + 1500.0 * math.sqrt(thrust_ratio)
    
    def set_control_input(self, thrust_change=0, pitch=0, roll=0, yaw=0):
        """Устанавливает управляющие сигналы"""
        # Базовая тяга + изменение от пользователя
        self.control_input['thrust'] = 9.81 + thrust_change * 2.0
        
        # Ограничиваем максимальную тягу
        self.control_input['thrust'] = np.clip(self.control_input['thrust'], 5.0, 20.0)
        
        # Углы ориентации
        self.control_input['pitch'] = np.clip(pitch, -0.5, 0.5)
        self.control_input['roll'] = np.clip(roll, -0.5, 0.5)
        self.control_input['yaw'] = np.clip(yaw, -0.3, 0.3)
        
        # Вычисляем индивидуальные тяги пропеллеров
        self.propeller_thrusts = self.calculate_propeller_thrusts(
            self.control_input['thrust'],
            self.control_input['roll'],
            self.control_input['pitch'],
            self.control_input['yaw']
        )
        
        # Обновляем скорости пропеллеров
        self.update_propeller_speeds()
    
    def toggle_control_mode(self):
        """Переключает режим управления"""
        if self.control_mode == 'MANUAL':
            self.control_mode = 'AUTO'
            self.target_position = self.generate_random_target()
            if self.current_session_id:
                self.database.record_flight_event(
                    self.current_session_id, 
                    'MODE_CHANGE', 
                    'Переключен в режим АВТОПИЛОТ'
                )
        else:
            self.control_mode = 'MANUAL'
            if self.current_session_id:
                self.database.record_flight_event(
                    self.current_session_id, 
                    'MODE_CHANGE', 
                    'Переключен в режим РУЧНОЕ УПРАВЛЕНИЕ'
                )
    
    def update_geometry_based_on_forces(self):
        """Обновляет геометрию дрона на основе действующих сил"""
        # Деформация лучей под действием сил
        total_force_magnitude = np.linalg.norm(self.forces['thrust'])
        
        for i, arm in enumerate(self.geometry['arms']):
            # Деформация зависит от силы тяги соответствующего пропеллера
            propeller_force = self.propeller_thrusts[i]
            deformation = propeller_force * 0.02 * math.sin(datetime.now().timestamp() * 12 + i)
            
            # Применяем деформацию к лучам
            deformed_arm = arm.copy()
            deformed_arm[1][2] += deformation
            self.geometry['arms'][i] = deformed_arm
            
            # Обновляем позиции моторов
            self.geometry['motors'][i] = deformed_arm[1].copy()
            
            # Обновляем пропеллеры
            motor_pos = deformed_arm[1].copy()
            self.geometry['propellers'][i] = self._create_propeller_geometry(motor_pos, i)
    
    def auto_pilot(self):
        """Автопилот для движения к цели"""
        if self.control_mode != 'AUTO':
            return self.control_input['thrust'], 0, 0, 0
        
        # Вектор к цели
        to_target = self.target_position - self.position
        distance_to_target = np.linalg.norm(to_target)
        
        # Если близко к цели - генерируем новую
        if distance_to_target < 1.0:
            self.target_position = self.generate_random_target()
            to_target = self.target_position - self.position
            distance_to_target = np.linalg.norm(to_target)
            self.target_counter += 1
        
        # Нормализованный вектор направления
        if distance_to_target > 0.1:
            direction = to_target / distance_to_target
        else:
            direction = np.array([0.0, 0.0, 0.0])
        
        # Управляющие сигналы для движения к цели
        target_pitch = -direction[0] * 0.8
        target_roll = direction[1] * 0.8
        target_yaw = 0.0
        
        # Корректируем тягу для поддержания высоты
        height_error = self.target_position[2] - self.position[2]
        thrust_correction = height_error * 0.5
        
        total_thrust = 9.81 + thrust_correction
        total_thrust = np.clip(total_thrust, 5.0, 20.0)
        
        return total_thrust, target_roll, target_pitch, target_yaw
    
    def apply_control(self):
        """Применяет управляющие сигналы к дрону"""
        if self.control_mode == 'AUTO':
            thrust, roll, pitch, yaw = self.auto_pilot()
        else:
            thrust = self.control_input['thrust']
            roll = self.control_input['roll']
            pitch = self.control_input['pitch'] 
            yaw = self.control_input['yaw']
        
        # Плавное изменение ориентации
        self.angular_velocity = np.array([roll, pitch, yaw]) * 0.8
        
        # Обновляем суммарную силу тяги
        self.forces['thrust'] = thrust
        
        # Вычисляем индивидуальные тяги пропеллеров
        self.propeller_thrusts = self.calculate_propeller_thrusts(thrust, roll, pitch, yaw)
        self.update_propeller_speeds()
        
        # Случайные небольшие возмущения (турбулентность)
        if random.random() < 0.2:
            self.forces['wind'] = np.array([
                random.uniform(-0.3, 0.3),
                random.uniform(-0.3, 0.3),
                random.uniform(-0.1, 0.1)
            ])
        else:
            self.forces['wind'] = np.array([0.0, 0.0, 0.0])
        
        # Обновляем геометрию на основе новых сил
        self.update_geometry_based_on_forces()
    
    def enforce_area_limits(self):
        """Обеспечивает нахождение дрона в пределах области"""
        limits = self.area_limits
        
        # Ограничиваем позицию
        self.position[0] = np.clip(self.position[0], limits['x_min'], limits['x_max'])
        self.position[1] = np.clip(self.position[1], limits['y_min'], limits['y_max']) 
        self.position[2] = np.clip(self.position[2], limits['z_min'], limits['z_max'])
        
        # Если接近 границы - отталкиваемся
        border_margin = 1.0
        repulsion_force = 0.0
        
        # Проверяем границы по X
        if self.position[0] > limits['x_max'] - border_margin:
            repulsion_force = (limits['x_max'] - self.position[0] - border_margin) * 3.0
            self.velocity[0] -= repulsion_force
        elif self.position[0] < limits['x_min'] + border_margin:
            repulsion_force = (self.position[0] - limits['x_min'] - border_margin) * 3.0
            self.velocity[0] += repulsion_force
            
        # Проверяем границы по Y
        if self.position[1] > limits['y_max'] - border_margin:
            repulsion_force = (limits['y_max'] - self.position[1] - border_margin) * 3.0
            self.velocity[1] -= repulsion_force
        elif self.position[1] < limits['y_min'] + border_margin:
            repulsion_force = (self.position[1] - limits['y_min'] - border_margin) * 3.0
            self.velocity[1] += repulsion_force
            
        # Проверяем границы по Z (высота)
        if self.position[2] > limits['z_max'] - border_margin:
            repulsion_force = (limits['z_max'] - self.position[2] - border_margin) * 3.0
            self.velocity[2] -= repulsion_force
        elif self.position[2] < limits['z_min'] + border_margin:
            repulsion_force = (self.position[2] - limits['z_min'] - border_margin) * 3.0
            self.velocity[2] += repulsion_force
    
    def update_physics(self, dt):
        """Обновляет физику дрона"""
        # Обновляем ориентацию
        self.orientation += self.angular_velocity * dt
        
        # Ограничиваем углы (не более 45 градусов)
        max_angle = math.pi / 4
        self.orientation = np.clip(self.orientation, -max_angle, max_angle)
        
        # Матрица поворота
        roll, pitch, yaw = self.orientation
        
        # Вычисляем вектор тяги в глобальной системе координат
        thrust_vector = np.array([
            math.sin(pitch) * math.cos(roll),
            -math.sin(roll) * math.cos(pitch), 
            math.cos(roll) * math.cos(pitch)
        ]) * self.forces['thrust']
        
        # Сила тяжести
        gravity_force = np.array([0, 0, -self.forces['gravity']])
        
        # Сила сопротивления (пропорциональна скорости)
        drag_force = -self.velocity * 0.2
        
        # Суммарная сила
        total_force = thrust_vector + gravity_force + drag_force + self.forces['wind']
        
        # Ускорение (F = ma, m = 1)
        acceleration = total_force
        
        # Обновляем скорость и позицию
        self.velocity += acceleration * dt
        self.position += self.velocity * dt
        
        # Обновляем статистику
        self.flight_time += dt
        self.distance_traveled += np.linalg.norm(self.position - self.last_position)
        self.last_position = self.position.copy()
        
        # Обновляем максимумы
        current_altitude = self.position[2]
        current_speed = np.linalg.norm(self.velocity)
        self.max_altitude = max(self.max_altitude, current_altitude)
        self.max_speed = max(self.max_speed, current_speed)
        
        # Обеспечиваем нахождение в пределах области
        self.enforce_area_limits()
        
        # Демпфирование скорости у границ
        border_damping = 0.7
        limits = self.area_limits
        if (self.position[0] >= limits['x_max'] - 0.5 or self.position[0] <= limits['x_min'] + 0.5):
            self.velocity[0] *= border_damping
        if (self.position[1] >= limits['y_max'] - 0.5 or self.position[1] <= limits['y_min'] + 0.5):
            self.velocity[1] *= border_damping
        if (self.position[2] >= limits['z_max'] - 0.5 or self.position[2] <= limits['z_min'] + 0.5):
            self.velocity[2] *= border_damping
        
        # Сохраняем данные в базу
        self.save_current_state()
        
        # Обновляем время
        self.last_update = datetime.now()
    
    def get_transformed_geometry(self):
        """Возвращает геометрию дрона с учётом позиции и ориентации"""
        transformed = {}
        
        # Матрица поворота
        roll, pitch, yaw = self.orientation
        
        Rx = np.array([
            [1, 0, 0],
            [0, math.cos(roll), -math.sin(roll)],
            [0, math.sin(roll), math.cos(roll)]
        ])
        
        Ry = np.array([
            [math.cos(pitch), 0, math.sin(pitch)],
            [0, 1, 0],
            [-math.sin(pitch), 0, math.cos(pitch)]
        ])
        
        Rz = np.array([
            [math.cos(yaw), -math.sin(yaw), 0],
            [math.sin(yaw), math.cos(yaw), 0],
            [0, 0, 1]
        ])
        
        rotation_matrix = Rz @ Ry @ Rx
        
        # Преобразуем все элементы геометрии
        transformed['body'] = (rotation_matrix @ self.geometry['body'].T).T + self.position
        
        transformed['arms'] = []
        for arm in self.geometry['arms']:
            rotated_arm = (rotation_matrix @ arm.T).T + self.position
            transformed['arms'].append(rotated_arm)
        
        transformed['motors'] = (rotation_matrix @ np.array(self.geometry['motors']).T).T + self.position
        
        transformed['propellers'] = []
        for propeller_data in self.geometry['propellers']:
            rotated_points = (rotation_matrix @ propeller_data['points'].T).T + self.position
            transformed['propellers'].append({
                'points': rotated_points,
                'radius': propeller_data['radius'],
                'center': (rotation_matrix @ propeller_data['center'].T).T + self.position,
                'id': propeller_data['id'],
                'thrust': self.propeller_thrusts[propeller_data['id']],
                'speed': self.propeller_speeds[propeller_data['id']]
            })
        
        return transformed
    
    def get_force_vectors(self):
        """Возвращает векторы сил для визуализации"""
        vectors = {}
        
        roll, pitch, yaw = self.orientation
        thrust_direction = np.array([
            math.sin(pitch) * math.cos(roll),
            -math.sin(roll) * math.cos(pitch),
            math.cos(roll) * math.cos(pitch)
        ])
        
        vectors['thrust'] = {
            'start': self.position,
            'end': self.position + thrust_direction * self.forces['thrust'] * 0.1
        }
        
        # Векторы индивидуальной тяги пропеллеров
        vectors['propeller_thrusts'] = []
        geometry = self.get_transformed_geometry()
        for propeller_data in geometry['propellers']:
            thrust_magnitude = propeller_data['thrust'] * 0.05  # Масштаб для визуализации
            thrust_end = propeller_data['center'] + np.array([0, 0, thrust_magnitude])
            vectors['propeller_thrusts'].append({
                'start': propeller_data['center'],
                'end': thrust_end,
                'thrust': propeller_data['thrust'],
                'propeller_id': propeller_data['id']
            })
        
        # Вектор скорости
        vectors['velocity'] = {
            'start': self.position,
            'end': self.position + self.velocity * 0.5
        }
        
        # Вектор гравитации
        vectors['gravity'] = {
            'start': self.position,
            'end': self.position + np.array([0, 0, -self.forces['gravity'] * 0.1])
        }
        
        if self.control_mode == 'AUTO':
            vectors['target'] = {
                'start': self.position,
                'end': self.target_position
            }
        
        return vectors
    
    def get_propeller_data(self):
        """Возвращает данные о пропеллерах для сохранения"""
        return {
            'thrusts': self.propeller_thrusts.tolist(),
            'speeds': self.propeller_speeds.tolist(),
            'efficiencies': self.propeller_efficiency.tolist()
        }
    
    def get_area_boundaries(self):
        """Возвращает границы области для визуализации"""
        return self.area_limits
    
    def reset(self):
        """Сбрасывает дрон в начальное состояние"""
        # Останавливаем запись текущей сессии
        if self.current_session_id:
            self.stop_data_logging()
        
        self.position = np.array([0.0, 0.0, 3.0])
        self.orientation = np.array([0.0, 0.0, 0.0])
        self.velocity = np.array([0.0, 0.0, 0.0])
        self.angular_velocity = np.array([0.0, 0.0, 0.0])
        self.control_input = {'thrust': 9.81, 'pitch': 0.0, 'roll': 0.0, 'yaw': 0.0}
        self.propeller_thrusts = np.array([2.4525, 2.4525, 2.4525, 2.4525])
        self.propeller_speeds = np.array([1000.0, 1000.0, 1000.0, 1000.0])
        self.flight_time = 0.0
        self.distance_traveled = 0.0
        self.last_position = self.position.copy()
        self.max_altitude = 0.0
        self.max_speed = 0.0
        self.target_counter = 0
        
        # Начинаем новую сессию записи
        self.start_data_logging()

# Дополнительный класс для тестирования
class DroneTest:
    def __init__(self):
        self.drone = ControllableDrone()
        self.database = DroneDatabase()
    
    def run_test_flight(self, duration=10):
        """Запускает тестовый полёт"""
        print("🧪 ЗАПУСК ТЕСТОВОГО ПОЛЁТА")
        print("=" * 40)
        
        # Начинаем сессию
        session_id = self.database.start_new_flight_session()
        
        # Имитируем полёт
        start_time = datetime.now()
        dt = 0.1
        
        for i in range(int(duration / dt)):
            # Случайное управление для теста
            thrust_change = random.uniform(-0.5, 0.5)
            pitch = random.uniform(-0.2, 0.2)
            roll = random.uniform(-0.2, 0.2)
            yaw = random.uniform(-0.1, 0.1)
            
            self.drone.set_control_input(thrust_change, pitch, roll, yaw)
            self.drone.apply_control()
            self.drone.update_physics(dt)
            
            # Сохраняем состояние каждые 0.5 секунды
            if i % 5 == 0:
                self.database.save_drone_position(session_id, self.drone)
        
        # Завершаем сессию
        self.database.end_flight_session(
            session_id,
            self.drone.flight_time,
            self.drone.distance_traveled,
            self.drone.max_altitude,
            self.drone.max_speed
        )
        
        print("✅ Тестовый полёт завершён")
        print(f"📊 Сессия #{session_id} сохранена")
        
        return session_id

if __name__ == "__main__":
    # Тестирование дрона
    test = DroneTest()
    test_session = test.run_test_flight(5)
    
    print(f"\n🎯 Тест завершён. Данные сохранены в сессии #{test_session}")
    print("💡 Запустите flight_data_viewer.py для просмотра данных")