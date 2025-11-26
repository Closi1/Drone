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
        
        # Скорости вращения (гироскоп)
        self.angular_velocity = np.array([0.0, 0.0, 0.0])
        
        # Линейная скорость (акселерометр)
        self.velocity = np.array([0.0, 0.0, 0.0])
        
        # Линейное ускорение (акселерометр)
        self.linear_acceleration = np.array([0.0, 0.0, 0.0])
        
        # Управляющие сигналы от пользователя
        self.control_input = {
            'thrust': 9.81,     # Базовая тяга (компенсирует гравитацию)
            'pitch': 0.0,       # Наклон вперед/назад
            'roll': 0.0,        # Наклон влево/вправо  
            'yaw': 0.0          # Поворот
        }
        
        # Индивидуальная тяга каждого пропеллера
        self.propeller_thrusts = np.array([2.4525, 2.4525, 2.4525, 2.4525])
        
        # Параметры пропеллеров
        self.propeller_speeds = np.array([1000.0, 1000.0, 1000.0, 1000.0])
        self.propeller_max_thrust = 6.0
        self.propeller_efficiency = np.array([0.95, 0.97, 0.96, 0.94])
        
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
        self.control_mode = 'MANUAL'
        
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
        self.save_interval = 0.2
        
        # Параметры датчиков
        self.sensor_noise = {
            'gyro': 0.01,      # Шум гироскопа (рад/с)
            'accel': 0.1,      # Шум акселерометра (м/с²)
            'bias_gyro': np.array([0.002, 0.001, -0.003]),  # Смещение гироскопа
            'bias_accel': np.array([0.05, -0.03, 0.02])     # Смещение акселерометра
        }
        
        # Калибровочные параметры
        self.sensor_calibration = {
            'gyro_scale': np.array([1.02, 0.98, 1.01]),  # Масштабные коэффициенты
            'accel_scale': np.array([0.99, 1.01, 1.03])
        }
        
    def get_gyroscope_data(self):
        """Возвращает данные гироскопа с шумом и смещением"""
        # Идеальные данные (угловая скорость в рад/с)
        ideal_gyro = self.angular_velocity.copy()
        
        # Добавляем смещение
        gyro_with_bias = ideal_gyro + self.sensor_noise['bias_gyro']
        
        # Применяем масштабные коэффициенты
        gyro_scaled = gyro_with_bias * self.sensor_calibration['gyro_scale']
        
        # Добавляем случайный шум
        noise = np.random.normal(0, self.sensor_noise['gyro'], 3)
        gyro_noisy = gyro_scaled + noise
        
        return {
            'roll_rate': float(gyro_noisy[0]),   # Скорость крена (рад/с)
            'pitch_rate': float(gyro_noisy[1]),  # Скорость тангажа (рад/с)
            'yaw_rate': float(gyro_noisy[2]),    # Скорость рыскания (рад/с)
            'timestamp': datetime.now(),
            'temperature': 25.0 + random.uniform(-2, 2)  # Температура датчика
        }
    
    def get_accelerometer_data(self):
        """Возвращает данные акселерометра с шумом и смещением"""
        # Идеальные данные (ускорение в м/с²)
        # Акселерометр измеряет ускорение + гравитацию
        gravity_vector = np.array([0, 0, -9.81])
        
        # Преобразуем гравитацию в систему координат дрона
        roll, pitch, yaw = self.orientation
        rotation_matrix = self._get_rotation_matrix(roll, pitch, yaw)
        gravity_body = rotation_matrix.T @ gravity_vector
        
        # Общее ускорение в системе координат дрона
        ideal_accel = self.linear_acceleration + gravity_body
        
        # Добавляем смещение
        accel_with_bias = ideal_accel + self.sensor_noise['bias_accel']
        
        # Применяем масштабные коэффициенты
        accel_scaled = accel_with_bias * self.sensor_calibration['accel_scale']
        
        # Добавляем случайный шум
        noise = np.random.normal(0, self.sensor_noise['accel'], 3)
        accel_noisy = accel_scaled + noise
        
        return {
            'accel_x': float(accel_noisy[0]),    # Ускорение по X (м/с²)
            'accel_y': float(accel_noisy[1]),    # Ускорение по Y (м/с²)
            'accel_z': float(accel_noisy[2]),    # Ускорение по Z (м/с²)
            'timestamp': datetime.now(),
            'temperature': 25.0 + random.uniform(-2, 2),
            'vibration_level': random.uniform(0.1, 0.5)  # Уровень вибрации
        }
    
    def get_imu_data(self):
        """Возвращает полные данные IMU (Inertial Measurement Unit)"""
        gyro_data = self.get_gyroscope_data()
        accel_data = self.get_accelerometer_data()
        
        return {
            'gyroscope': gyro_data,
            'accelerometer': accel_data,
            'timestamp': datetime.now(),
            'orientation_estimate': self._estimate_orientation_from_imu(),
            'motion_detected': np.linalg.norm(self.velocity) > 0.1 or np.linalg.norm(self.angular_velocity) > 0.1
        }
    
    def _estimate_orientation_from_imu(self):
        """Простая оценка ориентации по данным акселерометра"""
        accel_data = self.get_accelerometer_data()
        accel = np.array([accel_data['accel_x'], accel_data['accel_y'], accel_data['accel_z']])
        
        # Оценка углов по акселерометру (только крен и тангаж)
        pitch_est = math.atan2(-accel[0], math.sqrt(accel[1]**2 + accel[2]**2))
        roll_est = math.atan2(accel[1], accel[2])
        
        return {
            'roll_estimated': float(roll_est),
            'pitch_estimated': float(pitch_est),
            'yaw_estimated': float(self.orientation[2]),  # Рыскание нельзя определить по акселерометру
            'confidence': 0.8 if np.linalg.norm(accel) > 8 and np.linalg.norm(accel) < 12 else 0.3
        }
    
    def _get_rotation_matrix(self, roll, pitch, yaw):
        """Возвращает матрицу поворота для заданных углов Эйлера"""
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
        
        return Rz @ Ry @ Rx
    
    def calibrate_sensors(self):
        """Калибровка датчиков (сбрасывает смещения)"""
        print("🎯 Калибровка датчиков...")
        
        # В реальной системе здесь была бы процедура калибровки
        # Для симуляции просто уменьшаем смещения
        self.sensor_noise['bias_gyro'] = np.random.normal(0, 0.001, 3)
        self.sensor_noise['bias_accel'] = np.random.normal(0, 0.01, 3)
        
        print("✅ Датчики откалиброваны")
    
    def update_physics(self, dt):
        """Обновляет физику дрона"""
        # Сохраняем предыдущее состояние для расчёта ускорения
        previous_velocity = self.velocity.copy()
        
        # Обновляем ориентацию (интегрируем гироскоп)
        self.orientation += self.angular_velocity * dt
        
        # Ограничиваем углы
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
        
        # Сила сопротивления
        drag_force = -self.velocity * 0.2
        
        # Суммарная сила
        total_force = thrust_vector + gravity_force + drag_force + self.forces['wind']
        
        # Ускорение (F = ma, m = 1)
        acceleration = total_force
        self.linear_acceleration = acceleration  # Сохраняем для акселерометра
        
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
        
        # Сохраняем данные в базу (включая данные датчиков)
        self.save_current_state()
        
        # Обновляем время
        self.last_update = datetime.now()

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
            random.uniform(-6.0, 6.0),
            random.uniform(-6.0, 6.0),
            random.uniform(2.0, 10.0)
        ])
    
    def _create_quadcopter_geometry(self):
        """Создаёт геометрию квадрокоптера с 4 пропеллерами"""
        geometry = {
            'body': np.array([
                [0, 0, 0],
                [0.2, 0, 0],
                [-0.2, 0, 0],
                [0, 0.2, 0],
                [0, -0.2, 0],
            ]),
            'arms': [
                np.array([[0.2, 0, 0], [0.5, 0, 0]]),
                np.array([[-0.2, 0, 0], [-0.5, 0, 0]]),
                np.array([[0, 0.2, 0], [0, 0.5, 0]]),
                np.array([[0, -0.2, 0], [0, -0.5, 0]]),
            ],
            'motors': [
                np.array([0.5, 0, 0]),
                np.array([-0.5, 0, 0]),
                np.array([0, 0.5, 0]),
                np.array([0, -0.5, 0]),
            ],
            'propellers': []
        }
        
        for i, motor_pos in enumerate(geometry['motors']):
            propeller = self._create_propeller_geometry(motor_pos, i)
            geometry['propellers'].append(propeller)
        
        return geometry
    
    def _create_propeller_geometry(self, center, propeller_id):
        """Создаёт геометрию пропеллера с индивидуальными параметрами"""
        points = []
        radius = 0.15
        num_points = 8
        
        size_variation = [1.0, 0.95, 1.05, 0.98]
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
        base_thrust = total_thrust / 4.0
        roll_correction = roll * 0.5
        pitch_correction = pitch * 0.5
        yaw_correction = yaw * 0.3
        
        thrusts = np.array([
            base_thrust - roll_correction + pitch_correction - yaw_correction,
            base_thrust + roll_correction - pitch_correction - yaw_correction,
            base_thrust + roll_correction + pitch_correction + yaw_correction,
            base_thrust - roll_correction - pitch_correction + yaw_correction
        ])
        
        thrusts = thrusts * self.propeller_efficiency
        thrusts = np.clip(thrusts, 0.1, self.propeller_max_thrust)
        
        return thrusts
    
    def update_propeller_speeds(self):
        """Обновляет скорости вращения пропеллеров на основе тяги"""
        for i in range(4):
            thrust_ratio = self.propeller_thrusts[i] / self.propeller_max_thrust
            self.propeller_speeds[i] = 500.0 + 1500.0 * math.sqrt(thrust_ratio)
    
    def set_control_input(self, thrust_change=0, pitch=0, roll=0, yaw=0):
        """Устанавливает управляющие сигналы"""
        self.control_input['thrust'] = 9.81 + thrust_change * 2.0
        self.control_input['thrust'] = np.clip(self.control_input['thrust'], 5.0, 20.0)
        self.control_input['pitch'] = np.clip(pitch, -0.5, 0.5)
        self.control_input['roll'] = np.clip(roll, -0.5, 0.5)
        self.control_input['yaw'] = np.clip(yaw, -0.3, 0.3)
        
        self.propeller_thrusts = self.calculate_propeller_thrusts(
            self.control_input['thrust'],
            self.control_input['roll'],
            self.control_input['pitch'],
            self.control_input['yaw']
        )
        
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
        total_force_magnitude = np.linalg.norm(self.forces['thrust'])
        
        for i, arm in enumerate(self.geometry['arms']):
            propeller_force = self.propeller_thrusts[i]
            deformation = propeller_force * 0.02 * math.sin(datetime.now().timestamp() * 12 + i)
            
            deformed_arm = arm.copy()
            deformed_arm[1][2] += deformation
            self.geometry['arms'][i] = deformed_arm
            
            self.geometry['motors'][i] = deformed_arm[1].copy()
            
            motor_pos = deformed_arm[1].copy()
            self.geometry['propellers'][i] = self._create_propeller_geometry(motor_pos, i)
    
    def auto_pilot(self):
        """Автопилот для движения к цели"""
        if self.control_mode != 'AUTO':
            return self.control_input['thrust'], 0, 0, 0
        
        to_target = self.target_position - self.position
        distance_to_target = np.linalg.norm(to_target)
        
        if distance_to_target < 1.0:
            self.target_position = self.generate_random_target()
            to_target = self.target_position - self.position
            distance_to_target = np.linalg.norm(to_target)
            self.target_counter += 1
        
        if distance_to_target > 0.1:
            direction = to_target / distance_to_target
        else:
            direction = np.array([0.0, 0.0, 0.0])
        
        target_pitch = -direction[0] * 0.8
        target_roll = direction[1] * 0.8
        target_yaw = 0.0
        
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
        
        self.angular_velocity = np.array([roll, pitch, yaw]) * 0.8
        self.forces['thrust'] = thrust
        
        self.propeller_thrusts = self.calculate_propeller_thrusts(thrust, roll, pitch, yaw)
        self.update_propeller_speeds()
        
        if random.random() < 0.2:
            self.forces['wind'] = np.array([
                random.uniform(-0.3, 0.3),
                random.uniform(-0.3, 0.3),
                random.uniform(-0.1, 0.1)
            ])
        else:
            self.forces['wind'] = np.array([0.0, 0.0, 0.0])
        
        self.update_geometry_based_on_forces()
    
    def enforce_area_limits(self):
        """Обеспечивает нахождение дрона в пределах области"""
        limits = self.area_limits
        
        self.position[0] = np.clip(self.position[0], limits['x_min'], limits['x_max'])
        self.position[1] = np.clip(self.position[1], limits['y_min'], limits['y_max']) 
        self.position[2] = np.clip(self.position[2], limits['z_min'], limits['z_max'])
        
        border_margin = 1.0
        repulsion_force = 0.0
        
        if self.position[0] > limits['x_max'] - border_margin:
            repulsion_force = (limits['x_max'] - self.position[0] - border_margin) * 3.0
            self.velocity[0] -= repulsion_force
        elif self.position[0] < limits['x_min'] + border_margin:
            repulsion_force = (self.position[0] - limits['x_min'] - border_margin) * 3.0
            self.velocity[0] += repulsion_force
            
        if self.position[1] > limits['y_max'] - border_margin:
            repulsion_force = (limits['y_max'] - self.position[1] - border_margin) * 3.0
            self.velocity[1] -= repulsion_force
        elif self.position[1] < limits['y_min'] + border_margin:
            repulsion_force = (self.position[1] - limits['y_min'] - border_margin) * 3.0
            self.velocity[1] += repulsion_force
            
        if self.position[2] > limits['z_max'] - border_margin:
            repulsion_force = (limits['z_max'] - self.position[2] - border_margin) * 3.0
            self.velocity[2] -= repulsion_force
        elif self.position[2] < limits['z_min'] + border_margin:
            repulsion_force = (self.position[2] - limits['z_min'] - border_margin) * 3.0
            self.velocity[2] += repulsion_force
    
    def get_transformed_geometry(self):
        """Возвращает геометрию дрона с учётом позиции и ориентации"""
        transformed = {}
        
        roll, pitch, yaw = self.orientation
        rotation_matrix = self._get_rotation_matrix(roll, pitch, yaw)
        
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
        
        vectors['propeller_thrusts'] = []
        geometry = self.get_transformed_geometry()
        for propeller_data in geometry['propellers']:
            thrust_magnitude = propeller_data['thrust'] * 0.05
            thrust_end = propeller_data['center'] + np.array([0, 0, thrust_magnitude])
            vectors['propeller_thrusts'].append({
                'start': propeller_data['center'],
                'end': thrust_end,
                'thrust': propeller_data['thrust'],
                'propeller_id': propeller_data['id']
            })
        
        vectors['velocity'] = {
            'start': self.position,
            'end': self.position + self.velocity * 0.5
        }
        
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
        if self.current_session_id:
            self.stop_data_logging()
        
        self.position = np.array([0.0, 0.0, 3.0])
        self.orientation = np.array([0.0, 0.0, 0.0])
        self.velocity = np.array([0.0, 0.0, 0.0])
        self.angular_velocity = np.array([0.0, 0.0, 0.0])
        self.linear_acceleration = np.array([0.0, 0.0, 0.0])
        self.control_input = {'thrust': 9.81, 'pitch': 0.0, 'roll': 0.0, 'yaw': 0.0}
        self.propeller_thrusts = np.array([2.4525, 2.4525, 2.4525, 2.4525])
        self.propeller_speeds = np.array([1000.0, 1000.0, 1000.0, 1000.0])
        self.flight_time = 0.0
        self.distance_traveled = 0.0
        self.last_position = self.position.copy()
        self.max_altitude = 0.0
        self.max_speed = 0.0
        self.target_counter = 0
        
        # Автоматическая калибровка при сбросе
        self.calibrate_sensors()
        
        self.start_data_logging()