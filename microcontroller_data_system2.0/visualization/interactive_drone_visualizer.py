import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
import matplotlib.animation as animation
from controllable_drone_model import ControllableDrone
import random
from datetime import datetime

class InteractiveDroneVisualizer:
    def __init__(self):
        self.drone = ControllableDrone()
        self.fig = plt.figure(figsize=(16, 10))
        self.ax = self.fig.add_subplot(111, projection='3d')
        
        # Текущие управляющие сигналы
        self.current_controls = {
            'thrust_change': 0,
            'pitch': 0,
            'roll': 0, 
            'yaw': 0
        }
        
        # Флаг записи данных
        self.is_recording = False
        
        # Настройка графика
        self.setup_plot()
        
        # Подключаем обработчики клавиш
        self.fig.canvas.mpl_connect('key_press_event', self.on_key_press)
        self.fig.canvas.mpl_connect('key_release_event', self.on_key_release)
        
        # Для анимации
        self.animation = None
        
        # Автоматически начинаем запись при запуске
        self.start_recording()
        
    def setup_plot(self):
        """Настраивает внешний вид графика"""
        self.ax.set_xlabel('X (м)')
        self.ax.set_ylabel('Y (м)')
        self.ax.set_zlabel('Z (м)')
        self.ax.set_title('🎮 ИНТЕРАКТИВНОЕ УПРАВЛЕНИЕ ДРОНОМ - НАЖМИТЕ H ДЛЯ СПРАВКИ')
        
        # Устанавливаем пределы осей согласно области дрона
        area = self.drone.get_area_boundaries()
        self.ax.set_xlim(area['x_min'] - 1, area['x_max'] + 1)
        self.ax.set_ylim(area['y_min'] - 1, area['y_max'] + 1)
        self.ax.set_zlim(area['z_min'] - 1, area['z_max'] + 1)
        
        # Добавляем сетку
        self.ax.grid(True)
        
        # Добавляем визуализацию границ области
        self.draw_area_boundaries()
    
    def draw_area_boundaries(self):
        """Отрисовывает границы области перемещения"""
        area = self.drone.get_area_boundaries()
        
        # Создаём прозрачный куб для визуализации границ
        x = [area['x_min'], area['x_max']]
        y = [area['y_min'], area['y_max']]
        z = [area['z_min'], area['z_max']]
        
        # Рёбра куба
        from itertools import product
        for s, e in product(np.array(list(product(x, y, z))), repeat=2):
            if np.sum(np.abs(s-e)) == (x[1]-x[0]) or np.sum(np.abs(s-e)) == (y[1]-y[0]) or np.sum(np.abs(s-e)) == (z[1]-z[0]):
                self.ax.plot3D(*zip(s, e), color="gray", alpha=0.3, linewidth=1)
        
        # Пол прозрачный
        xx, yy = np.meshgrid([area['x_min'], area['x_max']], [area['y_min'], area['y_max']])
        zz = np.ones_like(xx) * area['z_min']
        self.ax.plot_surface(xx, yy, zz, alpha=0.1, color='green')
        
        # Потолок прозрачный
        zz_ceil = np.ones_like(xx) * area['z_max']
        self.ax.plot_surface(xx, yy, zz_ceil, alpha=0.1, color='blue')
    
    def start_recording(self):
        """Начинает запись данных полёта"""
        if not self.is_recording:
            self.drone.start_data_logging()
            self.is_recording = True
            print("🔴 ЗАПИСЬ ДАННЫХ НАЧАТА")
    
    def stop_recording(self):
        """Останавливает запись данных полёта"""
        if self.is_recording:
            self.drone.stop_data_logging()
            self.is_recording = False
            print("⏹️ ЗАПИСЬ ДАННЫХ ОСТАНОВЛЕНА")
    
    def on_key_press(self, event):
        """Обработчик нажатия клавиш"""
        key = event.key.lower()
        
        # Управление тягой
        if key == 'w':
            self.current_controls['thrust_change'] = 1
        elif key == 's':
            self.current_controls['thrust_change'] = -1
            
        # Управление наклоном
        elif key == 'up':
            self.current_controls['pitch'] = 0.5
        elif key == 'down':
            self.current_controls['pitch'] = -0.5
        elif key == 'left':
            self.current_controls['roll'] = -0.5
        elif key == 'right':
            self.current_controls['roll'] = 0.5
            
        # Управление поворотом
        elif key == 'a':
            self.current_controls['yaw'] = -0.3
        elif key == 'd':
            self.current_controls['yaw'] = 0.3
            
        # Специальные команды
        elif key == ' ':
            self.current_controls['thrust_change'] = 2  # Резкий взлёт
        elif key == 'r':
            self.drone.reset()
            print("🔄 Дрон сброшен в начальное положение")
        elif key == 'm':
            self.drone.toggle_control_mode()
            mode = "АВТОПИЛОТ" if self.drone.control_mode == 'AUTO' else "РУЧНОЕ УПРАВЛЕНИЕ"
            print(f"🔄 Режим управления: {mode}")
        elif key == 'h':
            self.show_help()
        elif key == 'f1':
            self.start_recording()
        elif key == 'f2':
            self.stop_recording()
    
    def on_key_release(self, event):
        """Обработчик отпускания клавиш"""
        key = event.key.lower()
        
        # Сбрасываем управление при отпускании
        if key in ['w', 's', ' ']:
            self.current_controls['thrust_change'] = 0
        elif key in ['up', 'down']:
            self.current_controls['pitch'] = 0
        elif key in ['left', 'right']:
            self.current_controls['roll'] = 0
        elif key in ['a', 'd']:
            self.current_controls['yaw'] = 0
    
    def show_help(self):
        """Показывает справку по управлению"""
        help_text = """
        🎮 УПРАВЛЕНИЕ ДРОНОМ:
        
        🚀 ТЯГА:
          W - Увеличить тягу (взлёт)
          S - Уменьшить тягу (посадка)
          ПРОБЕЛ - Резкий взлёт
        
        📐 НАКЛОН:
          СТРЕЛКА ВВЕРХ - Наклон вперёд
          СТРЕЛКА ВНИЗ - Наклон назад  
          СТРЕЛКА ВЛЕВО - Наклон влево
          СТРЕЛКА ВПРАВО - Наклон вправо
        
        🌀 ПОВОРОТ:
          A - Поворот против часовой
          D - Поворот по часовой
        
        ⚙️ СИСТЕМА:
          R - Сброс дрона (начинает новую запись)
          M - Переключить режим (ручной/автопилот)
          F1 - Начать запись данных
          F2 - Остановить запись данных
          H - Показать справку
        
        📊 РЕЖИМЫ:
          РУЧНОЙ - Полный контроль 4 пропеллеров
          АВТОПИЛОТ - Дрон сам летает к целям
        
        💾 ЗАПИСЬ ДАННЫХ:
          Данные автоматически сохраняются при запуске
          Каждая позиция сохраняется каждые 0.2 секунды
          Сохраняются данные всех 4 пропеллеров
          При сбросе (R) начинается новая сессия записи
        """
        print(help_text)
    
    def update_drone(self, frame):
        """Обновляет состояние дрона для анимации"""
        # Применяем управляющие сигналы
        self.drone.set_control_input(
            self.current_controls['thrust_change'],
            self.current_controls['pitch'], 
            self.current_controls['roll'],
            self.current_controls['yaw']
        )
        
        # Применяем управление к дрону
        self.drone.apply_control()
        
        # Обновляем физику
        self.drone.update_physics(0.05)  # dt = 0.05 секунды
        
        return self.draw_drone()
    
    def draw_drone(self):
        """Отрисовывает дрон и векторы сил"""
        self.ax.clear()
        self.setup_plot()
        
        # Получаем преобразованную геометрию
        geometry = self.drone.get_transformed_geometry()
        force_vectors = self.drone.get_force_vectors()
        
        # Отрисовываем body (центр дрона)
        self.ax.scatter(geometry['body'][:, 0], geometry['body'][:, 1], geometry['body'][:, 2], 
                       c='red', s=50, label='Центр дрона')
        
        # Отрисовываем arms (лучи)
        for arm in geometry['arms']:
            self.ax.plot(arm[:, 0], arm[:, 1], arm[:, 2], 'b-', linewidth=3, alpha=0.7)
        
        # Отрисовываем motors (моторы)
        self.ax.scatter(geometry['motors'][:, 0], geometry['motors'][:, 1], geometry['motors'][:, 2], 
                       c='black', s=100, marker='s', label='Моторы')
        
        # Отрисовываем пропеллеры с индивидуальными цветами по тяге
        for propeller_data in geometry['propellers']:
            thrust_ratio = propeller_data['thrust'] / self.drone.propeller_max_thrust
            # Цвет от синего (малая тяга) к красному (максимальная тяга)
            color = (thrust_ratio, 0.2, 1.0 - thrust_ratio, 0.8)
            
            points = propeller_data['points']
            self.ax.plot(points[:, 0], points[:, 1], points[:, 2], 
                        color=color, linewidth=3, alpha=0.8)
            # Замыкаем пропеллер
            self.ax.plot([points[-1, 0], points[0, 0]], 
                        [points[-1, 1], points[0, 1]], 
                        [points[-1, 2], points[0, 2]], 
                        color=color, linewidth=3, alpha=0.8)
        
        # Отрисовываем целевую позицию в режиме автопилота
        if self.drone.control_mode == 'AUTO':
            target = self.drone.target_position
            self.ax.scatter(target[0], target[1], target[2], 
                           c='yellow', s=200, marker='*', label='Цель', alpha=0.7)
        
        # Отрисовываем векторы сил
        self.draw_force_vectors(force_vectors)
        
        # Добавляем информацию о состоянии
        self.add_status_info()
        
        # Добавляем информацию об управлении
        self.add_control_info()
        
        # Добавляем информацию о пропеллерах
        self.add_propeller_info()
        
        return []
    
    def draw_force_vectors(self, vectors):
        """Отрисовывает векторы сил"""
        # Вектор тяги (красный)
        thrust = vectors['thrust']
        self.ax.quiver(thrust['start'][0], thrust['start'][1], thrust['start'][2],
                      thrust['end'][0] - thrust['start'][0],
                      thrust['end'][1] - thrust['start'][1],
                      thrust['end'][2] - thrust['start'][2],
                      color='red', linewidth=3, arrow_length_ratio=0.1, label='Суммарная тяга')
        
        # Векторы индивидуальной тяги пропеллеров
        if 'propeller_thrusts' in vectors:
            for prop_thrust in vectors['propeller_thrusts']:
                thrust_ratio = prop_thrust['thrust'] / self.drone.propeller_max_thrust
                color = (thrust_ratio, 0.2, 1.0 - thrust_ratio, 0.6)
                
                self.ax.quiver(prop_thrust['start'][0], prop_thrust['start'][1], prop_thrust['start'][2],
                             0, 0, prop_thrust['end'][2] - prop_thrust['start'][2],
                             color=color, linewidth=2, arrow_length_ratio=0.2)
        
        # Вектор скорости (синий)
        velocity = vectors['velocity']
        if np.linalg.norm(self.drone.velocity) > 0.1:
            self.ax.quiver(velocity['start'][0], velocity['start'][1], velocity['start'][2],
                          velocity['end'][0] - velocity['start'][0],
                          velocity['end'][1] - velocity['start'][1],
                          velocity['end'][2] - velocity['start'][2],
                          color='blue', linewidth=2, arrow_length_ratio=0.1, label='Скорость')
        
        # Вектор гравитации (зелёный)
        gravity = vectors['gravity']
        self.ax.quiver(gravity['start'][0], gravity['start'][1], gravity['start'][2],
                      gravity['end'][0] - gravity['start'][0],
                      gravity['end'][1] - gravity['start'][1],
                      gravity['end'][2] - gravity['start'][2],
                      color='green', linewidth=2, arrow_length_ratio=0.1, label='Гравитация')
        
        # Вектор к цели в режиме автопилота
        if self.drone.control_mode == 'AUTO' and 'target' in vectors:
            target = vectors['target']
            self.ax.quiver(target['start'][0], target['start'][1], target['start'][2],
                          target['end'][0] - target['start'][0],
                          target['end'][1] - target['start'][1],
                          target['end'][2] - target['start'][2],
                          color='purple', linewidth=2, arrow_length_ratio=0.1, 
                          label='Направление к цели', alpha=0.5)
    
    def add_status_info(self):
        """Добавляет информацию о состоянии дрона"""
        pos = self.drone.position
        vel = self.drone.velocity
        orientation = np.degrees(self.drone.orientation)
        thrust = self.drone.forces['thrust']
        
        info_text = f"""📊 СОСТОЯНИЕ ДРОНА:
Позиция: X={pos[0]:.1f}м, Y={pos[1]:.1f}м, Z={pos[2]:.1f}м
Скорость: {np.linalg.norm(vel):.1f} м/с
Ориентация: Крен={orientation[0]:.1f}°, Тангаж={orientation[1]:.1f}°, Рыскание={orientation[2]:.1f}°
Суммарная тяга: {thrust:.1f} Н
Режим: {self.drone.control_mode}
"""
        
        self.ax.text2D(0.02, 0.98, info_text, transform=self.ax.transAxes, 
                      bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.9),
                      verticalalignment='top', fontsize=9)
    
    def add_control_info(self):
        """Добавляет информацию об управлении"""
        controls = self.current_controls
        
        control_text = f"""🎮 ТЕКУЩЕЕ УПРАВЛЕНИЕ:
Тяга: {'↑' if controls['thrust_change'] > 0 else '↓' if controls['thrust_change'] < 0 else '●'}
Наклон: {'↖' if controls['roll'] < 0 else '↗' if controls['roll'] > 0 else '●'} {'↑' if controls['pitch'] > 0 else '↓' if controls['pitch'] < 0 else '●'}
Поворот: {'↶' if controls['yaw'] < 0 else '↷' if controls['yaw'] > 0 else '●'}
"""
        
        self.ax.text2D(0.02, 0.15, control_text, transform=self.ax.transAxes, 
                      bbox=dict(boxstyle="round,pad=0.3", facecolor="lightblue", alpha=0.8),
                      verticalalignment='top', fontsize=9)
        
        # Добавляем статистику
        stats_text = f"""📈 СТАТИСТИКА:
Время полёта: {self.drone.flight_time:.1f} сек
Пройдено: {self.drone.distance_traveled:.1f} м
Макс. высота: {self.drone.max_altitude:.1f} м
Макс. скорость: {self.drone.max_speed:.1f} м/с
"""
        
        self.ax.text2D(0.65, 0.15, stats_text, transform=self.ax.transAxes, 
                      bbox=dict(boxstyle="round,pad=0.3", facecolor="lightgreen", alpha=0.8),
                      verticalalignment='top', fontsize=9)
    
    def add_propeller_info(self):
        """Добавляет информацию о пропеллерах"""
        propeller_text = "🚁 ДАННЫЕ ПРОПЕЛЛЕРОВ:\n"
        for i in range(4):
            thrust = self.drone.propeller_thrusts[i]
            speed = self.drone.propeller_speeds[i]
            efficiency = self.drone.propeller_efficiency[i] * 100
            propeller_text += f"П{i+1}: {thrust:.2f}Н, {speed:.0f} RPM, {efficiency:.0f}%\n"
        
        self.ax.text2D(0.02, 0.02, propeller_text, transform=self.ax.transAxes, 
                      bbox=dict(boxstyle="round,pad=0.3", facecolor="lightyellow", alpha=0.8),
                      verticalalignment='bottom', fontsize=8)
        
        # Добавляем информацию о записи
        recording_text = f"""💾 ЗАПИСЬ ДАННЫХ:
Статус: {'🔴 ВКЛ' if self.is_recording else '⏹️ ВЫКЛ'}
Сессия: #{self.drone.current_session_id if self.drone.current_session_id else 'Нет'}
"""
        
        self.ax.text2D(0.65, 0.02, recording_text, transform=self.ax.transAxes, 
                      bbox=dict(boxstyle="round,pad=0.3", 
                              facecolor="red" if self.is_recording else "gray", 
                              alpha=0.8),
                      verticalalignment='bottom', fontsize=9, color="white")
    
    def start_animation(self):
        """Запускает анимацию"""
        self.animation = animation.FuncAnimation(
            self.fig, self.update_drone, frames=None, 
            interval=50, blit=False, repeat=True
        )
        
        # Добавляем легенду
        self.ax.legend(loc='upper right', bbox_to_anchor=(1, 1))
        
        plt.tight_layout()
        plt.show()

# Запуск визуализации
if __name__ == "__main__":
    print("🎮 ЗАПУСК ИНТЕРАКТИВНОГО УПРАВЛЕНИЯ ДРОНОМ")
    print("🚁 Модель с 4 независимыми пропеллерами")
    print("💾 Автоматическое сохранение данных ВКЛЮЧЕНО")
    print("⏳ Инициализация...")
    print("✅ Готово! Нажмите H для справки по управлению")
    
    visualizer = InteractiveDroneVisualizer()
    visualizer.start_animation()