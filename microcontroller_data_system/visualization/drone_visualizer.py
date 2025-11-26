import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import numpy as np
import matplotlib.animation as animation
from drone_model import Drone
import random
from datetime import datetime

class DroneVisualizer:
    def __init__(self):
        self.drone = Drone()
        self.fig = plt.figure(figsize=(14, 10))
        self.ax = self.fig.add_subplot(111, projection='3d')
        
        # Настройка графика
        self.setup_plot()
        
        # Для анимации
        self.animation = None
        
    def setup_plot(self):
        """Настраивает внешний вид графика"""
        self.ax.set_xlabel('X (м)')
        self.ax.set_ylabel('Y (м)')
        self.ax.set_zlabel('Z (м)')
        self.ax.set_title('🚁 3D ВИЗУАЛИЗАЦИЯ ДРОНА - ОГРАНИЧЕННАЯ ОБЛАСТЬ ПЕРЕМЕЩЕНИЯ')
        
        # Устанавливаем пределы осей согласно области дрона
        area = self.drone.get_area_boundaries()
        self.ax.set_xlim(area['x_min'] - 0.5, area['x_max'] + 0.5)
        self.ax.set_ylim(area['y_min'] - 0.5, area['y_max'] + 0.5)
        self.ax.set_zlim(area['z_min'] - 0.5, area['z_max'] + 0.5)
        
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
    
    def update_drone(self, frame):
        """Обновляет состояние дрона для анимации"""
        # Вычисляем управление для движения к цели
        thrust, roll, pitch, yaw = self.drone.calculate_control_to_target()
        
        # Применяем управление
        self.drone.apply_control_input(thrust, roll, pitch, yaw)
        
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
        
        # Отрисовываем propellers (пропеллеры)
        colors = ['red', 'blue', 'green', 'orange']
        for i, propeller in enumerate(geometry['propellers']):
            self.ax.plot(propeller[:, 0], propeller[:, 1], propeller[:, 2], 
                        c=colors[i], linewidth=2, alpha=0.6)
            # Замыкаем пропеллер
            self.ax.plot([propeller[-1, 0], propeller[0, 0]], 
                        [propeller[-1, 1], propeller[0, 1]], 
                        [propeller[-1, 2], propeller[0, 2]], 
                        c=colors[i], linewidth=2, alpha=0.6)
        
        # Отрисовываем целевую позицию
        target = self.drone.target_position
        self.ax.scatter(target[0], target[1], target[2], 
                       c='yellow', s=200, marker='*', label='Цель', alpha=0.7)
        
        # Отрисовываем векторы сил
        self.draw_force_vectors(force_vectors)
        
        # Добавляем информацию о состоянии
        self.add_status_info()
        
        return []
    
    def draw_force_vectors(self, vectors):
        """Отрисовывает векторы сил"""
        # Вектор тяги (красный)
        thrust = vectors['thrust']
        self.ax.quiver(thrust['start'][0], thrust['start'][1], thrust['start'][2],
                      thrust['end'][0] - thrust['start'][0],
                      thrust['end'][1] - thrust['start'][1],
                      thrust['end'][2] - thrust['start'][2],
                      color='red', linewidth=3, arrow_length_ratio=0.1, label='Тяга')
        
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
        
        # Вектор к цели (фиолетовый)
        target = vectors['target']
        self.ax.quiver(target['start'][0], target['start'][1], target['start'][2],
                      target['end'][0] - target['start'][0],
                      target['end'][1] - target['start'][1],
                      target['end'][2] - target['start'][2],
                      color='purple', linewidth=2, arrow_length_ratio=0.1, label='Направление к цели', alpha=0.5)
        
        # Вектор ветра (оранжевый)
        if np.linalg.norm(self.drone.forces['wind']) > 0.1:
            wind_start = self.drone.position
            wind_end = self.drone.position + self.drone.forces['wind'] * 0.3
            self.ax.quiver(wind_start[0], wind_start[1], wind_start[2],
                          wind_end[0] - wind_start[0],
                          wind_end[1] - wind_start[1],
                          wind_end[2] - wind_start[2],
                          color='orange', linewidth=2, arrow_length_ratio=0.1, label='Ветер')
    
    def add_status_info(self):
        """Добавляет информацию о состоянии дрона"""
        pos = self.drone.position
        vel = self.drone.velocity
        orientation = np.degrees(self.drone.orientation)
        target = self.drone.target_position
        distance_to_target = np.linalg.norm(target - pos)
        
        info_text = f"""Состояние дрона:
Позиция: X={pos[0]:.2f}м, Y={pos[1]:.2f}м, Z={pos[2]:.2f}м
Скорость: {np.linalg.norm(vel):.2f} м/с
Ориентация: Крен={orientation[0]:.1f}°, Тангаж={orientation[1]:.1f}°, Рыскание={orientation[2]:.1f}°
Тяга: {self.drone.forces['thrust']:.2f} Н
Цель: X={target[0]:.1f}, Y={target[1]:.1f}, Z={target[2]:.1f}
Дистанция до цели: {distance_to_target:.2f}м
Достигнуто целей: {self.drone.target_counter}
"""
        
        self.ax.text2D(0.02, 0.98, info_text, transform=self.ax.transAxes, 
                      bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.9),
                      verticalalignment='top', fontsize=9)
    
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
    print("🚁 Запуск 3D визуализации дрона с ограниченной областью...")
    print("📊 Область перемещения: X[-4,4], Y[-4,4], Z[1,8] метров")
    print("🎯 Дрон будет перемещаться к случайным целям в пределах области")
    print("📈 Векторы сил:")
    print("   🔴 Красный - Тяга двигателей")
    print("   🔵 Синий - Вектор скорости") 
    print("   🟢 Зелёный - Гравитация")
    print("   🟣 Фиолетовый - Направление к цели")
    print("   🟠 Оранжевый - Ветер")
    print("   💛 Жёлтая звезда - Текущая цель")
    print("⏳ Запускаем анимацию...")
    
    visualizer = DroneVisualizer()
    visualizer.start_animation()