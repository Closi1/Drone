import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import time
from datetime import datetime
import json

class CubeFormation:
    def __init__(self, cube_size=10.0):
        """
        Инициализация формации куба из 8 дронов
        
        Args:
            cube_size (float): размер стороны куба в метрах
        """
        self.cube_size = cube_size
        self.drone_count = 8
        self.drones = {}
        self.setup_cube_positions()
        
        print("🎯 ИНИЦИАЛИЗАЦИЯ ФОРМАЦИИ КУБА")
        print(f"📦 Количество дронов: {self.drone_count}")
        print(f"📏 Размер куба: {cube_size} м")
        
    def setup_cube_positions(self):
        """Устанавливает начальные позиции дронов в вершинах куба"""
        # Вершины куба
        vertices = [
            [0, 0, 0],  # 0: Нижняя левая ближняя
            [1, 0, 0],  # 1: Нижняя правая ближняя
            [1, 1, 0],  # 2: Нижняя правая дальняя
            [0, 1, 0],  # 3: Нижняя левая дальняя
            [0, 0, 1],  # 4: Верхняя левая ближняя
            [1, 0, 1],  # 5: Верхняя правая ближняя
            [1, 1, 1],  # 6: Верхняя правая дальняя
            [0, 1, 1]   # 7: Верхняя левая дальняя
        ]
        
        # Масштабируем до нужного размера
        for i, vertex in enumerate(vertices):
            x = vertex[0] * self.cube_size - self.cube_size/2
            y = vertex[1] * self.cube_size - self.cube_size/2
            z = vertex[2] * self.cube_size
            
            self.drones[i] = {
                'id': f"DRONE_{i:02d}",
                'position': np.array([x, y, z], dtype=float),
                'target_position': np.array([x, y, z], dtype=float),
                'color': self.get_drone_color(i),
                'status': 'ready'
            }
    
    def get_drone_color(self, index):
        """Возвращает цвет для дрона"""
        colors = ['red', 'blue', 'green', 'yellow', 
                 'orange', 'purple', 'pink', 'cyan']
        return colors[index % len(colors)]
    
    def move_formation(self, new_center, rotation_angle=0):
        """
        Перемещает всю формацию в новую позицию
        
        Args:
            new_center (list): новые координаты центра [x, y, z]
            rotation_angle (float): угол поворота вокруг оси Z в радианах
        """
        rotation_matrix = np.array([
            [np.cos(rotation_angle), -np.sin(rotation_angle), 0],
            [np.sin(rotation_angle), np.cos(rotation_angle), 0],
            [0, 0, 1]
        ])
        
        for drone_id, drone in self.drones.items():
            # Поворачиваем позицию относительно центра
            rotated_pos = np.dot(rotation_matrix, drone['position'])
            # Смещаем в новую позицию
            drone['target_position'] = rotated_pos + new_center
    
    def animate_movement(self, target_center, duration=10, steps=100):
        """
        Анимирует движение формации к новой позиции
        
        Args:
            target_center (list): целевая позиция центра
            duration (float): длительность анимации в секундах
            steps (int): количество шагов анимации
        """
        fig = plt.figure(figsize=(15, 10))
        ax = fig.add_subplot(111, projection='3d')
        
        current_center = np.array([0, 0, self.cube_size/2])
        
        for step in range(steps + 1):
            ax.cla()
            
            # Интерполяция позиции
            t = step / steps
            current_pos = current_center + (target_center - current_center) * t
            
            self.move_formation(current_pos, rotation_angle=t * 2 * np.pi)
            
            self.plot_formation(ax, step, steps)
            
            plt.pause(duration / steps)
        
        plt.show()
    
    def plot_formation(self, ax, current_step=0, total_steps=100):
        """
        Отрисовывает текущую формацию
        
        Args:
            ax: ось для отрисовки
            current_step (int): текущий шаг анимации
            total_steps (int): общее количество шагов
        """
        # Отрисовываем дронов
        for drone_id, drone in self.drones.items():
            pos = drone['position']
            ax.scatter(pos[0], pos[1], pos[2], 
                      c=drone['color'], s=100, marker='o', label=drone['id'])
            
            # Подписываем дронов
            ax.text(pos[0], pos[1], pos[2] + 0.5, drone['id'], 
                   fontsize=8, ha='center')
        
        # Отрисовываем рёбра куба
        self.draw_cube_edges(ax)
        
        # Настройки графика
        ax.set_xlabel('X (м)')
        ax.set_ylabel('Y (м)')
        ax.set_zlabel('Z (м)')
        ax.set_title(f'🎯 Формация 8 дронов - Куб\nШаг {current_step}/{total_steps}')
        
        # Устанавливаем равный масштаб по осям
        max_range = self.cube_size * 1.5
        ax.set_xlim([-max_range, max_range])
        ax.set_ylim([-max_range, max_range])
        ax.set_zlim([0, max_range])
        
        ax.legend()
        ax.grid(True)
    
    def draw_cube_edges(self, ax):
        """Отрисовывает рёбра куба"""
        edges = [
            [0, 1], [1, 2], [2, 3], [3, 0],  # Нижняя грань
            [4, 5], [5, 6], [6, 7], [7, 4],  # Верхняя грань
            [0, 4], [1, 5], [2, 6], [3, 7]   # Вертикальные рёбра
        ]
        
        for edge in edges:
            start_pos = self.drones[edge[0]]['position']
            end_pos = self.drones[edge[1]]['position']
            
            ax.plot([start_pos[0], end_pos[0]],
                   [start_pos[1], end_pos[1]],
                   [start_pos[2], end_pos[2]],
                   'gray', alpha=0.5, linewidth=1)
    
    def export_formation_data(self, filename="cube_formation.json"):
        """Экспортирует данные формации в JSON файл"""
        formation_data = {
            "export_time": datetime.now().isoformat(),
            "cube_size": self.cube_size,
            "drone_count": self.drone_count,
            "drones": {}
        }
        
        for drone_id, drone in self.drones.items():
            formation_data["drones"][drone_id] = {
                "id": drone['id'],
                "position": drone['position'].tolist(),
                "color": drone['color'],
                "status": drone['status']
            }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(formation_data, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Данные формации экспортированы в {filename}")
    
    def print_formation_info(self):
        """Выводит информацию о формации"""
        print("\n📊 ИНФОРМАЦИЯ О ФОРМАЦИИ КУБА:")
        print("=" * 50)
        for drone_id, drone in self.drones.items():
            pos = drone['position']
            print(f"{drone['id']}: ({pos[0]:.1f}, {pos[1]:.1f}, {pos[2]:.1f}) - {drone['color']}")

def demonstrate_cube_formation():
    """Демонстрация работы формации куба"""
    print("🚀 ДЕМОНСТРАЦИЯ ФОРМАЦИИ 8 ДРОНОВ - КУБ")
    print("=" * 60)
    
    # Создаем формацию
    formation = CubeFormation(cube_size=8.0)
    
    # Показываем информацию
    formation.print_formation_info()
    
    # Экспортируем данные
    formation.export_formation_data()
    
    # Демонстрируем движение
    print("\n🎬 ЗАПУСК АНИМАЦИИ ДВИЖЕНИЯ...")
    
    # Движение к новой позиции
    target_center = np.array([5, 3, 4])
    formation.animate_movement(target_center, duration=8, steps=50)

if __name__ == "__main__":
    demonstrate_cube_formation()