import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import time
from datetime import datetime
import json

class LeaderCubeFormation:
    def __init__(self, cube_size=8.0):
        """
        Инициализация формации куба с ведущим дроном
        
        Args:
            cube_size (float): размер стороны куба в метрах
        """
        self.cube_size = cube_size
        self.drone_count = 8
        self.drones = {}
        self.leader_id = 0  # DRONE_00 - ведущий дрон
        self.setup_cube_positions()
        
        print("🎯 ИНИЦИАЛИЗАЦИЯ ФОРМАЦИИ С ВЕДУЩИМ ДРОНОМ")
        print(f"📦 Количество дронов: {self.drone_count}")
        print(f"📏 Размер куба: {cube_size} м")
        print(f"👑 Ведущий дрон: {self.drones[self.leader_id]['id']}")
        
    def setup_cube_positions(self):
        """Устанавливает начальные позиции дронов в вершинах куба"""
        # Вершины куба относительно центра
        vertices = [
            [-1, -1, -1],  # 0: Нижняя левая ближняя (ВЕДУЩИЙ)
            [1, -1, -1],   # 1: Нижняя правая ближняя
            [1, 1, -1],    # 2: Нижняя правая дальняя
            [-1, 1, -1],   # 3: Нижняя левая дальняя
            [-1, -1, 1],   # 4: Верхняя левая ближняя
            [1, -1, 1],    # 5: Верхняя правая ближняя
            [1, 1, 1],     # 6: Верхняя правая дальняя
            [-1, 1, 1]     # 7: Верхняя левая дальняя
        ]
        
        # Масштабируем до нужного размера
        scale = self.cube_size / 2
        
        for i, vertex in enumerate(vertices):
            x = vertex[0] * scale
            y = vertex[1] * scale
            z = vertex[2] * scale + self.cube_size  # Поднимаем над землей
            
            self.drones[i] = {
                'id': f"DRONE_{i:02d}",
                'position': np.array([x, y, z], dtype=float),
                'relative_position': np.array(vertex, dtype=float) * scale,  # Относительно лидера
                'color': self.get_drone_color(i),
                'status': 'ready',
                'is_leader': (i == self.leader_id)
            }
    
    def get_drone_color(self, index):
        """Возвращает цвет для дрона"""
        colors = ['red', 'blue', 'green', 'yellow', 
                 'orange', 'purple', 'pink', 'cyan']
        return colors[index % len(colors)]
    
    def set_leader(self, drone_id):
        """
        Устанавливает нового ведущего дрона
        
        Args:
            drone_id (int): ID дрона (0-7)
        """
        if 0 <= drone_id < self.drone_count:
            # Снимаем пометку лидера со старого
            self.drones[self.leader_id]['is_leader'] = False
            
            # Устанавливаем нового лидера
            self.leader_id = drone_id
            self.drones[self.leader_id]['is_leader'] = True
            
            print(f"👑 Новый ведущий дрон: {self.drones[self.leader_id]['id']}")
            
            # Пересчитываем относительные позиции
            self.update_relative_positions()
        else:
            print(f"❌ Неверный ID дрона: {drone_id}")
    
    def update_relative_positions(self):
        """Обновляет относительные позиции всех дронов относительно лидера"""
        leader_pos = self.drones[self.leader_id]['position']
        
        for drone_id, drone in self.drones.items():
            if drone_id != self.leader_id:
                drone['relative_position'] = drone['position'] - leader_pos
    
    def move_leader(self, new_position, move_followers=True):
        """
        Перемещает ведущего дрона в новую позицию
        
        Args:
            new_position (array): новые координаты [x, y, z]
            move_followers (bool): двигать ли ведомых дронов
        """
        old_leader_pos = self.drones[self.leader_id]['position'].copy()
        
        # Перемещаем лидера
        self.drones[self.leader_id]['position'] = np.array(new_position, dtype=float)
        
        if move_followers:
            # Вычисляем смещение
            displacement = new_position - old_leader_pos
            
            # Перемещаем всех ведомых дронов на такое же смещение
            for drone_id, drone in self.drones.items():
                if drone_id != self.leader_id:
                    drone['position'] += displacement
            
            print(f"👑 Лидер перемещен в ({new_position[0]:.1f}, {new_position[1]:.1f}, {new_position[2]:.1f})")
            print(f"📦 Все дроны перемещены, формация сохранена")
        else:
            # Только лидер двигается, формация меняется
            self.update_relative_positions()
            print(f"👑 Лидер перемещен в ({new_position[0]:.1f}, {new_position[1]:.1f}, {new_position[2]:.1f})")
            print(f"⚠️  Формация изменена")
    
    def rotate_formation(self, angle_degrees, axis='z'):
        """
        Вращает всю формацию вокруг ведущего дрона
        
        Args:
            angle_degrees (float): угол поворота в градусах
            axis (str): ось вращения ('x', 'y', 'z')
        """
        angle_rad = np.radians(angle_degrees)
        leader_pos = self.drones[self.leader_id]['position']
        
        # Матрицы вращения
        if axis == 'z':
            rotation_matrix = np.array([
                [np.cos(angle_rad), -np.sin(angle_rad), 0],
                [np.sin(angle_rad), np.cos(angle_rad), 0],
                [0, 0, 1]
            ])
        elif axis == 'y':
            rotation_matrix = np.array([
                [np.cos(angle_rad), 0, np.sin(angle_rad)],
                [0, 1, 0],
                [-np.sin(angle_rad), 0, np.cos(angle_rad)]
            ])
        elif axis == 'x':
            rotation_matrix = np.array([
                [1, 0, 0],
                [0, np.cos(angle_rad), -np.sin(angle_rad)],
                [0, np.sin(angle_rad), np.cos(angle_rad)]
            ])
        else:
            print(f"❌ Неверная ось вращения: {axis}")
            return
        
        # Вращаем ведомых дронов вокруг лидера
        for drone_id, drone in self.drones.items():
            if drone_id != self.leader_id:
                # Вращаем относительную позицию
                rotated_relative = np.dot(rotation_matrix, drone['relative_position'])
                # Обновляем абсолютную позицию
                drone['position'] = leader_pos + rotated_relative
                # Обновляем относительную позицию
                drone['relative_position'] = rotated_relative
        
        print(f"🔄 Формация повернута на {angle_degrees}° вокруг оси {axis}")
    
    def interactive_control(self):
        """
        Интерактивное управление формацией через консоль
        """
        print("\n🎮 РЕЖИМ ИНТЕРАКТИВНОГО УПРАВЛЕНИЯ")
        print("=" * 50)
        
        fig = plt.figure(figsize=(15, 10))
        ax = fig.add_subplot(111, projection='3d')
        
        while True:
            print(f"\nТекущий лидер: {self.drones[self.leader_id]['id']}")
            print("Команды:")
            print("  move x y z  - переместить лидера (например: move 5 3 10)")
            print("  rotate deg axis - повернуть формацию (например: rotate 45 z)")
            print("  leader id   - сменить лидера (0-7)")
            print("  show        - показать формацию")
            print("  info        - информация о дронах")
            print("  export      - экспорт данных")
            print("  quit        - выход")
            
            command = input("\nВведите команду: ").strip().lower()
            
            if command == 'quit':
                print("👋 Выход из режима управления")
                break
                
            elif command == 'show':
                self.plot_formation(ax)
                plt.show()
                
            elif command == 'info':
                self.print_detailed_info()
                
            elif command == 'export':
                self.export_formation_data()
                
            elif command.startswith('move '):
                try:
                    parts = command.split()
                    x, y, z = float(parts[1]), float(parts[2]), float(parts[3])
                    self.move_leader(np.array([x, y, z]))
                    self.plot_formation(ax)
                    plt.draw()
                    plt.pause(0.1)
                except (ValueError, IndexError):
                    print("❌ Неверный формат. Используйте: move x y z")
                    
            elif command.startswith('rotate '):
                try:
                    parts = command.split()
                    angle = float(parts[1])
                    axis = parts[2] if len(parts) > 2 else 'z'
                    self.rotate_formation(angle, axis)
                    self.plot_formation(ax)
                    plt.draw()
                    plt.pause(0.1)
                except (ValueError, IndexError):
                    print("❌ Неверный формат. Используйте: rotate угол ось")
                    
            elif command.startswith('leader '):
                try:
                    new_leader = int(command.split()[1])
                    self.set_leader(new_leader)
                    self.plot_formation(ax)
                    plt.draw()
                    plt.pause(0.1)
                except (ValueError, IndexError):
                    print("❌ Неверный формат. Используйте: leader id (0-7)")
                    
            else:
                print("❌ Неизвестная команда")
    
    def plot_formation(self, ax):
        """Отрисовывает текущую формацию"""
        ax.cla()
        
        # Отрисовываем дронов
        for drone_id, drone in self.drones.items():
            pos = drone['position']
            
            # Разные маркеры для лидера и ведомых
            if drone['is_leader']:
                marker = 'D'  # Ромб для лидера
                size = 150
                edgecolor = 'gold'
                linewidth = 2
            else:
                marker = 'o'  # Круг для ведомых
                size = 100
                edgecolor = 'black'
                linewidth = 1
            
            ax.scatter(pos[0], pos[1], pos[2], 
                      c=drone['color'], s=size, marker=marker, 
                      edgecolors=edgecolor, linewidth=linewidth,
                      label=drone['id'])
            
            # Подписываем дронов
            label = f"👑 {drone['id']}" if drone['is_leader'] else drone['id']
            ax.text(pos[0], pos[1], pos[2] + 0.5, label, 
                   fontsize=8, ha='center')
        
        # Отрисовываем рёбра куба
        self.draw_cube_edges(ax)
        
        # Настройки графика
        ax.set_xlabel('X (м)')
        ax.set_ylabel('Y (м)')
        ax.set_zlabel('Z (м)')
        ax.set_title('🎯 Управление формацией куба (ведущий дрон - ромб)')
        
        # Устанавливаем равный масштаб по осям
        max_range = self.cube_size * 2
        ax.set_xlim([-max_range, max_range])
        ax.set_ylim([-max_range, max_range])
        ax.set_zlim([0, max_range * 1.5])
        
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
    
    def print_detailed_info(self):
        """Выводит подробную информацию о формации"""
        print("\n📊 ДЕТАЛЬНАЯ ИНФОРМАЦИЯ О ФОРМАЦИИ:")
        print("=" * 70)
        leader_pos = self.drones[self.leader_id]['position']
        
        for drone_id, drone in self.drones.items():
            pos = drone['position']
            rel_pos = drone['relative_position']
            leader_flag = "👑 ЛИДЕР" if drone['is_leader'] else "  ведомый"
            
            print(f"{drone['id']} {leader_flag}")
            print(f"   Абсолютная позиция: ({pos[0]:6.1f}, {pos[1]:6.1f}, {pos[2]:6.1f})")
            print(f"   Относительно лидера: ({rel_pos[0]:6.1f}, {rel_pos[1]:6.1f}, {rel_pos[2]:6.1f})")
            print(f"   Цвет: {drone['color']}")
            print()
    
    def export_formation_data(self, filename="leader_formation.json"):
        """Экспортирует данные формации в JSON файл"""
        formation_data = {
            "export_time": datetime.now().isoformat(),
            "cube_size": self.cube_size,
            "leader_id": self.leader_id,
            "leader_drone": self.drones[self.leader_id]['id'],
            "drones": {}
        }
        
        for drone_id, drone in self.drones.items():
            formation_data["drones"][drone_id] = {
                "id": drone['id'],
                "position": drone['position'].tolist(),
                "relative_position": drone['relative_position'].tolist(),
                "color": drone['color'],
                "is_leader": drone['is_leader'],
                "status": drone['status']
            }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(formation_data, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Данные формации экспортированы в {filename}")

def main():
    """Главная функция демонстрации"""
    print("🚀 СИСТЕМА УПРАВЛЕНИЯ ФОРМАЦИЕЙ КУБА")
    print("🎯 Управляйте одним дроном - остальные следуют за ним")
    print("=" * 60)
    
    # Создаем формацию
    formation = LeaderCubeFormation(cube_size=6.0)
    
    # Показываем начальную информацию
    formation.print_detailed_info()
    
    # Запускаем интерактивное управление
    formation.interactive_control()

if __name__ == "__main__":
    main()