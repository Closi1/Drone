import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import matplotlib.widgets as widgets
from matplotlib.animation import FuncAnimation
import time
from datetime import datetime
import json

class InteractiveDroneFormation:
    def __init__(self, cube_size=6.0):
        """
        Интерактивное управление формацией дронов в реальном времени
        """
        self.cube_size = cube_size
        self.drone_count = 8
        self.drones = {}
        self.leader_id = 0
        self.is_dragging = False
        self.dragged_drone = None
        self.animation = None
        
        self.setup_cube_positions()
        self.setup_plot()
        
        print("🎮 ИНТЕРАКТИВНОЕ УПРАВЛЕНИЕ ФОРМАЦИЕЙ ДРОНОВ")
        print("👑 Перетаскивайте ЛИДЕРА (красный ромб) мышью!")
        print("📦 Остальные дроны автоматически сохранят форму куба")
        print("🖱️  ЛКМ + движение - перемещение по X,Y")
        print("🖱️  ПКМ + движение - перемещение по Z")
        print("🔧 Настройте скорость анимации слайдером")
        
    def setup_cube_positions(self):
        """Устанавливает начальные позиции дронов"""
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
        
        scale = self.cube_size / 2
        center = np.array([0, 0, self.cube_size])
        
        for i, vertex in enumerate(vertices):
            relative_pos = np.array(vertex, dtype=float) * scale
            absolute_pos = center + relative_pos
            
            self.drones[i] = {
                'id': f"DRONE_{i:02d}",
                'position': absolute_pos.copy(),
                'relative_position': relative_pos.copy(),
                'color': self.get_drone_color(i),
                'is_leader': (i == self.leader_id),
                'scatter': None,
                'label': None
            }
    
    def get_drone_color(self, index):
        """Возвращает цвет для дрона"""
        colors = ['red', 'blue', 'green', 'yellow', 
                 'orange', 'purple', 'pink', 'cyan']
        return colors[index % len(colors)]
    
    def setup_plot(self):
        """Настраивает интерактивный график"""
        self.fig = plt.figure(figsize=(16, 12))
        self.ax = self.fig.add_subplot(111, projection='3d')
        
        # Настройка внешнего вида
        self.ax.set_xlabel('X (м)', fontsize=12, fontweight='bold')
        self.ax.set_ylabel('Y (м)', fontsize=12, fontweight='bold')
        self.ax.set_zlabel('Z (м)', fontsize=12, fontweight='bold')
        self.ax.set_title('🎮 ИНТЕРАКТИВНОЕ УПРАВЛЕНИЕ ФОРМАЦИЕЙ ДРОНОВ\n👑 Перетаскивайте ЛИДЕРА мышью', 
                         fontsize=14, fontweight='bold', pad=20)
        
        # Создаем слайдер для скорости анимации
        slider_ax = self.fig.add_axes([0.15, 0.02, 0.7, 0.03])
        self.speed_slider = widgets.Slider(
            slider_ax, 'Скорость анимации', 0.1, 2.0, 
            valinit=1.0, valfmt='%0.1f'
        )
        self.speed_slider.on_changed(self.update_animation_speed)
        
        # Создаем кнопки
        buttons_ax = self.fig.add_axes([0.02, 0.7, 0.12, 0.2])
        buttons_ax.axis('off')
        
        # Кнопка сброса
        reset_ax = self.fig.add_axes([0.02, 0.85, 0.1, 0.04])
        self.reset_button = widgets.Button(reset_ax, '🔄 Сброс')
        self.reset_button.on_clicked(self.reset_positions)
        
        # Кнопка экспорта
        export_ax = self.fig.add_axes([0.02, 0.80, 0.1, 0.04])
        self.export_button = widgets.Button(export_ax, '💾 Экспорт')
        self.export_button.on_clicked(self.export_data)
        
        # Кнопка смены лидера
        leader_ax = self.fig.add_axes([0.02, 0.75, 0.1, 0.04])
        self.leader_button = widgets.Button(leader_ax, '👑 След. лидер')
        self.leader_button.on_clicked(self.next_leader)
        
        # Текстовое поле для информации
        self.info_text = self.fig.text(0.02, 0.55, '', fontsize=10, 
                                      bbox=dict(boxstyle="round,pad=0.5", facecolor="lightblue"))
        
        # Подключаем обработчики событий мыши
        self.fig.canvas.mpl_connect('button_press_event', self.on_click)
        self.fig.canvas.mpl_connect('button_release_event', self.on_release)
        self.fig.canvas.mpl_connect('motion_notify_event', self.on_motion)
        
        self.animation_speed = 1.0
        self.update_display()
        
    def update_display(self):
        """Обновляет отображение дронов"""
        self.ax.cla()
        
        # Отрисовываем дронов
        for drone_id, drone in self.drones.items():
            pos = drone['position']
            
            # Разные маркеры для лидера и ведомых
            if drone['is_leader']:
                marker = 'D'  # Ромб для лидера
                size = 200
                edgecolor = 'gold'
                linewidth = 3
                alpha = 1.0
            else:
                marker = 'o'  # Круг для ведомых
                size = 120
                edgecolor = 'black'
                linewidth = 1
                alpha = 0.8
            
            scatter = self.ax.scatter(pos[0], pos[1], pos[2], 
                                    c=drone['color'], s=size, marker=marker, 
                                    edgecolors=edgecolor, linewidth=linewidth,
                                    alpha=alpha, picker=True, pickradius=10)
            
            drone['scatter'] = scatter
            
            # Подписываем дронов
            label = f"👑 {drone['id']}" if drone['is_leader'] else drone['id']
            text = self.ax.text(pos[0], pos[1], pos[2] + 0.8, label, 
                              fontsize=9, ha='center', fontweight='bold',
                              bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.8))
            drone['label'] = text
        
        # Отрисовываем рёбра куба
        self.draw_cube_edges()
        
        # Настройки графика
        self.ax.set_xlabel('X (м)', fontsize=12, fontweight='bold')
        self.ax.set_ylabel('Y (м)', fontsize=12, fontweight='bold')
        self.ax.set_zlabel('Z (м)', fontsize=12, fontweight='bold')
        
        max_range = self.cube_size * 1.8
        self.ax.set_xlim([-max_range, max_range])
        self.ax.set_ylim([-max_range, max_range])
        self.ax.set_zlim([0, max_range * 2])
        
        self.ax.grid(True, alpha=0.3)
        self.ax.set_facecolor('whitesmoke')
        
        # Обновляем информационное поле
        self.update_info_text()
        
        plt.draw()
    
    def draw_cube_edges(self):
        """Отрисовывает рёбра куба"""
        edges = [
            [0, 1], [1, 2], [2, 3], [3, 0],  # Нижняя грань
            [4, 5], [5, 6], [6, 7], [7, 4],  # Верхняя грань
            [0, 4], [1, 5], [2, 6], [3, 7]   # Вертикальные рёбра
        ]
        
        for edge in edges:
            start_pos = self.drones[edge[0]]['position']
            end_pos = self.drones[edge[1]]['position']
            
            self.ax.plot([start_pos[0], end_pos[0]],
                        [start_pos[1], end_pos[1]],
                        [start_pos[2], end_pos[2]],
                        'gray', alpha=0.6, linewidth=2)
    
    def update_info_text(self):
        """Обновляет информационное текстовое поле"""
        leader = self.drones[self.leader_id]
        pos = leader['position']
        
        info = f"👑 ТЕКУЩИЙ ЛИДЕР: {leader['id']}\n"
        info += f"📍 Позиция: ({pos[0]:.1f}, {pos[1]:.1f}, {pos[2]:.1f})\n"
        info += f"📦 Размер куба: {self.cube_size} м\n"
        info += f"🎯 Скорость: {self.animation_speed:.1f}x\n\n"
        info += "🖱️ УПРАВЛЕНИЕ:\n"
        info += "• ЛКМ + движение - X,Y\n"
        info += "• ПКМ + движение - Z\n"
        info += "• Клик на дроне - выбрать\n"
        info += "• Слайдер - скорость\n\n"
        info += "🔧 КНОПКИ СЛЕВА:\n"
        info += "• Сброс позиций\n"
        info += "• Экспорт данных\n"
        info += "• Сменить лидера"
        
        self.info_text.set_text(info)
    
    def on_click(self, event):
        """Обработчик нажатия мыши"""
        if event.inaxes != self.ax:
            return
        
        if event.button == 1:  # Левая кнопка мыши
            # Проверяем, кликнули ли на дроне
            if hasattr(event, 'artist'):
                for drone_id, drone in self.drones.items():
                    if drone['scatter'] == event.artist:
                        if drone['is_leader']:
                            self.is_dragging = True
                            self.dragged_drone = drone_id
                            self.drag_start_pos = np.array([event.xdata, event.ydata])
                            self.drag_start_z = drone['position'][2]
                        else:
                            # Клик на ведомом дроне - делаем его лидером
                            self.set_leader(drone_id)
                        break
    
    def on_release(self, event):
        """Обработчик отпускания мыши"""
        self.is_dragging = False
        self.dragged_drone = None
    
    def on_motion(self, event):
        """Обработчик движения мыши"""
        if not self.is_dragging or self.dragged_drone is None:
            return
        
        if event.inaxes != self.ax:
            return
        
        leader = self.drones[self.leader_id]
        current_pos = leader['position'].copy()
        
        if event.button == 1:  # Левая кнопка - движение по X,Y
            if event.xdata is not None and event.ydata is not None:
                new_x = event.xdata
                new_y = event.ydata
                new_z = current_pos[2]
                
                self.move_leader(np.array([new_x, new_y, new_z]))
                
        elif event.button == 3:  # Правая кнопка - движение по Z
            if event.ydata is not None:
                # Используем движение мыши по Y для изменения высоты
                delta_y = event.ydata - self.drag_start_pos[1]
                new_z = max(1.0, self.drag_start_z + delta_y * 0.5)  # Минимальная высота 1м
                
                self.move_leader(np.array([current_pos[0], current_pos[1], new_z]))
    
    def move_leader(self, new_position):
        """Перемещает ведущего дрона и всю формацию"""
        old_leader_pos = self.drones[self.leader_id]['position'].copy()
        
        # Перемещаем лидера
        self.drones[self.leader_id]['position'] = np.array(new_position, dtype=float)
        
        # Вычисляем смещение
        displacement = new_position - old_leader_pos
        
        # Перемещаем всех ведомых дронов на такое же смещение
        for drone_id, drone in self.drones.items():
            if not drone['is_leader']:
                drone['position'] += displacement
        
        self.update_display()
    
    def set_leader(self, new_leader_id):
        """Устанавливает нового ведущего дрона"""
        if 0 <= new_leader_id < self.drone_count:
            # Снимаем пометку лидера со старого
            self.drones[self.leader_id]['is_leader'] = False
            
            # Устанавливаем нового лидера
            self.leader_id = new_leader_id
            self.drones[self.leader_id]['is_leader'] = True
            
            # Пересчитываем относительные позиции
            self.update_relative_positions()
            self.update_display()
            
            print(f"👑 Новый ведущий дрон: {self.drones[self.leader_id]['id']}")
    
    def update_relative_positions(self):
        """Обновляет относительные позиции всех дронов относительно лидера"""
        leader_pos = self.drones[self.leader_id]['position']
        
        for drone_id, drone in self.drones.items():
            if not drone['is_leader']:
                drone['relative_position'] = drone['position'] - leader_pos
    
    def next_leader(self, event=None):
        """Переключает на следующего дрона как лидера"""
        new_leader_id = (self.leader_id + 1) % self.drone_count
        self.set_leader(new_leader_id)
    
    def reset_positions(self, event=None):
        """Сбрасывает позиции дронов к начальным"""
        print("🔄 Сброс позиций...")
        self.setup_cube_positions()
        self.update_display()
    
    def export_data(self, event=None):
        """Экспортирует данные формации"""
        filename = f"formation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
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
                "is_leader": drone['is_leader']
            }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(formation_data, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Данные экспортированы в {filename}")
    
    def update_animation_speed(self, val):
        """Обновляет скорость анимации"""
        self.animation_speed = val
        self.update_display()
    
    def start_interactive_mode(self):
        """Запускает интерактивный режим"""
        self.update_display()
        plt.show()

def main():
    """Главная функция"""
    print("🚀 ЗАПУСК ИНТЕРАКТИВНОГО УПРАВЛЕНИЯ ДРОНАМИ")
    print("=" * 60)
    
    # Создаем систему управления
    controller = InteractiveDroneFormation(cube_size=6.0)
    
    # Запускаем интерактивный режим
    controller.start_interactive_mode()

if __name__ == "__main__":
    main()