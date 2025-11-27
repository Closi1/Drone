from drone_formation import CubeFormation
import matplotlib.pyplot as plt
import numpy as np

class FormationController:
    def __init__(self):
        self.formations = {}
        self.active_formation = None
    
    def create_cube_formation(self, formation_id="cube_01", cube_size=10.0):
        """Создает новую формацию куба"""
        formation = CubeFormation(cube_size)
        self.formations[formation_id] = formation
        
        if self.active_formation is None:
            self.active_formation = formation_id
            
        print(f"✅ Создана формация куба: {formation_id}")
        return formation
    
    def show_formation(self, formation_id=None):
        """Показывает выбранную формацию"""
        if formation_id is None:
            formation_id = self.active_formation
        
        if formation_id not in self.formations:
            print(f"❌ Формация {formation_id} не найдена")
            return
        
        formation = self.formations[formation_id]
        
        fig = plt.figure(figsize=(12, 8))
        ax = fig.add_subplot(111, projection='3d')
        
        formation.plot_formation(ax)
        plt.show()
    
    def list_formations(self):
        """Показывает список всех формаций"""
        print("\n📋 СПИСОК ФОРМАЦИЙ:")
        print("=" * 30)
        for formation_id in self.formations.keys():
            status = "⚡ АКТИВНА" if formation_id == self.active_formation else "💤 НЕАКТИВНА"
            print(f"{formation_id} - {status}")

def main():
    """Главное меню управления формациями"""
    controller = FormationController()
    
    print("🎮 КОНТРОЛЛЕР ФОРМАЦИЙ ДРОНОВ")
    print("=" * 40)
    
    while True:
        print("\nВыберите действие:")
        print("1 - Создать формацию куба")
        print("2 - Показать формацию")
        print("3 - Список формаций")
        print("4 - Демонстрация анимации")
        print("0 - Выход")
        
        choice = input("Ваш выбор: ").strip()
        
        if choice == '1':
            formation_id = input("ID формации (например: cube_01): ").strip()
            size = float(input("Размер куба (м): ").strip() or "10.0")
            controller.create_cube_formation(formation_id, size)
            
        elif choice == '2':
            formation_id = input("ID формации (Enter для активной): ").strip()
            if formation_id == "":
                controller.show_formation()
            else:
                controller.show_formation(formation_id)
                
        elif choice == '3':
            controller.list_formations()
            
        elif choice == '4':
            from drone_formation import demonstrate_cube_formation
            demonstrate_cube_formation()
            
        elif choice == '0':
            print("👋 Выход из контроллера формаций")
            break
            
        else:
            print("❌ Неверный выбор")

if __name__ == "__main__":
    main()