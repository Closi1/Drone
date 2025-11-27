from drone_database import DroneDatabase
from datetime import timedelta

def main():
    db = DroneDatabase()
    
    print("🔧 ОБСЛУЖИВАНИЕ БАЗЫ ДАННЫХ")
    print("=" * 40)
    
    # Показать статистику
    stats = db.get_database_stats()
    
    # Очистить старые данные (опционально)
    choice = input("\nОчистить данные старше 30 дней? (y/n): ").strip().lower()
    if choice == 'y':
        deleted = db.clear_old_data(30)
        print(f"✅ Удалено {deleted} записей")
    
    print("\n✅ Обслуживание завершено")

if __name__ == "__main__":
    main()