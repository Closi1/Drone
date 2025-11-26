import socket
import time
import json
import random
from datetime import datetime

class MicrocontrollerEmulator:
    def __init__(self):
        self.device_id = "MCU_001"
        self.server_host = "localhost"
        self.server_port = 8888
        
    def generate_sensor_data(self):
        """Генерирует случайные данные с датчиков"""
        return {
            "device_id": self.device_id,
            "timestamp": datetime.now().isoformat(),
            "temperature": round(random.uniform(20.0, 30.0), 2),  # Температура 20-30°C
            "humidity": round(random.uniform(40.0, 80.0), 2),     # Влажность 40-80%
            "pressure": round(random.uniform(980.0, 1020.0), 2),  # Давление 980-1020 hPa
            "voltage": round(random.uniform(3.2, 3.8), 2)        # Напряжение 3.2-3.8V
        }
    
    def send_data_to_server(self):
        """Отправляет данные на сервер"""
        try:
            # Создаём сокет для подключения
            client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_socket.connect((self.server_host, self.server_port))
            
            # Генерируем данные
            sensor_data = self.generate_sensor_data()
            
            # Преобразуем в JSON и отправляем
            json_data = json.dumps(sensor_data)
            client_socket.sendall(json_data.encode('utf-8'))
            
            # Получаем ответ от сервера
            response = client_socket.recv(1024).decode('utf-8')
            print(f"📨 Отправлено: {sensor_data}")
            print(f"📩 Ответ сервера: {response}")
            
            client_socket.close()
            return True
            
        except Exception as e:
            print(f"❌ Ошибка отправки: {e}")
            return False
    
    def run(self):
        """Запускает эмулятор"""
        print("🚀 ЗАПУСК ЭМУЛЯТОРА МИКРОКОНТРОЛЛЕРА")
        print(f"📟 ID устройства: {self.device_id}")
        print(f"🌐 Сервер: {self.server_host}:{self.server_port}")
        print("=" * 50)
        
        counter = 0
        while True:
            counter += 1
            print(f"\n🔁 Цикл отправки #{counter}")
            
            success = self.send_data_to_server()
            
            if success:
                print("✅ Данные успешно отправлены!")
            else:
                print("❌ Ошибка отправки данных!")
            
            # Ждём 10 секунд перед следующей отправкой
            print("⏰ Ожидание 10 секунд...")
            time.sleep(10)

# Запускаем эмулятор
if __name__ == "__main__":
    emulator = MicrocontrollerEmulator()
    emulator.run()