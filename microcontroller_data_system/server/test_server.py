import socket
import time

print("🔄 ЗАПУСКАЕМ ТЕСТОВЫЙ СЕРВЕР...")

# Создаём простой сервер
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

try:
    server_socket.bind(('localhost', 8888))
    server_socket.listen(5)
    print("✅ Сервер запущен на localhost:8888")
    print("⏳ Ожидаем подключения...")
    
    while True:
        client_socket, address = server_socket.accept()
        print(f"🔌 Подключился клиент: {address}")
        
        # Просто отвечаем "OK"
        client_socket.send("OK".encode('utf-8'))
        client_socket.close()
        
except Exception as e:
    print(f"❌ Ошибка: {e}")
    print("Возможные причины:")
    print("1. Порт 8888 уже занят")
    print("2. Брандмауэр блокирует")
    print("3. Проблемы с сокетами")
    
input("Нажми Enter чтобы закрыть...")