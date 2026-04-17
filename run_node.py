import asyncio
import sys
from client.p2p_worker import P2PWorker

async def main():
    print("--- P2P Messenger Test Node ---")
    
    # 1. Настройка текущего узла
    try:
        port = int(input("Введите порт для этого узла (например, 8001): "))
        user_id = input("Введите ваш ник: ")
    except ValueError:
        print("Ошибка: Порт должен быть числом.")
        return

    node = P2PWorker("127.0.0.1", port, user_id)

    # Запускаем серверную часть в фоновом режиме
    server_task = asyncio.create_task(node.start())
    
    # Даем серверу немного времени на запуск
    await asyncio.sleep(0.5)

    print("\nКоманды:")
    print("1. send - отправить сообщение")
    print("2. exit - выйти")

    try:
        while True:
            cmd = await asyncio.to_thread(input, f"\n[{user_id}] > ")
            
            if cmd.lower() == "send":
                target_ip = input("IP получателя (для теста 127.0.0.1): ")
                target_port = int(input("Порт получателя: "))
                target_key = input("Публичный ключ получателя (Base64): ")
                text = input("Сообщение: ")
                
                await node.send_message(target_ip, target_port, target_key, text)
            
            elif cmd.lower() == "exit":
                print("Выход...")
                break
    except KeyboardInterrupt:
        pass
    finally:
        server_task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)