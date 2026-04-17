import asyncio
import sys
from client.p2p_worker import P2PWorker

async def main():
    print("--- P2P Messenger Node ---")
    user_id = input("Твой ник (напр. Alice): ").strip()
    port = int(input("Твой порт (напр. 8001): ").strip())
    
    worker = P2PWorker("0.0.0.0", port, user_id)
    
    # Запускаем сервер в фоне
    server_task = asyncio.create_task(worker.start())

    print("\nКОМАНДЫ:")
    print("1. connect <ip> <port> <key>  -- Подключиться к новому другу")
    print("2. send <ник> <сообщение>     -- Отправить сообщение по нику")
    print("3. exit                       -- Выход")
    print("-" * 30)

    try:
        while True:
            # Читаем ввод пользователя асинхронно
            cmd_line = await asyncio.to_thread(input, f"[{user_id}] > ")
            parts = cmd_line.strip().split(" ", 3)
            
            if not parts or not parts[0]:
                continue
                
            command = parts[0].lower()

            if command == "exit":
                break

            elif command == "connect":
                if len(parts) < 4:
                    print("[!] Юзай: connect <ip> <port> <pub_key>")
                    continue
                ip, t_port, key = parts[1], int(parts[2]), parts[3]
                # Шлем приветствие, чтобы другой узел нас узнал
                await worker.send_message(ip, t_port, key, "Привет! Давай общаться.")
                print("[*] Попытка подключения отправлена...")

            elif command == "send":
                if len(parts) < 3:
                    print("[!] Юзай: send <ник> <сообщение>")
                    continue
                alias, text = parts[1], " ".join(parts[2:])
                await worker.send_to_contact(alias, text)

            else:
                print("[?] Неизвестная команда")

    except Exception as e:
        print(f"[!] Ошибка: {e}")
    finally:
        print("[*] Выключение...")
        server_task.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass