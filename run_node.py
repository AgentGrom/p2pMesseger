import asyncio
import time
from client.p2p_worker import P2PWorker

async def main():
    print("--- P2P Messenger Node ---")
    user_id = input("Твой ник: ").strip()
    port = int(input("Твой порт: ").strip())
    
    worker = P2PWorker("0.0.0.0", port, user_id)
    server_task = asyncio.create_task(worker.start())

    print("\nКОМАНДЫ:")
    print("1. connect <ip> <port> -- Подключиться (ник узнается сам)")
    print("2. list                       -- Показать контакты")
    print("3. send <ник> <сообщение>      -- Отправить по нику")
    print("4. exit                       -- Выход")
    print("-" * 30)

    try:
        while True:
            cmd_line = await asyncio.to_thread(input, f"[{user_id}] > ")
            parts = cmd_line.strip().split(" ", 3)
            if not parts or not parts[0]: continue
            
            cmd = parts[0].lower()

            if cmd == "exit":
                break

            elif cmd == "connect":
                if len(parts) < 3:
                    print("[!] Юзай: connect <ip> <port>")
                    continue
                ip, t_port = parts[1], int(parts[2])
                # Вызываем метод, который просто шлет визитку
                await worker.send_initial_handshake(ip, t_port)


    except Exception as e:
        print(f"[!] Ошибка: {e}")
    finally:
        server_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())