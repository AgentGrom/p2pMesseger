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
    print("1. connect <ip> <port> <key>  -- Подключиться (ник узнается сам)")
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
                if len(parts) < 4:
                    print("[!] Юзай: connect <ip> <port> <pub_key>")
                    continue
                ip, t_port, key = parts[1], int(parts[2]), parts[3]
                await worker.send_message(ip, t_port, key, "Запрос на соединение", is_handshake=True)
                print(f"[*] Инициализация связи с {ip}...")

            elif cmd == "list":
                print("\n--- ТВОИ КОНТАКТЫ ---")
                if not worker.contacts:
                    print("Список пуст.")
                for name, info in worker.contacts.items():
                    seen_ago = int(time.time() - info['last_seen'])
                    print(f"- {name} [{info['ip']}:{info['port']}] (активен {seen_ago}с назад)")
                print("-" * 25)

            elif cmd == "send":
                if len(parts) < 3:
                    print("[!] Юзай: send <ник> <сообщение>")
                    continue
                alias, text = parts[1], " ".join(parts[2:])
                await worker.send_to_contact(alias, text)

    except Exception as e:
        print(f"[!] Ошибка: {e}")
    finally:
        server_task.cancel()

if __name__ == "__main__":
    asyncio.run(main())