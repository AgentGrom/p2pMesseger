import asyncio
import time
import sys
from client.p2p_worker import P2PWorker

async def main():
    print("--- P2P Messenger Node ---")
    user_id = input("Твой ник (напр. Alice): ").strip()
    port = int(input("Твой порт (напр. 8001): ").strip())
    
    worker = P2PWorker("0.0.0.0", port, user_id)
    
    # Запускаем сервер в фоне
    server_task = asyncio.create_task(worker.start())

    print("КОМАНДЫ:")
    print("connect <имя> <ip> <порт> <ключ>  -- Подключиться и дать имя")
    print("list                             -- Показать активные контакты")
    print("send <имя> <сообщение>           -- Отправить")

    try:
        while True:
            cmd_line = await asyncio.to_thread(input, f"[{user_id}] > ")
            parts = cmd_line.strip().split(" ", 4)
            if not parts: continue
            cmd = parts[0].lower()

            if cmd == "connect":
                if len(parts) < 5: continue
                name, ip, t_port, key = parts[1], parts[2], int(parts[3]), parts[4]
                await worker.send_message(ip, t_port, key, "Запрос на связь", target_name=name)
                print(f"[*] Вы добавили {name} в список и отправили запрос.")

            elif cmd == "list":
                print("\n--- СПИСОК КОНТАКТОВ ---")
                for name, info in worker.contacts.items():
                    seen_ago = int(time.time() - info['last_seen'])
                    print(f"- {name} ({info['ip']}:{info['port']}) | Активен {seen_ago}с назад")
                print("------------------------")

            elif cmd == "send":
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